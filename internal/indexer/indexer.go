package indexer

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	core "github.com/ethereum/go-ethereum/core"
	"github.com/hashicorp/golang-lru/v2"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/logger"
	"github.com/weiihann/state-expiry-indexer/internal/repository"
	"github.com/weiihann/state-expiry-indexer/pkg/rpc"
	"github.com/weiihann/state-expiry-indexer/pkg/storage"
)

const (
	defaultCommitSize = 1000000
)

type Indexer struct {
	repo           repository.StateRepositoryInterface
	rangeProcessor *storage.RangeProcessor
	rpcClient      rpc.ClientInterface
	config         internal.Config
	log            *slog.Logger
	accountType    *lru.Cache[string, bool] // address -> isContract
}

type Service struct {
	indexer   *Indexer
	repo      repository.StateRepositoryInterface
	rpcClient rpc.ClientInterface
	config    internal.Config
	log       *slog.Logger
}

func NewIndexer(repo repository.StateRepositoryInterface, rangeProcessor *storage.RangeProcessor, rpcClient rpc.ClientInterface, config internal.Config) *Indexer {
	accountType, _ := lru.New[string, bool](50000000)

	return &Indexer{
		repo:           repo,
		rangeProcessor: rangeProcessor,
		rpcClient:      rpcClient,
		config:         config,
		log:            logger.GetLogger("indexer"),
		accountType:    accountType,
	}
}

func NewService(repo repository.StateRepositoryInterface, rpcClient rpc.ClientInterface, config internal.Config) *Service {
	log := logger.GetLogger("indexer-service")

	// Initialize range processor
	rangeProcessor, err := storage.NewRangeProcessor(config.DataDir, rpcClient, config.RangeSize)
	if err != nil {
		log.Error("Failed to create range processor", "error", err)
		return nil
	}

	return &Service{
		indexer:   NewIndexer(repo, rangeProcessor, rpcClient, config),
		rpcClient: rpcClient,
		repo:      repo,
		config:    config,
		log:       log,
	}
}

// Close properly closes the service and its resources
func (s *Service) Close() {
	if s.indexer != nil && s.indexer.rangeProcessor != nil {
		s.indexer.rangeProcessor.Close()
	}
}

func (i *Indexer) ProcessGenesis(ctx context.Context) error {
	genesis := core.DefaultGenesisBlock()

	accessedAccounts := make(map[uint64]map[string]struct{}, len(genesis.Alloc))
	accessedAccounts[0] = make(map[string]struct{}, len(genesis.Alloc))
	accessedAccountsType := make(map[string]bool, len(genesis.Alloc))
	for acc, alloc := range genesis.Alloc {
		// Check if this genesis account has code (is a contract)
		isContract := len(alloc.Code) > 0
		accessedAccounts[0][acc.String()] = struct{}{}
		accessedAccountsType[acc.String()] = isContract
	}

	return i.repo.InsertRange(ctx, accessedAccounts, accessedAccountsType, map[uint64]map[string]map[string]struct{}{}, 0)
}

// ProcessRange processes an entire range of blocks
func (i *Indexer) ProcessRange(ctx context.Context, rangeNumber uint64, sa StateAccess, force bool) error {
	if rangeNumber == 0 {
		// Genesis is handled separately
		return i.ProcessGenesis(ctx)
	}

	start, end := i.rangeProcessor.GetRangeBlockNumbers(rangeNumber)

	i.log.Info("Processing range",
		"range_number", rangeNumber,
		"range_start", start,
		"range_end", end,
		"range_size", end-start+1)

	// Ensure the range file exists (download if necessary)
	if err := i.rangeProcessor.EnsureRangeExists(ctx, rangeNumber); err != nil {
		return fmt.Errorf("could not ensure range %d exists: %w", rangeNumber, err)
	}

	// Read the range file
	rangeDiffs, err := i.rangeProcessor.ReadRange(rangeNumber)
	if err != nil {
		return fmt.Errorf("could not read range %d: %w", rangeNumber, err)
	}

	i.log.Debug("Read range file",
		"range_number", rangeNumber,
		"blocks_in_range", len(rangeDiffs))

	// Process all blocks in the range and prepare batch data
	for _, rangeDiff := range rangeDiffs {
		err := i.processBlockDiff(ctx, rangeDiff, sa)
		if err != nil {
			return fmt.Errorf("could not process block %d in range %d: %w", rangeDiff.BlockNum, rangeNumber, err)
		}
	}

	if sa.Count() > defaultCommitSize || force {
		i.log.Info("Triggering commit", "range_number", rangeNumber, "count", sa.Count())

		// Update database with all blocks in the range in a single transaction
		if err := sa.Commit(ctx, i.repo, rangeNumber); err != nil {
			return fmt.Errorf("could not update range data for range %d: %w", rangeNumber, err)
		}

		i.log.Info("Committed range data", "range_number", rangeNumber, "account_events", sa.Count())
		sa.Reset()
	}

	i.log.Info("Successfully processed range",
		"range_number", rangeNumber,
		"blocks_processed", len(rangeDiffs),
		"range_start", start,
		"range_end", end)

	return nil
}

// processBlockDiff processes a single block's state diff data and returns the processed data
func (i *Indexer) processBlockDiff(ctx context.Context, rangeDiff storage.RangeDiffs, sa StateAccess) error {
	blockNumber := rangeDiff.BlockNum
	stateDiffs := rangeDiff.Diffs

	i.log.Debug("Processing block from range",
		"block_number", blockNumber,
		"transaction_count", len(stateDiffs))

	for _, txResult := range stateDiffs {
		for addr, diff := range txResult.StateDiff {
			isContract := i.determineAccountType(ctx, addr, blockNumber, diff)

			err := sa.AddAccount(addr, blockNumber, isContract)
			if err != nil {
				return fmt.Errorf("could not process account %s in block %d: %w", addr, blockNumber, err)
			}

			if diff.Storage != nil {
				storageMap, ok := diff.Storage.(map[string]any)
				if ok {
					for slot := range storageMap {
						sa.AddStorage(addr, slot, blockNumber)
					}
				}
			}
		}
	}

	i.log.Debug("Processed block",
		"block_number", blockNumber,
		"account_events", sa.Count())

	return nil
}

// determineAccountType analyzes the account diff to determine if it's a contract
func (i *Indexer) determineAccountType(ctx context.Context, addr string, blockNumber uint64, diff rpc.AccountDiff) bool {
	if isContract, ok := i.accountType.Get(addr); ok {
		return isContract
	}

	// If the account has code changes, it's definitely a contract
	if diff.Code != nil {
		if _, ok := diff.Code.(map[string]any); ok {
			i.accountType.Add(addr, true)
			return true
		}
	}

	// If the account has storage changes, it's definitely a contract
	if diff.Storage != nil {
		if _, ok := diff.Storage.(map[string]any); ok {
			i.accountType.Add(addr, true)
			return true
		}
	}

	// If both cache misses and diff miss, check via RPC
	code, err := i.rpcClient.GetCode(ctx, addr, big.NewInt(int64(blockNumber)))
	if err != nil {
		return false
	}
	if len(code) > 2 { // Omit the 0x prefix
		i.accountType.Add(addr, true)
		return true
	}

	i.accountType.Add(addr, false)
	return false
}

// RunProcessor starts the indexer processor workflow that processes available ranges
func (s *Service) RunProcessor(ctx context.Context) error {
	s.log.Info("Starting range-based indexer processor workflow",
		"poll_interval", s.config.PollInterval,
		"data_path", s.config.DataDir,
		"range_size", s.config.RangeSize)

	pollInterval := time.Duration(s.config.PollInterval) * time.Second

	for {
		select {
		case <-ctx.Done():
			s.log.Info("Indexer processor workflow stopped")
			return nil
		default:
			// Continue with processing logic
		}

		if err := s.processAvailableRanges(ctx); err != nil {
			s.log.Warn("Processing cycle failed, retrying...",
				"error", err,
				"retry_interval", pollInterval)
			time.Sleep(pollInterval)
			continue
		}

		// Wait before next processing cycle
		time.Sleep(pollInterval)
	}
}

// processAvailableRanges processes all available ranges that haven't been indexed yet
func (s *Service) processAvailableRanges(ctx context.Context) error {
	lastIndexedRange, err := s.repo.GetLastIndexedRange(ctx)
	if err != nil {
		return fmt.Errorf("could not get last processed range: %w", err)
	}

	s.log.Debug("Checking for ranges to process", "last_indexed_range", lastIndexedRange)

	sa := newStateAccessArchive()

	// Special case: process genesis if starting from range 0
	if lastIndexedRange == 0 {
		if err := s.indexer.ProcessRange(ctx, 0, sa, true); err != nil {
			return fmt.Errorf("could not process genesis range: %w", err)
		}
		s.log.Info("Successfully processed genesis")
		lastIndexedRange = 0
	}

	processedCount := 0
	currentRange := lastIndexedRange + 1
	lastProgressTime := time.Now()
	lastProgressRange := lastIndexedRange

	// Get latest block to determine how many ranges we can process
	latestBlock, err := s.rpcClient.GetLatestBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("could not get latest available block: %w", err)
	}

	latestRange := s.indexer.rangeProcessor.GetRangeNumber(latestBlock.Uint64())

	s.log.Debug("Range processing scope",
		"latest_block", latestBlock,
		"latest_range", latestRange,
		"current_range", currentRange)

	// Process ranges sequentially
	for currentRange <= latestRange {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Process the range
		if err := s.indexer.ProcessRange(ctx, currentRange, sa, false); err != nil {
			return fmt.Errorf("could not process range %d: %w", currentRange, err)
		}

		// Show progress every few ranges or 30 seconds
		now := time.Now()
		rangesSinceProgress := currentRange - lastProgressRange
		timeSinceProgress := now.Sub(lastProgressTime).Seconds()

		if rangesSinceProgress >= 5 || timeSinceProgress >= 30 {
			start, end := s.indexer.rangeProcessor.GetRangeBlockNumbers(currentRange)
			s.log.Info("Range processing progress",
				"current_range", currentRange,
				"current_range_blocks", fmt.Sprintf("%d-%d", start, end),
				"latest_range", latestRange,
				"remaining_ranges", latestRange-currentRange,
				"processed_this_cycle", processedCount+1)
			lastProgressTime = now
			lastProgressRange = currentRange
		}

		s.log.Debug("Successfully processed range",
			"range_number", currentRange,
			"blocks_in_range", s.config.RangeSize)

		processedCount++
		currentRange++
	}

	// TODO: When caught up to the latest range, switch to block-by-block processing.
	// The current implementation waits for a full new range to become available, which
	// can lead to delays in indexing the most recent blocks. The new logic should:
	// 1. Check if we are at the chain head (e.g., lastIndexedRange == latestRange).
	// 2. If so, switch to polling for new blocks individually.
	// 3. Process new blocks one by one until a new full range is available behind us.
	// 4. Once a full range is available, we can potentially switch back to range processing.
	// This will require careful state management and new logic to fetch and process
	// single blocks.

	if processedCount > 0 {
		s.log.Info("Completed range processing cycle",
			"processed_ranges", processedCount,
			"last_indexed_range", currentRange-1)
	} else {
		s.log.Debug("No new ranges available for processing",
			"last_indexed_range", lastIndexedRange)
	}

	return nil
}
