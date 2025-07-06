package indexer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	core "github.com/ethereum/go-ethereum/core"
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
	repo           *repository.StateRepository
	rangeProcessor *storage.RangeProcessor
	config         internal.Config
	log            *slog.Logger
}

type Service struct {
	indexer   *Indexer
	repo      *repository.StateRepository
	rpcClient *rpc.Client
	config    internal.Config
	log       *slog.Logger
}

func NewIndexer(repo *repository.StateRepository, rangeProcessor *storage.RangeProcessor, config internal.Config) *Indexer {
	return &Indexer{
		repo:           repo,
		rangeProcessor: rangeProcessor,
		config:         config,
		log:            logger.GetLogger("indexer"),
	}
}

func NewService(repo *repository.StateRepository, rpcClient *rpc.Client, config internal.Config) *Service {
	log := logger.GetLogger("indexer-service")

	// Initialize range processor
	rangeProcessor, err := storage.NewRangeProcessor(config.DataDir, rpcClient, config.RangeSize)
	if err != nil {
		log.Error("Failed to create range processor", "error", err)
		return nil
	}

	return &Service{
		indexer:   NewIndexer(repo, rangeProcessor, config),
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

	accessedAccounts := make(map[string]uint64, len(genesis.Alloc))
	accessedAccountsType := make(map[string]bool, len(genesis.Alloc))
	for acc, alloc := range genesis.Alloc {
		// Check if this genesis account has code (is a contract)
		isContract := len(alloc.Code) > 0
		accessedAccounts[acc.String()] = 0
		accessedAccountsType[acc.String()] = isContract
	}

	return i.repo.UpdateRangeDataInTx(ctx, accessedAccounts, accessedAccountsType, nil, 0)
}

type stateAccess struct {
	accounts    map[string]uint64
	accountType map[string]bool
	storage     map[string]map[string]uint64

	count int
}

func newStateAccess() stateAccess {
	return stateAccess{
		accounts:    make(map[string]uint64),
		accountType: make(map[string]bool),
		storage:     make(map[string]map[string]uint64),
	}
}

func (s *stateAccess) addAccount(addr string, blockNumber uint64, isContract bool) {
	if _, ok := s.accounts[addr]; !ok {
		s.count++
	}

	s.accounts[addr] = blockNumber

	if old, ok := s.accountType[addr]; !ok { // new account
		s.accountType[addr] = isContract
	} else if !old {
		s.accountType[addr] = isContract
	} else {
		s.accountType[addr] = isContract
	}
}

func (s *stateAccess) addStorage(addr string, slot string, blockNumber uint64) {
	if _, ok := s.storage[addr]; !ok {
		s.storage[addr] = make(map[string]uint64)
	}

	if _, ok := s.storage[addr][slot]; !ok {
		s.count++
	}

	s.storage[addr][slot] = blockNumber
}

func (s *stateAccess) reset() {
	clear(s.accounts)
	clear(s.accountType)
	clear(s.storage)
	s.count = 0
}

// ProcessRange processes an entire range of blocks
func (i *Indexer) ProcessRange(ctx context.Context, rangeNumber uint64, sa *stateAccess, force bool) error {
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
		err := i.processBlockDiff(rangeDiff, sa)
		if err != nil {
			return fmt.Errorf("could not process block %d in range %d: %w", rangeDiff.BlockNum, rangeNumber, err)
		}
	}

	if sa.count > defaultCommitSize || force {
		// Update database with all blocks in the range in a single transaction
		if err := i.repo.UpdateRangeDataInTx(ctx, sa.accounts, sa.accountType, sa.storage, rangeNumber); err != nil {
			return fmt.Errorf("could not update range data for range %d: %w", rangeNumber, err)
		}

		i.log.Info("Committing range data", "range_number", rangeNumber, "accounts", len(sa.accounts), "storage", len(sa.storage))
		sa.reset()
	}

	i.log.Info("Successfully processed range",
		"range_number", rangeNumber,
		"blocks_processed", len(rangeDiffs),
		"range_start", start,
		"range_end", end)

	return nil
}

// processBlockDiff processes a single block's state diff data and returns the processed data
func (i *Indexer) processBlockDiff(rangeDiff storage.RangeDiffs, sa *stateAccess) error {
	blockNumber := rangeDiff.BlockNum
	stateDiffs := rangeDiff.Diffs

	i.log.Debug("Processing block from range",
		"block_number", blockNumber,
		"transaction_count", len(stateDiffs))

	for _, txResult := range stateDiffs {
		for addr, diff := range txResult.StateDiff {
			sa.addAccount(addr, blockNumber, i.determineAccountType(diff))

			if diff.Storage != nil {
				storageMap, ok := diff.Storage.(map[string]any)
				if ok {
					for slot := range storageMap {
						sa.addStorage(addr, slot, blockNumber)
					}
				}
			}
		}
	}

	i.log.Debug("Processed state access patterns",
		"block_number", blockNumber,
		"accessed_accounts_count", len(sa.accounts),
		"accessed_storage_accounts", len(sa.storage))

	return nil
}

// determineAccountType analyzes the account diff to determine if it's a contract
func (i *Indexer) determineAccountType(diff rpc.AccountDiff) bool {
	// If the account has code changes, it's definitely a contract
	if diff.Code != nil {
		if _, ok := diff.Code.(map[string]any); ok {
			return true
		}
	}

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

	// Special case: process genesis if starting from range 0
	sa := newStateAccess()
	if lastIndexedRange == 0 {
		if err := s.indexer.ProcessRange(ctx, 0, &sa, true); err != nil {
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
		if err := s.indexer.ProcessRange(ctx, currentRange, &sa, false); err != nil {
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
