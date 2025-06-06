package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	core "github.com/ethereum/go-ethereum/core"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/logger"
	"github.com/weiihann/state-expiry-indexer/internal/repository"
	"github.com/weiihann/state-expiry-indexer/pkg/rpc"
)

type Indexer struct {
	Path string
	repo *repository.StateRepository
	log  *slog.Logger
}

type Service struct {
	indexer *Indexer
	repo    *repository.StateRepository
	config  internal.Config
	log     *slog.Logger
}

func NewIndexer(path string, repo *repository.StateRepository) *Indexer {
	return &Indexer{
		Path: path,
		repo: repo,
		log:  logger.GetLogger("indexer"),
	}
}

func NewService(path string, repo *repository.StateRepository, config internal.Config) *Service {
	log := logger.GetLogger("indexer-service")
	return &Service{
		indexer: NewIndexer(path, repo),
		repo:    repo,
		config:  config,
		log:     log,
	}
}

func (i *Indexer) ProcessGenesis(ctx context.Context) error {
	genesis := core.DefaultGenesisBlock()

	accessedAccounts := make(map[string]struct{}, len(genesis.Alloc))
	for acc := range genesis.Alloc {
		accessedAccounts[acc.String()] = struct{}{}
	}

	return i.repo.UpdateBlockDataInTx(ctx, 0, accessedAccounts, nil)
}

func (i *Indexer) ProcessBlock(ctx context.Context, blockNumber uint64) error {
	filename := fmt.Sprintf("%d.json", blockNumber)
	filePath := filepath.Join(i.Path, filename)

	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("could not read state diff file for block %d: %w", blockNumber, err)
	}

	var stateDiffs []rpc.TransactionResult
	if err := json.Unmarshal(data, &stateDiffs); err != nil {
		return fmt.Errorf("could not unmarshal state diff for block %d: %w", blockNumber, err)
	}

	i.log.Debug("Processing block", "block_number", blockNumber, "transaction_count", len(stateDiffs))

	accessedAccounts := make(map[string]struct{})
	accessedStorage := make(map[string]map[string]struct{})

	for _, txResult := range stateDiffs {
		for addr, diff := range txResult.StateDiff {
			accessedAccounts[addr] = struct{}{} // Any appearance in the state diff means the account was accessed

			if diff.Storage != nil {
				if _, ok := accessedStorage[addr]; !ok {
					accessedStorage[addr] = make(map[string]struct{})
				}
				storageMap, ok := diff.Storage.(map[string]any)
				if ok {
					for slot := range storageMap {
						accessedStorage[addr][slot] = struct{}{}
					}
				}
			}
		}
	}

	i.log.Debug("Processed state access patterns",
		"block_number", blockNumber,
		"accessed_accounts_count", len(accessedAccounts),
		"accessed_storage_accounts", len(accessedStorage))

	// Log detailed access patterns at debug level
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		for addr := range accessedAccounts {
			i.log.Debug("Accessed account", "block_number", blockNumber, "account", addr)
		}

		for addr, slots := range accessedStorage {
			for slot := range slots {
				i.log.Debug("Accessed storage",
					"block_number", blockNumber,
					"account", addr,
					"slot", slot)
			}
		}
	}

	if err := i.repo.UpdateBlockDataInTx(ctx, blockNumber, accessedAccounts, accessedStorage); err != nil {
		return fmt.Errorf("could not update block data for block %d: %w", blockNumber, err)
	}

	i.log.Debug("Successfully updated block data", "block_number", blockNumber)

	return nil
}

// ProcessBlock processes a single block through the indexer
func (s *Service) ProcessBlock(ctx context.Context, blockNumber uint64) error {
	return s.indexer.ProcessBlock(ctx, blockNumber)
}

// RunProcessor starts the indexer processor workflow that processes available files
func (s *Service) RunProcessor(ctx context.Context) error {
	s.log.Info("Starting indexer processor workflow",
		"poll_interval", s.config.PollInterval,
		"data_path", s.indexer.Path)

	pollInterval := time.Duration(s.config.PollInterval) * time.Second

	for {
		select {
		case <-ctx.Done():
			s.log.Info("Indexer processor workflow stopped")
			return nil
		default:
			// Continue with processing logic
		}

		if err := s.processAvailableFiles(ctx); err != nil {
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

// processAvailableFiles processes all available files that haven't been indexed yet
func (s *Service) processAvailableFiles(ctx context.Context) error {
	lastIndexedBlock, err := s.repo.GetLastIndexedBlock(ctx)
	if err != nil {
		return fmt.Errorf("could not get last processed block: %w", err)
	}

	s.log.Debug("Checking for files to process", "last_indexed_block", lastIndexedBlock)

	// Special case: process genesis if starting from block 0
	if lastIndexedBlock == 0 {
		if err := s.indexer.ProcessGenesis(ctx); err != nil {
			return fmt.Errorf("could not process genesis: %w", err)
		}
		s.log.Info("Successfully processed genesis")
	}

	processedCount := 0
	currentBlock := lastIndexedBlock + 1
	lastProgressTime := time.Now()
	lastProgressBlock := lastIndexedBlock

	// Process files sequentially from the next unprocessed block
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		filename := fmt.Sprintf("%d.json", currentBlock)
		filePath := filepath.Join(s.indexer.Path, filename)

		// Check if file exists
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			// No more files available to process
			break
		} else if err != nil {
			return fmt.Errorf("could not check file %s: %w", filename, err)
		}

		// Process the file
		if err := s.indexer.ProcessBlock(ctx, currentBlock); err != nil {
			return fmt.Errorf("could not process block %d: %w", currentBlock, err)
		}

		// Show simple progress every 1000 blocks or 8 seconds
		now := time.Now()
		blocksSinceProgress := currentBlock - lastProgressBlock
		timeSinceProgress := now.Sub(lastProgressTime).Seconds()

		if blocksSinceProgress >= 1000 || timeSinceProgress >= 8 {
			s.log.Info("Processing progress",
				"current_block", currentBlock,
				"processed_this_cycle", processedCount+1)
			lastProgressTime = now
			lastProgressBlock = currentBlock
		}

		s.log.Debug("Successfully processed block",
			"block_number", currentBlock,
			"filename", filename)

		processedCount++
		currentBlock++
	}

	if processedCount > 0 {
		s.log.Info("Completed processing cycle",
			"processed_blocks", processedCount,
			"last_indexed_block", currentBlock-1)
	} else {
		s.log.Debug("No new files available for processing",
			"last_indexed_block", lastIndexedBlock)
	}

	return nil
}
