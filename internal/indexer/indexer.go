package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

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
	indexer      *Indexer
	repo         *repository.StateRepository
	progressFile string
	startBlock   uint64
	log          *slog.Logger
}

func NewIndexer(path string, repo *repository.StateRepository) *Indexer {
	return &Indexer{
		Path: path,
		repo: repo,
		log:  logger.GetLogger("indexer"),
	}
}

func NewService(path string, repo *repository.StateRepository, startBlock uint64) *Service {
	return &Service{
		indexer:      NewIndexer(filepath.Join(path, "statediffs"), repo),
		repo:         repo,
		progressFile: filepath.Join(path, "last_processed_block.txt"),
		startBlock:   startBlock,
		log:          logger.GetLogger("indexer-service"),
	}
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

	i.log.Info("Processing block", "block_number", blockNumber, "transaction_count", len(stateDiffs))

	accessedAccounts := make(map[string]bool)
	accessedStorage := make(map[string]map[string]bool)

	for _, txResult := range stateDiffs {
		for addr, diff := range txResult.StateDiff {
			accessedAccounts[addr] = true // Any appearance in the state diff means the account was accessed

			if diff.Storage != nil {
				if _, ok := accessedStorage[addr]; !ok {
					accessedStorage[addr] = make(map[string]bool)
				}
				storageMap, ok := diff.Storage.(map[string]interface{})
				if ok {
					for slot := range storageMap {
						accessedStorage[addr][slot] = true
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

	i.log.Info("Successfully updated block data", "block_number", blockNumber)

	return nil
}

func (s *Service) Run(ctx context.Context, endBlock uint64) error {
	lastProcessed, err := s.repo.GetLastIndexedBlock(ctx)
	if err != nil {
		return fmt.Errorf("could not get last processed block: %w", err)
	}

	start := lastProcessed + 1
	if s.startBlock > start {
		start = s.startBlock
	}

	s.log.Info("Starting indexer service",
		"start_block", start,
		"end_block", endBlock,
		"total_blocks", endBlock-start+1)

	for blockNumber := start; blockNumber <= endBlock; blockNumber++ {
		s.log.Debug("Processing block", "block_number", blockNumber)
		if err := s.indexer.ProcessBlock(ctx, blockNumber); err != nil {
			return fmt.Errorf("could not process block %d: %w", blockNumber, err)
		}
	}

	s.log.Info("Indexer finished processing all blocks in range",
		"start_block", start,
		"end_block", endBlock)
	return nil
}

// ProcessBlock processes a single block through the indexer
func (s *Service) ProcessBlock(ctx context.Context, blockNumber uint64) error {
	return s.indexer.ProcessBlock(ctx, blockNumber)
}
