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
	"github.com/weiihann/state-expiry-indexer/pkg/utils"
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

	accessedAccounts := make(map[string]bool, len(genesis.Alloc))
	for acc, alloc := range genesis.Alloc {
		// Check if this genesis account has code (is a contract)
		isContract := len(alloc.Code) > 0
		accessedAccounts[acc.String()] = isContract
	}

	return i.repo.UpdateBlockDataInTx(ctx, 0, accessedAccounts, nil)
}

func (i *Indexer) ProcessBlock(ctx context.Context, blockNumber uint64) error {
	// Smart file detection: check for .json.zst first, fallback to .json
	filePath, isCompressed, err := i.findBlockFile(blockNumber)
	if err != nil {
		return fmt.Errorf("could not find state diff file for block %d: %w", blockNumber, err)
	}

	i.log.Debug("Found block file",
		"block_number", blockNumber,
		"file_path", filePath,
		"is_compressed", isCompressed)

	data, err := i.readBlockFile(filePath, isCompressed)
	if err != nil {
		return fmt.Errorf("could not read state diff file for block %d: %w", blockNumber, err)
	}

	var stateDiffs []rpc.TransactionResult
	if err := json.Unmarshal(data, &stateDiffs); err != nil {
		return fmt.Errorf("could not unmarshal state diff for block %d: %w", blockNumber, err)
	}

	i.log.Debug("Processing block",
		"block_number", blockNumber,
		"transaction_count", len(stateDiffs),
		"compressed", isCompressed)

	accessedAccounts := make(map[string]bool)
	accessedStorage := make(map[string]map[string]struct{})

	for _, txResult := range stateDiffs {
		for addr, diff := range txResult.StateDiff {
			// Determine if this account is a contract based on state diff
			accessedAccounts[addr] = i.determineAccountType(diff)

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

// checkBlockFileExists checks if a block file exists in either compressed or uncompressed format
func (s *Service) checkBlockFileExists(blockNumber uint64) (bool, string, bool) {
	baseFileName := fmt.Sprintf("%d.json", blockNumber)

	// Check for compressed file first (.json.zst)
	compressedPath := filepath.Join(s.indexer.Path, baseFileName+".zst")
	if _, err := os.Stat(compressedPath); err == nil {
		return true, compressedPath, true
	}

	// Fallback to uncompressed file (.json)
	uncompressedPath := filepath.Join(s.indexer.Path, baseFileName)
	if _, err := os.Stat(uncompressedPath); err == nil {
		return true, uncompressedPath, false
	}

	return false, "", false
}

// findBlockFile finds the block file, checking for .json.zst first, then .json
func (i *Indexer) findBlockFile(blockNumber uint64) (string, bool, error) {
	baseFileName := fmt.Sprintf("%d.json", blockNumber)

	// Check for compressed file first (.json.zst)
	compressedPath := filepath.Join(i.Path, baseFileName+".zst")
	if _, err := os.Stat(compressedPath); err == nil {
		return compressedPath, true, nil
	}

	// Fallback to uncompressed file (.json)
	uncompressedPath := filepath.Join(i.Path, baseFileName)
	if _, err := os.Stat(uncompressedPath); err == nil {
		return uncompressedPath, false, nil
	}

	return "", false, fmt.Errorf("no file found for block %d (checked both .json and .json.zst)", blockNumber)
}

// readBlockFile reads and optionally decompresses block file data
func (i *Indexer) readBlockFile(filePath string, isCompressed bool) ([]byte, error) {
	// Read file from disk
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	if !isCompressed {
		// Uncompressed JSON file - return as-is
		return data, nil
	}

	// Compressed file - decompress in memory
	i.log.Debug("Decompressing file",
		"file_path", filePath,
		"compressed_size", len(data))

	decoder, err := utils.NewZstdDecoder()
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}
	defer decoder.Close()

	decompressed, err := decoder.Decompress(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress file %s: %w", filePath, err)
	}

	i.log.Debug("Successfully decompressed file",
		"file_path", filePath,
		"compressed_size", len(data),
		"decompressed_size", len(decompressed),
		"compression_ratio", fmt.Sprintf("%.2f%%", utils.GetCompressionRatio(len(decompressed), len(data))))

	return decompressed, nil
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

		// Check if either .json.zst or .json file exists for this block
		hasFile, filePath, isCompressed := s.checkBlockFileExists(currentBlock)
		if !hasFile {
			// No more files available to process
			break
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
			"file_path", filePath,
			"compressed", isCompressed)

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
