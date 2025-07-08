package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/logger"
	"github.com/weiihann/state-expiry-indexer/pkg/rpc"
	"github.com/weiihann/state-expiry-indexer/pkg/utils"
)

var (
	mergeStartBlock uint64
	mergeEndBlock   uint64
	mergeRangeSize  uint64
	mergeDryRun     bool
	mergeNoCleanup  bool
	mergeRPC        bool
)

// RangeDiffs represents a block range with its state diffs
type RangeDiffs struct {
	BlockNum uint64                  `json:"blockNum"`
	Diffs    []rpc.TransactionResult `json:"diffs"`
}

var mergeCmd = &cobra.Command{
	Use:   "merge",
	Short: "Merge JSON state diff files by block range with compression",
	Long: `Merge individual JSON state diff files into compressed block ranges to optimize filesystem performance.
	
For each block in the range, the command will:
1. Check if {block}.json exists
2. If not, check if {block}.json.zst exists and decompress it
3. If neither exists, download the block via RPC
4. Aggregate all blocks into RangeDiffs structure
5. Compress and save as {start}_{end}.json.zst
6. Delete individual files after successful merge

Examples:
  # Merge a specific block range
  state-expiry-indexer merge --start-block 1000000 --end-block 2000000
  
  # Merge with custom range size (default 1000)
  state-expiry-indexer merge --start-block 1000000 --end-block 2000000 --range-size 500
  
  # Preview merge without actually doing it
  state-expiry-indexer merge --start-block 1000000 --end-block 2000000 --dry-run
  
  # Merge but keep individual files
  state-expiry-indexer merge --start-block 1000000 --end-block 2000000 --no-cleanup`,
	Run: mergeBlocks,
}

func mergeBlocks(cmd *cobra.Command, args []string) {
	log := logger.GetLogger("merge")

	// Load configuration
	config, err := internal.LoadConfig("./configs")
	if err != nil {
		log.Error("Configuration validation failed", "error", err)
		os.Exit(1)
	}

	// Validate input parameters
	if mergeStartBlock > mergeEndBlock {
		log.Error("Invalid block range", "start_block", mergeStartBlock, "end_block", mergeEndBlock)
		os.Exit(1)
	}

	totalBlocks := mergeEndBlock - mergeStartBlock + 1
	if totalBlocks > 1000000 {
		log.Warn("Large block range detected", "total_blocks", totalBlocks)
	}

	log.Info("Starting merge process",
		"start_block", mergeStartBlock,
		"end_block", mergeEndBlock,
		"range_size", mergeRangeSize,
		"total_blocks", totalBlocks,
		"data_dir", config.DataDir,
		"dry_run", mergeDryRun,
		"no_cleanup", mergeNoCleanup,
	)

	// Initialize RPC client for downloading missing blocks
	ctx := context.Background()
	var rpcClient *rpc.Client
	if len(config.RPCURLS) > 0 {
		client, err := rpc.NewClient(ctx, config.RPCURLS[0])
		if err != nil {
			log.Error("Failed to create RPC client", "error", err, "rpc_url", config.RPCURLS[0])
			os.Exit(1)
		}
		rpcClient = client
	} else {
		log.Warn("No RPC URLs configured, will not be able to download missing blocks")
	}

	// Initialize compression utilities
	encoder, err := utils.NewZstdEncoder()
	if err != nil {
		log.Error("Failed to create compression encoder", "error", err)
		os.Exit(1)
	}
	defer encoder.Close()

	decoder, err := utils.NewZstdDecoder()
	if err != nil {
		log.Error("Failed to create compression decoder", "error", err)
		os.Exit(1)
	}
	defer decoder.Close()

	// Process block ranges
	stats := processMergeRanges(log, config, rpcClient, encoder, decoder, ctx)

	// Log final statistics
	log.Info("Merge process completed",
		"total_ranges", stats.totalRanges,
		"successful_ranges", stats.successfulRanges,
		"failed_ranges", stats.failedRanges,
		"blocks_processed", stats.blocksProcessed,
		"blocks_downloaded", stats.blocksDownloaded,
		"files_cleaned", stats.filesCleaned,
		"original_size_mb", fmt.Sprintf("%.2f", float64(stats.originalSize)/1024/1024),
		"compressed_size_mb", fmt.Sprintf("%.2f", float64(stats.compressedSize)/1024/1024),
		"compression_ratio", fmt.Sprintf("%.2f%%", stats.compressionRatio))
}

type mergeStats struct {
	totalRanges      int
	successfulRanges int
	failedRanges     int
	blocksProcessed  int
	blocksDownloaded int
	filesCleaned     int
	originalSize     int64
	compressedSize   int64
	compressionRatio float64
}

func processMergeRanges(log *slog.Logger, config internal.Config, rpcClient *rpc.Client, encoder *utils.ZstdEncoder, decoder *utils.ZstdDecoder, ctx context.Context) mergeStats {
	stats := mergeStats{}

	// Calculate ranges to process
	currentStart := mergeStartBlock
	for currentStart <= mergeEndBlock {
		currentEnd := currentStart + mergeRangeSize - 1
		if currentEnd > mergeEndBlock {
			currentEnd = mergeEndBlock
		}

		stats.totalRanges++

		log.Info("Processing range",
			"range_start", currentStart,
			"range_end", currentEnd,
			"range_size", currentEnd-currentStart+1)

		if mergeDryRun {
			log.Info("DRY RUN - Would process range",
				"start", currentStart,
				"end", currentEnd,
				"output_file", fmt.Sprintf("%d_%d.json.zst", currentStart, currentEnd))
			stats.successfulRanges++
			currentStart = currentEnd + 1
			continue
		}

		// Process the range
		if err := processBlockRange(log, config, rpcClient, encoder, decoder, ctx, currentStart, currentEnd, &stats); err != nil {
			log.Error("Failed to process range", "start", currentStart, "end", currentEnd, "error", err)
			stats.failedRanges++
		} else {
			stats.successfulRanges++
		}

		currentStart = currentEnd + 1
	}

	// Calculate overall compression ratio
	if stats.originalSize > 0 {
		stats.compressionRatio = utils.GetCompressionRatio(int(stats.originalSize), int(stats.compressedSize))
	}

	return stats
}

func processBlockRange(log *slog.Logger, config internal.Config, rpcClient *rpc.Client, encoder *utils.ZstdEncoder, decoder *utils.ZstdDecoder, ctx context.Context, startBlock, endBlock uint64, stats *mergeStats) error {
	var rangeDiffs []RangeDiffs
	var filesToClean []uint64

	lastProgressTime := time.Now()
	lastProgressBlock := startBlock

	rangeFilename := fmt.Sprintf("%d_%d.json.zst", startBlock, endBlock)
	rangeFilePath := filepath.Join(config.DataDir, rangeFilename)
	if fileInfo, err := os.Stat(rangeFilePath); err == nil && fileInfo.Size() > 0 {
		log.Info("Range file already exists", "filename", rangeFilename)
		return nil
	}

	// Process each block in the range
	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		// Progress reporting every 100 blocks or 30 seconds
		now := time.Now()
		blocksSinceProgress := blockNum - lastProgressBlock
		timeSinceProgress := now.Sub(lastProgressTime).Seconds()

		if blocksSinceProgress >= 100 || timeSinceProgress >= 30 {
			log.Info("Range processing progress",
				"current_block", blockNum,
				"range_end", endBlock,
				"remaining", endBlock-blockNum)
			lastProgressTime = now
			lastProgressBlock = blockNum
		}

		// Get block data
		var blockData []rpc.TransactionResult
		var cleanupBlocks []uint64
		var err error
		if mergeRPC {
			blockData, err = getBlockDataFromRPC(log, rpcClient, ctx, blockNum)
			if err != nil {
				return fmt.Errorf("failed to get block data for block %d: %w", blockNum, err)
			}
		} else {
			blockData, cleanupBlocks, err = getBlockData(log, config, rpcClient, decoder, ctx, blockNum)
			if err != nil {
				return fmt.Errorf("failed to get block data for block %d: %w", blockNum, err)
			}
		}

		// Add block data to range
		rangeDiffs = append(rangeDiffs, RangeDiffs{
			BlockNum: blockNum,
			Diffs:    blockData,
		})

		// Track files to clean up and increment download counter if no files to clean (means RPC download)
		filesToClean = append(filesToClean, cleanupBlocks...)
		if len(cleanupBlocks) == 0 {
			stats.blocksDownloaded++
		}
		stats.blocksProcessed++
	}

	// Marshal range data to JSON
	rangeData, err := json.MarshalIndent(rangeDiffs, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal range data: %w", err)
	}

	// Compress the range data
	compressedData, err := encoder.Compress(rangeData)
	if err != nil {
		return fmt.Errorf("failed to compress range data: %w", err)
	}

	// Save compressed range file
	if err := os.WriteFile(rangeFilePath, compressedData, 0o644); err != nil {
		return fmt.Errorf("failed to write range file %s: %w", rangeFilename, err)
	}

	// Update statistics
	stats.originalSize += int64(len(rangeData))
	stats.compressedSize += int64(len(compressedData))

	log.Info("Successfully created range file",
		"filename", rangeFilename,
		"blocks", endBlock-startBlock+1,
		"original_size", len(rangeData),
		"compressed_size", len(compressedData),
		"compression_ratio", fmt.Sprintf("%.2f%%", utils.GetCompressionRatio(len(rangeData), len(compressedData))))

	// Clean up individual files if requested
	if !mergeNoCleanup {
		for _, block := range filesToClean {
			jsonFilename := fmt.Sprintf("%d.json", block)
			jsonFilePath := filepath.Join(config.DataDir, jsonFilename)
			if _, err := os.Stat(jsonFilePath); err == nil {
				if err := os.Remove(jsonFilePath); err != nil {
					log.Warn("Failed to remove file", "file", jsonFilePath, "error", err)
				} else {
					log.Debug("Cleaned up file", "file", jsonFilePath)
					stats.filesCleaned++
				}
			}

			compressedFilename := fmt.Sprintf("%d.json.zst", block)
			compressedFilePath := filepath.Join(config.DataDir, compressedFilename)
			if _, err := os.Stat(compressedFilePath); err == nil {
				if err := os.Remove(compressedFilePath); err != nil {
					log.Warn("Failed to remove file", "file", compressedFilePath, "error", err)
				} else {
					log.Debug("Cleaned up file", "file", compressedFilePath)
					stats.filesCleaned++
				}
			}
		}
	}

	return nil
}

func getBlockData(log *slog.Logger, config internal.Config, rpcClient *rpc.Client, decoder *utils.ZstdDecoder, ctx context.Context, blockNum uint64) ([]rpc.TransactionResult, []uint64, error) {
	var filesToClean []uint64

	// Check for uncompressed JSON file first
	jsonFilename := fmt.Sprintf("%d.json", blockNum)
	jsonFilePath := filepath.Join(config.DataDir, jsonFilename)

	if fileInfo, err := os.Stat(jsonFilePath); err == nil {
		// Sanity check: if file is empty, treat as corrupted and proceed to RPC
		if fileInfo.Size() == 0 {
			log.Warn("Found empty uncompressed file, treating as corrupted", "block", blockNum, "file", jsonFilename)
			// Don't add to cleanup list since we'll proceed to RPC download
		} else {
			// Read uncompressed JSON file
			data, err := os.ReadFile(jsonFilePath)
			if err != nil {
				log.Warn("Failed to read JSON file, proceeding to RPC", "block", blockNum, "file", jsonFilename, "error", err)
			} else {
				// Additional sanity check: ensure the data is valid JSON
				var transactionResults []rpc.TransactionResult
				if err := json.Unmarshal(data, &transactionResults); err != nil {
					log.Warn("Failed to unmarshal JSON data, treating as corrupted and proceeding to RPC", "block", blockNum, "file", jsonFilename, "error", err)
				} else {
					filesToClean = append(filesToClean, blockNum)
					log.Debug("Found valid uncompressed file", "block", blockNum, "file", jsonFilename, "transactions", len(transactionResults))
					return transactionResults, filesToClean, nil
				}
			}
		}
	}

	// Check for compressed JSON file
	compressedFilename := fmt.Sprintf("%d.json.zst", blockNum)
	compressedFilePath := filepath.Join(config.DataDir, compressedFilename)

	if fileInfo, err := os.Stat(compressedFilePath); err == nil {
		// Sanity check: if file is empty, treat as corrupted and proceed to RPC
		if fileInfo.Size() == 0 {
			log.Warn("Found empty compressed file, treating as corrupted", "block", blockNum, "file", compressedFilename)
			// Don't add to cleanup list since we'll proceed to RPC download
		} else {
			// Read and decompress file
			compressedData, err := os.ReadFile(compressedFilePath)
			if err != nil {
				log.Warn("Failed to read compressed file, proceeding to RPC", "block", blockNum, "file", compressedFilename, "error", err)
			} else {
				decompressedData, err := decoder.Decompress(compressedData)
				if err != nil {
					log.Warn("Failed to decompress file, treating as corrupted and proceeding to RPC", "block", blockNum, "file", compressedFilename, "error", err)
				} else {
					// Additional sanity check: ensure the decompressed data is valid JSON
					var transactionResults []rpc.TransactionResult
					if err := json.Unmarshal(decompressedData, &transactionResults); err != nil {
						log.Warn("Failed to unmarshal decompressed data, treating as corrupted and proceeding to RPC", "block", blockNum, "file", compressedFilename, "error", err)
					} else {
						filesToClean = append(filesToClean, blockNum)
						log.Debug("Found valid compressed file", "block", blockNum, "file", compressedFilename, "transactions", len(transactionResults))
						return transactionResults, filesToClean, nil
					}
				}
			}
		}
	}

	// Neither file exists or both files are corrupted, download via RPC
	if rpcClient == nil {
		return nil, nil, fmt.Errorf("block %d not found (or corrupted) and no RPC client available for download", blockNum)
	}

	log.Debug("Downloading block via RPC", "block", blockNum, "reason", "missing_or_corrupted_files")

	stateDiff, err := getBlockDataFromRPC(log, rpcClient, ctx, blockNum)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to download block %d via RPC: %w", blockNum, err)
	}

	return stateDiff, filesToClean, nil
}

func getBlockDataFromRPC(log *slog.Logger, rpcClient *rpc.Client, ctx context.Context, blockNum uint64) ([]rpc.TransactionResult, error) {
	blockBigInt := big.NewInt(int64(blockNum))
	stateDiff, err := rpcClient.GetStateDiff(ctx, blockBigInt)
	if err != nil {
		return nil, fmt.Errorf("failed to download block %d via RPC: %w", blockNum, err)
	}

	log.Debug("Successfully downloaded block via RPC", "block", blockNum, "transactions", len(stateDiff))
	return stateDiff, nil
}

func init() {
	mergeCmd.Flags().Uint64Var(&mergeStartBlock, "start-block", 1, "Start block number for merge range")
	mergeCmd.Flags().Uint64Var(&mergeEndBlock, "end-block", 1, "End block number for merge range")
	mergeCmd.Flags().Uint64Var(&mergeRangeSize, "range-size", 1000, "Number of blocks per merged range file")
	mergeCmd.Flags().BoolVar(&mergeDryRun, "dry-run", false, "Preview merge without actually doing it")
	mergeCmd.Flags().BoolVar(&mergeNoCleanup, "no-cleanup", false, "Keep individual files after merge")
	mergeCmd.Flags().BoolVar(&mergeRPC, "rpc", false, "Download blocks via RPC and skip file check")

	rootCmd.AddCommand(mergeCmd)
}
