package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/logger"
	"github.com/weiihann/state-expiry-indexer/pkg/utils"
)

var (
	compressStartBlock     uint64
	compressEndBlock       uint64
	compressAll            bool
	compressDryRun         bool
	compressOverwrite      bool
	compressDeleteOriginal bool
)

var compressCmd = &cobra.Command{
	Use:   "compress",
	Short: "Compress existing JSON state diff files to zstd format",
	Long: `Compress existing JSON state diff files to zstd format to save storage space.
	
Examples:
  # Compress a specific block range
  state-expiry-indexer compress --start-block 1000000 --end-block 2000000
  
  # Compress all JSON files in the data directory
  state-expiry-indexer compress --all
  
  # Preview what would be compressed without actually doing it
  state-expiry-indexer compress --all --dry-run
  
  # Overwrite existing .json.zst files
  state-expiry-indexer compress --all --overwrite`,
	Run: compress,
}

func compress(cmd *cobra.Command, args []string) {
	log := logger.GetLogger("compress")

	// Load configuration
	config, err := internal.LoadConfig("./configs")
	if err != nil {
		log.Error("Configuration validation failed", "error", err)
		os.Exit(1)
	}

	log.Info("Starting compression process",
		"data_dir", config.DataDir,
		"dry_run", compressDryRun,
		"overwrite", compressOverwrite,
		"delete", compressDeleteOriginal,
	)

	// Determine which files to compress
	var filesToCompress []string
	var err2 error

	if compressAll {
		filesToCompress, err2 = getAllJSONFiles(config.DataDir)
		if err2 != nil {
			log.Error("Failed to scan for JSON files", "error", err2)
			os.Exit(1)
		}
		log.Info("Found JSON files to compress", "count", len(filesToCompress))
	} else {
		filesToCompress, err2 = getJSONFilesInRange(config.DataDir, compressStartBlock, compressEndBlock)
		if err2 != nil {
			log.Error("Failed to get files in range", "error", err2)
			os.Exit(1)
		}
		log.Info("Found JSON files in range to compress",
			"start_block", compressStartBlock,
			"end_block", compressEndBlock,
			"count", len(filesToCompress))
	}

	if len(filesToCompress) == 0 {
		log.Info("No JSON files found to compress")
		return
	}

	// Show dry run preview
	if compressDryRun {
		log.Info("DRY RUN - Files that would be compressed:")
		for _, file := range filesToCompress {
			compressedFile := file + ".zst"
			if _, err := os.Stat(compressedFile); err == nil && !compressOverwrite {
				log.Info("Would skip (already exists)", "file", file, "compressed", compressedFile)
			} else {
				log.Info("Would compress", "file", file, "compressed", compressedFile)
			}
		}
		log.Info("Dry run completed", "total_files", len(filesToCompress))
		return
	}

	// Perform actual compression
	compressionStats := performCompression(log, config.DataDir, filesToCompress, compressDeleteOriginal)

	// Log final statistics
	log.Info("Compression completed successfully",
		"total_files", compressionStats.totalFiles,
		"compressed_files", compressionStats.compressedFiles,
		"skipped_files", compressionStats.skippedFiles,
		"failed_files", compressionStats.failedFiles,
		"original_size_mb", fmt.Sprintf("%.2f", float64(compressionStats.originalSize)/1024/1024),
		"compressed_size_mb", fmt.Sprintf("%.2f", float64(compressionStats.compressedSize)/1024/1024),
		"space_saved_mb", fmt.Sprintf("%.2f", float64(compressionStats.originalSize-compressionStats.compressedSize)/1024/1024),
		"compression_ratio", fmt.Sprintf("%.2f%%", compressionStats.compressionRatio))
}

type compressionStats struct {
	totalFiles       int
	compressedFiles  int
	skippedFiles     int
	failedFiles      int
	originalSize     int64
	compressedSize   int64
	compressionRatio float64
}

func performCompression(log *slog.Logger, dataDir string, files []string, deleteOriginal bool) compressionStats {
	stats := compressionStats{totalFiles: len(files)}
	lastProgressTime := time.Now()
	lastProgressCount := 0
	encoder, err := utils.NewZstdEncoder()
	if err != nil {
		log.Error("Failed to create zstd encoder", "error", err)
		os.Exit(1)
	}
	defer encoder.Close()

	for i, file := range files {
		// Progress reporting every 1000 files or 30 seconds
		now := time.Now()
		filesSinceProgress := i - lastProgressCount
		timeSinceProgress := now.Sub(lastProgressTime).Seconds()

		if filesSinceProgress >= 1000 || timeSinceProgress >= 30 {
			log.Info("Compression progress",
				"processed", i,
				"total", len(files),
				"percentage", fmt.Sprintf("%.1f%%", float64(i)/float64(len(files))*100))
			lastProgressTime = now
			lastProgressCount = i
		}

		// Check if compressed file already exists
		compressedFile := file + ".zst"
		if _, err := os.Stat(compressedFile); err == nil && !compressOverwrite {
			log.Debug("Skipping file (already compressed)", "file", file)
			stats.skippedFiles++
			if deleteOriginal {
				if _, err := os.Stat(filepath.Join(dataDir, file)); err == nil {
					if err := os.Remove(filepath.Join(dataDir, file)); err != nil {
						log.Error("Failed to delete original file", "file", file, "error", err)
						stats.failedFiles++
						continue
					}
				}
			}
			continue
		}

		// Read original file
		originalData, err := os.ReadFile(filepath.Join(dataDir, file))
		if err != nil {
			log.Error("Failed to read file", "file", file, "error", err)
			stats.failedFiles++
			continue
		}

		// Compress the data
		compressedData, err := encoder.Compress(originalData)
		if err != nil {
			log.Error("Failed to compress file", "file", file, "error", err)
			stats.failedFiles++
			continue
		}

		// Write compressed file
		compressedFilePath := filepath.Join(dataDir, compressedFile)
		if err := os.WriteFile(compressedFilePath, compressedData, 0o644); err != nil {
			log.Error("Failed to write compressed file", "file", compressedFile, "error", err)
			stats.failedFiles++
			continue
		}

		if deleteOriginal {
			if err := os.Remove(filepath.Join(dataDir, file)); err != nil {
				log.Error("Failed to delete original file", "file", file, "error", err)
				stats.failedFiles++
				continue
			}
		}

		// Update statistics
		stats.compressedFiles++
		stats.originalSize += int64(len(originalData))
		stats.compressedSize += int64(len(compressedData))

		log.Debug("Successfully compressed file",
			"file", file,
			"original_size", len(originalData),
			"compressed_size", len(compressedData),
			"ratio", fmt.Sprintf("%.2f%%", utils.GetCompressionRatio(len(originalData), len(compressedData))))
	}

	// Calculate overall compression ratio
	if stats.originalSize > 0 {
		stats.compressionRatio = utils.GetCompressionRatio(int(stats.originalSize), int(stats.compressedSize))
	}

	return stats
}

func getAllJSONFiles(dataDir string) ([]string, error) {
	var files []string

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read data directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			files = append(files, entry.Name())
		}
	}

	return files, nil
}

func getJSONFilesInRange(dataDir string, startBlock, endBlock uint64) ([]string, error) {
	var files []string

	for block := startBlock; block <= endBlock; block++ {
		filename := fmt.Sprintf("%d.json", block)
		filePath := filepath.Join(dataDir, filename)

		if _, err := os.Stat(filePath); err == nil {
			files = append(files, filename)
		} else if os.IsNotExist(err) {
			// File doesn't exist, skip it
			continue
		} else {
			// Other error
			return nil, fmt.Errorf("failed to check file %s: %w", filename, err)
		}
	}

	return files, nil
}

func init() {
	compressCmd.Flags().Uint64Var(&compressStartBlock, "start-block", 1, "Start block number for compression range")
	compressCmd.Flags().Uint64Var(&compressEndBlock, "end-block", 1, "End block number for compression range")
	compressCmd.Flags().BoolVar(&compressAll, "all", false, "Compress all JSON files in the data directory")
	compressCmd.Flags().BoolVar(&compressDryRun, "dry-run", false, "Preview what would be compressed without actually doing it")
	compressCmd.Flags().BoolVar(&compressOverwrite, "overwrite", false, "Overwrite existing .json.zst files")
	compressCmd.Flags().BoolVar(&compressDeleteOriginal, "delete", false, "Delete original JSON files after compression")

	// Mark flags as mutually exclusive
	compressCmd.MarkFlagsMutuallyExclusive("all", "start-block")
	compressCmd.MarkFlagsMutuallyExclusive("all", "end-block")

	rootCmd.AddCommand(compressCmd)
}
