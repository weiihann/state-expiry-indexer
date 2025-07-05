package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/weiihann/state-expiry-indexer/pkg/rpc"
	"github.com/weiihann/state-expiry-indexer/pkg/utils"
)

// RangeDiffs represents a block range with its state diffs (matches cmd/merge.go definition)
type RangeDiffs struct {
	BlockNum uint64                   `json:"blockNum"`
	Diffs    []*rpc.TransactionResult `json:"diffs"`
}

// RangeFileReader provides utilities for reading and processing range files
type RangeFileReader struct {
	decoder *utils.ZstdDecoder
}

// NewRangeFileReader creates a new range file reader with zstd decoder
func NewRangeFileReader() (*RangeFileReader, error) {
	decoder, err := utils.NewZstdDecoder()
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}

	return &RangeFileReader{
		decoder: decoder,
	}, nil
}

// Close properly closes the range file reader
func (r *RangeFileReader) Close() {
	if r.decoder != nil {
		r.decoder.Close()
	}
}

// ReadRangeFile reads and decompresses a range file, returning all block data
func (r *RangeFileReader) ReadRangeFile(filePath string) ([]RangeDiffs, error) {
	// Read compressed file from disk
	compressedData, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read range file %s: %w", filePath, err)
	}

	// Decompress the data
	decompressedData, err := r.decoder.Decompress(compressedData)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress range file %s: %w", filePath, err)
	}

	// Unmarshal JSON data
	var rangeDiffs []RangeDiffs
	if err := json.Unmarshal(decompressedData, &rangeDiffs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal range file %s: %w", filePath, err)
	}

	return rangeDiffs, nil
}

// ExtractBlockFromRange extracts state diff data for a specific block from range data
func (r *RangeFileReader) ExtractBlockFromRange(rangeDiffs []RangeDiffs, blockNum uint64) ([]*rpc.TransactionResult, error) {
	for _, rangeDiff := range rangeDiffs {
		if rangeDiff.BlockNum == blockNum {
			return rangeDiff.Diffs, nil
		}
	}

	return nil, fmt.Errorf("block %d not found in range data", blockNum)
}

// ScanRangeFiles scans a directory for range files and returns a map of block numbers to range file paths
func (r *RangeFileReader) ScanRangeFiles(dataDir string) (map[uint64]string, error) {
	blockToRangeFile := make(map[uint64]string)

	// Read directory entries
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", dataDir, err)
	}

	// Process each file
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()

		// Check if this is a range file (format: {start}_{end}.json.zst)
		if !strings.HasSuffix(filename, ".json.zst") {
			continue
		}

		// Remove the .json.zst extension
		nameWithoutExt := strings.TrimSuffix(filename, ".json.zst")

		// Check if it contains an underscore (range file format)
		if !strings.Contains(nameWithoutExt, "_") {
			continue
		}

		// Parse start and end block numbers
		parts := strings.Split(nameWithoutExt, "_")
		if len(parts) != 2 {
			continue
		}

		startBlock, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			continue
		}

		endBlock, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			continue
		}

		// Map each block number in the range to this file
		filePath := filepath.Join(dataDir, filename)
		for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
			blockToRangeFile[blockNum] = filePath
		}
	}

	return blockToRangeFile, nil
}

// ReadBlockFromRangeFile reads a specific block's data from a range file
func (r *RangeFileReader) ReadBlockFromRangeFile(filePath string, blockNum uint64) ([]*rpc.TransactionResult, error) {
	// Read and decompress the range file
	rangeDiffs, err := r.ReadRangeFile(filePath)
	if err != nil {
		return nil, err
	}

	// Extract the specific block's data
	return r.ExtractBlockFromRange(rangeDiffs, blockNum)
}
