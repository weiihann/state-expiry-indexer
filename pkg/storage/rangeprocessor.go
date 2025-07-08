package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"

	"github.com/weiihann/state-expiry-indexer/pkg/rpc"
	"github.com/weiihann/state-expiry-indexer/pkg/utils"
)

// RangeProcessor handles downloading and processing of block ranges
type RangeProcessor struct {
	dataDir   string
	rpcClient rpc.ClientInterface
	rangeSize int
	encoder   *utils.ZstdEncoder
	decoder   *utils.ZstdDecoder
}

// NewRangeProcessor creates a new range processor
func NewRangeProcessor(dataDir string, rpcClient rpc.ClientInterface, rangeSize int) (*RangeProcessor, error) {
	encoder, err := utils.NewZstdEncoder()
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
	}

	decoder, err := utils.NewZstdDecoder()
	if err != nil {
		encoder.Close()
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}

	return &RangeProcessor{
		dataDir:   dataDir,
		rpcClient: rpcClient,
		rangeSize: rangeSize,
		encoder:   encoder,
		decoder:   decoder,
	}, nil
}

// Close properly closes the range processor resources
func (rp *RangeProcessor) Close() {
	if rp.encoder != nil {
		rp.encoder.Close()
	}
	if rp.decoder != nil {
		rp.decoder.Close()
	}
}

// GetRangeNumber calculates the range number for a given block
func (rp *RangeProcessor) GetRangeNumber(blockNumber uint64) uint64 {
	if blockNumber == 0 {
		return 0
	}
	return (blockNumber - 1) / uint64(rp.rangeSize)
}

// GetRangeBlockNumbers returns the start and end block numbers for a range
func (rp *RangeProcessor) GetRangeBlockNumbers(rangeNumber uint64) (uint64, uint64) {
	if rangeNumber == 0 {
		return 0, 0 // Genesis is handled separately
	}
	start := (rangeNumber-1)*uint64(rp.rangeSize) + 1
	end := start + uint64(rp.rangeSize) - 1
	return start, end
}

// GetRangeFilePath returns the file path for a range
func (rp *RangeProcessor) GetRangeFilePath(rangeNumber uint64) string {
	if rangeNumber == 0 {
		return "" // Genesis doesn't have a range file
	}
	start, end := rp.GetRangeBlockNumbers(rangeNumber)
	filename := fmt.Sprintf("%d_%d.json.zst", start, end)
	return filepath.Join(rp.dataDir, filename)
}

// RangeExists checks if a range file exists
func (rp *RangeProcessor) RangeExists(rangeNumber uint64) bool {
	if rangeNumber == 0 {
		return true // Genesis is always considered to exist
	}
	filePath := rp.GetRangeFilePath(rangeNumber)
	_, err := os.Stat(filePath)
	return err == nil
}

// DownloadRange downloads all blocks in a range and saves as a compressed range file
func (rp *RangeProcessor) DownloadRange(ctx context.Context, rangeNumber uint64) error {
	if rangeNumber == 0 {
		return fmt.Errorf("cannot download genesis range, use genesis processing instead")
	}

	start, end := rp.GetRangeBlockNumbers(rangeNumber)
	rangeFilePath := rp.GetRangeFilePath(rangeNumber)

	// Check if file already exists
	if rp.RangeExists(rangeNumber) {
		return nil
	}

	var rangeDiffs []RangeDiffs

	// Download each block in the range
	for blockNum := start; blockNum <= end; blockNum++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Download state diff for this block
		blockBigInt := big.NewInt(int64(blockNum))
		stateDiff, err := rp.rpcClient.GetStateDiff(ctx, blockBigInt)
		if err != nil {
			return fmt.Errorf("failed to download block %d: %w", blockNum, err)
		}

		// Convert to the expected format
		var transactionResults []*rpc.TransactionResult
		for i := range stateDiff {
			transactionResults = append(transactionResults, &stateDiff[i])
		}

		// Add to range data
		rangeDiffs = append(rangeDiffs, RangeDiffs{
			BlockNum: blockNum,
			Diffs:    transactionResults,
		})
	}

	// Marshal range data to JSON
	rangeData, err := json.MarshalIndent(rangeDiffs, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal range data: %w", err)
	}

	// Compress the range data
	compressedData, err := rp.encoder.Compress(rangeData)
	if err != nil {
		return fmt.Errorf("failed to compress range data: %w", err)
	}

	// Save compressed range file
	if err := os.WriteFile(rangeFilePath, compressedData, 0o644); err != nil {
		return fmt.Errorf("failed to write range file %s: %w", rangeFilePath, err)
	}

	return nil
}

// ReadRange reads and decompresses a range file
func (rp *RangeProcessor) ReadRange(rangeNumber uint64) ([]RangeDiffs, error) {
	if rangeNumber == 0 {
		return nil, fmt.Errorf("cannot read genesis as range, use genesis processing instead")
	}

	rangeFilePath := rp.GetRangeFilePath(rangeNumber)

	// Read compressed file from disk
	compressedData, err := os.ReadFile(rangeFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read range file %s: %w", rangeFilePath, err)
	}

	// Decompress the data
	decompressedData, err := rp.decoder.Decompress(compressedData)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress range file %s: %w", rangeFilePath, err)
	}

	// Unmarshal JSON data
	var rangeDiffs []RangeDiffs
	if err := json.Unmarshal(decompressedData, &rangeDiffs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal range file %s: %w", rangeFilePath, err)
	}

	return rangeDiffs, nil
}

// EnsureRangeExists ensures a range file exists, downloading it if necessary
func (rp *RangeProcessor) EnsureRangeExists(ctx context.Context, rangeNumber uint64) error {
	if rangeNumber == 0 {
		return nil // Genesis is handled separately
	}

	if !rp.RangeExists(rangeNumber) {
		return rp.DownloadRange(ctx, rangeNumber)
	}

	return nil
}
