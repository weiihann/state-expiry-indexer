package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/weiihann/state-expiry-indexer/pkg/rpc"
	"github.com/weiihann/state-expiry-indexer/pkg/utils"
)

func TestRangeFileReader(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "rangefile_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test data
	testRangeDiffs := []RangeDiffs{
		{
			BlockNum: 1000,
			Diffs: []*rpc.TransactionResult{
				{
					StateDiff: map[string]rpc.AccountDiff{
						"0x1234": {
							Balance: map[string]interface{}{"*": map[string]interface{}{"from": "0x0", "to": "0x1000"}},
						},
					},
				},
			},
		},
		{
			BlockNum: 1001,
			Diffs: []*rpc.TransactionResult{
				{
					StateDiff: map[string]rpc.AccountDiff{
						"0x5678": {
							Nonce: map[string]interface{}{"*": map[string]interface{}{"from": "0x0", "to": "0x1"}},
						},
					},
				},
			},
		},
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(testRangeDiffs)
	if err != nil {
		t.Fatalf("Failed to marshal test data: %v", err)
	}

	// Compress the data
	encoder, err := utils.NewZstdEncoder()
	if err != nil {
		t.Fatalf("Failed to create encoder: %v", err)
	}
	defer encoder.Close()

	compressedData, err := encoder.Compress(jsonData)
	if err != nil {
		t.Fatalf("Failed to compress data: %v", err)
	}

	// Write compressed range file
	rangeFilePath := filepath.Join(tempDir, "1000_1001.json.zst")
	err = os.WriteFile(rangeFilePath, compressedData, 0o644)
	if err != nil {
		t.Fatalf("Failed to write range file: %v", err)
	}

	// Test RangeFileReader
	reader, err := NewRangeFileReader()
	if err != nil {
		t.Fatalf("Failed to create range file reader: %v", err)
	}
	defer reader.Close()

	// Test ReadRangeFile
	rangeDiffs, err := reader.ReadRangeFile(rangeFilePath)
	if err != nil {
		t.Fatalf("Failed to read range file: %v", err)
	}

	if len(rangeDiffs) != 2 {
		t.Errorf("Expected 2 blocks, got %d", len(rangeDiffs))
	}

	// Test ExtractBlockFromRange
	block1000, err := reader.ExtractBlockFromRange(rangeDiffs, 1000)
	if err != nil {
		t.Fatalf("Failed to extract block 1000: %v", err)
	}

	if len(block1000) != 1 {
		t.Errorf("Expected 1 transaction for block 1000, got %d", len(block1000))
	}

	// Test ScanRangeFiles
	rangeFileMap, err := reader.ScanRangeFiles(tempDir)
	if err != nil {
		t.Fatalf("Failed to scan range files: %v", err)
	}

	if len(rangeFileMap) != 2 {
		t.Errorf("Expected 2 blocks in range file map, got %d", len(rangeFileMap))
	}

	// Verify block mapping
	if rangeFileMap[1000] != rangeFilePath {
		t.Errorf("Block 1000 not mapped to correct file")
	}

	if rangeFileMap[1001] != rangeFilePath {
		t.Errorf("Block 1001 not mapped to correct file")
	}

	// Test ReadBlockFromRangeFile
	blockData, err := reader.ReadBlockFromRangeFile(rangeFilePath, 1001)
	if err != nil {
		t.Fatalf("Failed to read block from range file: %v", err)
	}

	if len(blockData) != 1 {
		t.Errorf("Expected 1 transaction for block 1001, got %d", len(blockData))
	}

	// Test error case - block not found
	_, err = reader.ExtractBlockFromRange(rangeDiffs, 9999)
	if err == nil {
		t.Error("Expected error for non-existent block, got nil")
	}
}

func TestScanRangeFiles_MultipleFiles(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "rangefile_scan_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create multiple range files
	rangeFiles := []string{
		"1000_1999.json.zst",
		"2000_2999.json.zst",
		"3000_3499.json.zst",
		"some_other_file.txt", // Should be ignored
		"4000.json.zst",       // Individual file, should be ignored
	}

	for _, filename := range rangeFiles {
		filePath := filepath.Join(tempDir, filename)
		err := os.WriteFile(filePath, []byte("dummy"), 0o644)
		if err != nil {
			t.Fatalf("Failed to create file %s: %v", filename, err)
		}
	}

	// Test scanning
	reader, err := NewRangeFileReader()
	if err != nil {
		t.Fatalf("Failed to create range file reader: %v", err)
	}
	defer reader.Close()

	rangeFileMap, err := reader.ScanRangeFiles(tempDir)
	if err != nil {
		t.Fatalf("Failed to scan range files: %v", err)
	}

	expectedBlocks := 1000 + 1000 + 500 // Three range files
	if len(rangeFileMap) != expectedBlocks {
		t.Errorf("Expected %d blocks in range file map, got %d", expectedBlocks, len(rangeFileMap))
	}

	// Verify some specific mappings
	if rangeFileMap[1000] != filepath.Join(tempDir, "1000_1999.json.zst") {
		t.Error("Block 1000 not mapped correctly")
	}

	if rangeFileMap[2500] != filepath.Join(tempDir, "2000_2999.json.zst") {
		t.Error("Block 2500 not mapped correctly")
	}

	if rangeFileMap[3499] != filepath.Join(tempDir, "3000_3499.json.zst") {
		t.Error("Block 3499 not mapped correctly")
	}

	// Verify non-range files are ignored
	if _, exists := rangeFileMap[4000]; exists {
		t.Error("Individual file should not be in range file map")
	}
}
