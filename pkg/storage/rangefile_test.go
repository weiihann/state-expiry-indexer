package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

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

// TestRangeFileCompressionValidation tests compression and validation scenarios
func TestRangeFileCompressionValidation(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "rangefile_compression_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create realistic test data based on the example.json format
	testRangeDiffs := []RangeDiffs{
		{
			BlockNum: 5000001,
			Diffs: []*rpc.TransactionResult{
				{
					TxHash: "0x49ccf4cc91d52f01b84773e73e21c327cfcc4e6b22471470b9c68d0fbbf66288",
					StateDiff: map[string]rpc.AccountDiff{
						"0x0baa3a6fa67de805ad5760d2a1e82b87b14e9365": {
							Balance: map[string]interface{}{
								"*": map[string]interface{}{
									"from": "0x549d40dfafc00",
									"to":   "0x4c2acb18094c00",
								},
							},
							Code:    "=",
							Nonce:   "=",
							Storage: map[string]interface{}{},
						},
						"0x5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c": {
							Balance: map[string]interface{}{
								"*": map[string]interface{}{
									"from": "0x693c124a2b710860c0",
									"to":   "0x693c19c01bcb0fa0c0",
								},
							},
							Code:    "=",
							Nonce:   "=",
							Storage: map[string]interface{}{},
						},
					},
				},
			},
		},
		{
			BlockNum: 5000002,
			Diffs: []*rpc.TransactionResult{
				{
					TxHash: "0x49ccf4cc91d52f01b84773e73e21c327cfcc4e6b22471470b9c68d0fbbf66288",
					StateDiff: map[string]rpc.AccountDiff{
						"0x5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c": {
							Balance: map[string]interface{}{
								"*": map[string]interface{}{
									"from": "0x693c124a2b710860c0",
									"to":   "0x693c19c01bcb0fa0c0",
								},
							},
							Code:  "=",
							Nonce: "=",
							Storage: map[string]interface{}{
								"0xe1f979c68554698fa8bf9552587bcd354b4ed0ddf809ee5e2ae60bfa0785ef74": map[string]interface{}{
									"*": map[string]interface{}{
										"from": "0x000000000000000000000000000000000000000000000000b469471f80140000",
										"to":   "0x00000000000000000000000000000000000000000000000e41dbb290f7bc0000",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	t.Run("Compression effectiveness", func(t *testing.T) {
		// Marshal to JSON
		jsonData, err := json.Marshal(testRangeDiffs)
		if err != nil {
			t.Fatalf("Failed to marshal test data: %v", err)
		}

		// Create encoder
		encoder, err := utils.NewZstdEncoder()
		if err != nil {
			t.Fatalf("Failed to create encoder: %v", err)
		}
		defer encoder.Close()

		// Compress the data
		compressedData, err := encoder.Compress(jsonData)
		if err != nil {
			t.Fatalf("Failed to compress data: %v", err)
		}

		// Verify compression ratio
		ratio := utils.GetCompressionRatio(len(jsonData), len(compressedData))
		if ratio <= 0 {
			t.Errorf("Expected positive compression ratio, got %f", ratio)
		}

		t.Logf("Compression ratio: %.2f%% (Original: %d bytes, Compressed: %d bytes)",
			ratio, len(jsonData), len(compressedData))

		// Validate compressed data
		if err := utils.ValidateCompressedData(compressedData); err != nil {
			t.Errorf("Compressed data validation failed: %v", err)
		}
	})

	t.Run("Decompression verification", func(t *testing.T) {
		// Create a complete compress/decompress cycle
		jsonData, err := json.Marshal(testRangeDiffs)
		if err != nil {
			t.Fatalf("Failed to marshal test data: %v", err)
		}

		// Compress
		encoder, err := utils.NewZstdEncoder()
		if err != nil {
			t.Fatalf("Failed to create encoder: %v", err)
		}
		defer encoder.Close()

		compressedData, err := encoder.Compress(jsonData)
		if err != nil {
			t.Fatalf("Failed to compress data: %v", err)
		}

		// Decompress
		decoder, err := utils.NewZstdDecoder()
		if err != nil {
			t.Fatalf("Failed to create decoder: %v", err)
		}
		defer decoder.Close()

		decompressedData, err := decoder.Decompress(compressedData)
		if err != nil {
			t.Fatalf("Failed to decompress data: %v", err)
		}

		// Verify data integrity
		if string(jsonData) != string(decompressedData) {
			t.Error("Decompressed data does not match original")
		}

		// Verify we can unmarshal back to original structure
		var restored []RangeDiffs
		if err := json.Unmarshal(decompressedData, &restored); err != nil {
			t.Fatalf("Failed to unmarshal decompressed data: %v", err)
		}

		if len(restored) != len(testRangeDiffs) {
			t.Errorf("Expected %d blocks, got %d", len(testRangeDiffs), len(restored))
		}

		// Verify specific content
		if restored[0].BlockNum != 5000001 {
			t.Errorf("Expected block 5000001, got %d", restored[0].BlockNum)
		}

		if restored[1].BlockNum != 5000002 {
			t.Errorf("Expected block 5000002, got %d", restored[1].BlockNum)
		}
	})
}

// TestRangeFileErrorHandling tests various error scenarios
func TestRangeFileErrorHandling(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "rangefile_error_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	reader, err := NewRangeFileReader()
	if err != nil {
		t.Fatalf("Failed to create range file reader: %v", err)
	}
	defer reader.Close()

	t.Run("Non-existent file", func(t *testing.T) {
		_, err := reader.ReadRangeFile("/non/existent/file.json.zst")
		if err == nil {
			t.Error("Expected error for non-existent file")
		}
	})

	t.Run("Invalid compressed data", func(t *testing.T) {
		// Create a file with invalid compressed data
		invalidFile := filepath.Join(tempDir, "invalid.json.zst")
		err := os.WriteFile(invalidFile, []byte("invalid compressed data"), 0o644)
		if err != nil {
			t.Fatalf("Failed to create invalid file: %v", err)
		}

		_, err = reader.ReadRangeFile(invalidFile)
		if err == nil {
			t.Error("Expected error for invalid compressed data")
		}
	})

	t.Run("Invalid JSON after decompression", func(t *testing.T) {
		// Create valid compressed data but with invalid JSON
		encoder, err := utils.NewZstdEncoder()
		if err != nil {
			t.Fatalf("Failed to create encoder: %v", err)
		}
		defer encoder.Close()

		invalidJSON := []byte("{invalid json}")
		compressedData, err := encoder.Compress(invalidJSON)
		if err != nil {
			t.Fatalf("Failed to compress invalid JSON: %v", err)
		}

		invalidJSONFile := filepath.Join(tempDir, "invalid_json.json.zst")
		err = os.WriteFile(invalidJSONFile, compressedData, 0o644)
		if err != nil {
			t.Fatalf("Failed to create invalid JSON file: %v", err)
		}

		_, err = reader.ReadRangeFile(invalidJSONFile)
		if err == nil {
			t.Error("Expected error for invalid JSON")
		}
	})

	t.Run("Empty file", func(t *testing.T) {
		emptyFile := filepath.Join(tempDir, "empty.json.zst")
		err := os.WriteFile(emptyFile, []byte{}, 0o644)
		if err != nil {
			t.Fatalf("Failed to create empty file: %v", err)
		}

		_, err = reader.ReadRangeFile(emptyFile)
		if err == nil {
			t.Error("Expected error for empty file")
		}
	})

	t.Run("Block not found in range", func(t *testing.T) {
		// Create valid range data
		testRangeDiffs := []RangeDiffs{
			{BlockNum: 1000, Diffs: []*rpc.TransactionResult{}},
			{BlockNum: 1001, Diffs: []*rpc.TransactionResult{}},
		}

		_, err := reader.ExtractBlockFromRange(testRangeDiffs, 9999)
		if err == nil {
			t.Error("Expected error for non-existent block")
		}
	})

	t.Run("Directory read error", func(t *testing.T) {
		// Try to scan a non-existent directory
		_, err := reader.ScanRangeFiles("/non/existent/directory")
		if err == nil {
			t.Error("Expected error for non-existent directory")
		}
	})
}

// TestRangeFileSystemIntegration tests file system integration scenarios
func TestRangeFileSystemIntegration(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "rangefile_fs_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	reader, err := NewRangeFileReader()
	if err != nil {
		t.Fatalf("Failed to create range file reader: %v", err)
	}
	defer reader.Close()

	// Create test data
	testRangeDiffs := []RangeDiffs{
		{BlockNum: 2000, Diffs: []*rpc.TransactionResult{{TxHash: "0x123"}}},
		{BlockNum: 2001, Diffs: []*rpc.TransactionResult{{TxHash: "0x456"}}},
	}

	// Helper function to create compressed range file
	createRangeFile := func(filename string, data []RangeDiffs) string {
		jsonData, err := json.Marshal(data)
		if err != nil {
			t.Fatalf("Failed to marshal data: %v", err)
		}

		encoder, err := utils.NewZstdEncoder()
		if err != nil {
			t.Fatalf("Failed to create encoder: %v", err)
		}
		defer encoder.Close()

		compressedData, err := encoder.Compress(jsonData)
		if err != nil {
			t.Fatalf("Failed to compress data: %v", err)
		}

		filePath := filepath.Join(tempDir, filename)
		err = os.WriteFile(filePath, compressedData, 0o644)
		if err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}

		return filePath
	}

	t.Run("Multiple range files", func(t *testing.T) {
		// Create multiple range files
		file1 := createRangeFile("2000_2001.json.zst", testRangeDiffs)
		file2 := createRangeFile("3000_3001.json.zst", []RangeDiffs{
			{BlockNum: 3000, Diffs: []*rpc.TransactionResult{{TxHash: "0x789"}}},
			{BlockNum: 3001, Diffs: []*rpc.TransactionResult{{TxHash: "0xabc"}}},
		})

		// Scan directory
		rangeFileMap, err := reader.ScanRangeFiles(tempDir)
		if err != nil {
			t.Fatalf("Failed to scan range files: %v", err)
		}

		// Verify mappings
		if len(rangeFileMap) != 4 {
			t.Errorf("Expected 4 blocks, got %d", len(rangeFileMap))
		}

		if rangeFileMap[2000] != file1 {
			t.Error("Block 2000 not mapped correctly")
		}

		if rangeFileMap[3000] != file2 {
			t.Error("Block 3000 not mapped correctly")
		}
	})

	t.Run("File permissions", func(t *testing.T) {
		// Create a file with restricted permissions
		restrictedFile := filepath.Join(tempDir, "restricted.json.zst")
		err := os.WriteFile(restrictedFile, []byte("test"), 0o000)
		if err != nil {
			t.Fatalf("Failed to create restricted file: %v", err)
		}

		// Try to read it
		_, err = reader.ReadRangeFile(restrictedFile)
		if err == nil {
			t.Error("Expected error for restricted file")
		}

		// Clean up - restore permissions to allow cleanup
		os.Chmod(restrictedFile, 0o644)
	})

	t.Run("Symbolic links", func(t *testing.T) {
		// Create original file
		originalFile := createRangeFile("original.json.zst", testRangeDiffs)

		// Create symbolic link
		linkFile := filepath.Join(tempDir, "link.json.zst")
		err := os.Symlink(originalFile, linkFile)
		if err != nil {
			t.Skipf("Skipping symbolic link test: %v", err)
		}

		// Try to read through symbolic link
		rangeDiffs, err := reader.ReadRangeFile(linkFile)
		if err != nil {
			t.Fatalf("Failed to read through symbolic link: %v", err)
		}

		if len(rangeDiffs) != 2 {
			t.Errorf("Expected 2 blocks, got %d", len(rangeDiffs))
		}
	})

	t.Run("Large directory with many files", func(t *testing.T) {
		// Create subdirectory for this test
		subDir := filepath.Join(tempDir, "large_dir")
		err := os.MkdirAll(subDir, 0o755)
		if err != nil {
			t.Fatalf("Failed to create subdirectory: %v", err)
		}

		// Create many range files
		numFiles := 100
		for i := 0; i < numFiles; i++ {
			startBlock := uint64(i * 1000)
			endBlock := startBlock + 999
			filename := fmt.Sprintf("%d_%d.json.zst", startBlock, endBlock)

			testData := []RangeDiffs{
				{BlockNum: startBlock, Diffs: []*rpc.TransactionResult{{TxHash: fmt.Sprintf("0x%x", i)}}},
			}

			jsonData, err := json.Marshal(testData)
			if err != nil {
				t.Fatalf("Failed to marshal data: %v", err)
			}

			encoder, err := utils.NewZstdEncoder()
			if err != nil {
				t.Fatalf("Failed to create encoder: %v", err)
			}
			defer encoder.Close()

			compressedData, err := encoder.Compress(jsonData)
			if err != nil {
				t.Fatalf("Failed to compress data: %v", err)
			}

			filePath := filepath.Join(subDir, filename)
			err = os.WriteFile(filePath, compressedData, 0o644)
			if err != nil {
				t.Fatalf("Failed to write file: %v", err)
			}
		}

		// Scan the directory
		rangeFileMap, err := reader.ScanRangeFiles(subDir)
		if err != nil {
			t.Fatalf("Failed to scan large directory: %v", err)
		}

		// Verify we found all blocks
		expectedBlocks := numFiles * 1000
		if len(rangeFileMap) != expectedBlocks {
			t.Errorf("Expected %d blocks, got %d", expectedBlocks, len(rangeFileMap))
		}

		// Verify a few specific mappings
		if rangeFileMap[0] != filepath.Join(subDir, "0_999.json.zst") {
			t.Error("Block 0 not mapped correctly")
		}

		if rangeFileMap[50500] != filepath.Join(subDir, "50000_50999.json.zst") {
			t.Error("Block 50500 not mapped correctly")
		}
	})
}

// TestRangeFileConcurrentAccess tests concurrent file access scenarios
func TestRangeFileConcurrentAccess(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "rangefile_concurrent_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create shared test data
	testRangeDiffs := []RangeDiffs{
		{BlockNum: 7000, Diffs: []*rpc.TransactionResult{{TxHash: "0x789"}}},
		{BlockNum: 7001, Diffs: []*rpc.TransactionResult{{TxHash: "0xabc"}}},
	}

	// Create range file
	jsonData, err := json.Marshal(testRangeDiffs)
	if err != nil {
		t.Fatalf("Failed to marshal test data: %v", err)
	}

	encoder, err := utils.NewZstdEncoder()
	if err != nil {
		t.Fatalf("Failed to create encoder: %v", err)
	}
	defer encoder.Close()

	compressedData, err := encoder.Compress(jsonData)
	if err != nil {
		t.Fatalf("Failed to compress data: %v", err)
	}

	rangeFile := filepath.Join(tempDir, "7000_7001.json.zst")
	err = os.WriteFile(rangeFile, compressedData, 0o644)
	if err != nil {
		t.Fatalf("Failed to write range file: %v", err)
	}

	t.Run("Concurrent readers", func(t *testing.T) {
		numReaders := 10
		var wg sync.WaitGroup
		errChan := make(chan error, numReaders)

		// Start multiple readers concurrently
		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func(readerID int) {
				defer wg.Done()

				reader, err := NewRangeFileReader()
				if err != nil {
					errChan <- fmt.Errorf("reader %d: failed to create reader: %w", readerID, err)
					return
				}
				defer reader.Close()

				// Read the range file
				rangeDiffs, err := reader.ReadRangeFile(rangeFile)
				if err != nil {
					errChan <- fmt.Errorf("reader %d: failed to read range file: %w", readerID, err)
					return
				}

				// Verify data integrity
				if len(rangeDiffs) != 2 {
					errChan <- fmt.Errorf("reader %d: expected 2 blocks, got %d", readerID, len(rangeDiffs))
					return
				}

				if rangeDiffs[0].BlockNum != 7000 {
					errChan <- fmt.Errorf("reader %d: expected block 7000, got %d", readerID, rangeDiffs[0].BlockNum)
					return
				}

				// Extract specific block
				blockData, err := reader.ExtractBlockFromRange(rangeDiffs, 7001)
				if err != nil {
					errChan <- fmt.Errorf("reader %d: failed to extract block: %w", readerID, err)
					return
				}

				if len(blockData) != 1 {
					errChan <- fmt.Errorf("reader %d: expected 1 transaction, got %d", readerID, len(blockData))
					return
				}
			}(i)
		}

		// Wait for all readers to complete
		wg.Wait()
		close(errChan)

		// Check for errors
		for err := range errChan {
			if err != nil {
				t.Errorf("Concurrent reader error: %v", err)
			}
		}
	})

	t.Run("Concurrent directory scanning", func(t *testing.T) {
		numScanners := 5
		var wg sync.WaitGroup
		errChan := make(chan error, numScanners)

		// Start multiple scanners concurrently
		for i := 0; i < numScanners; i++ {
			wg.Add(1)
			go func(scannerID int) {
				defer wg.Done()

				reader, err := NewRangeFileReader()
				if err != nil {
					errChan <- fmt.Errorf("scanner %d: failed to create reader: %w", scannerID, err)
					return
				}
				defer reader.Close()

				// Scan directory
				rangeFileMap, err := reader.ScanRangeFiles(tempDir)
				if err != nil {
					errChan <- fmt.Errorf("scanner %d: failed to scan directory: %w", scannerID, err)
					return
				}

				// Verify mapping
				if len(rangeFileMap) != 2 {
					errChan <- fmt.Errorf("scanner %d: expected 2 blocks, got %d", scannerID, len(rangeFileMap))
					return
				}

				if rangeFileMap[7000] != rangeFile {
					errChan <- fmt.Errorf("scanner %d: block 7000 not mapped correctly", scannerID)
					return
				}
			}(i)
		}

		// Wait for all scanners to complete
		wg.Wait()
		close(errChan)

		// Check for errors
		for err := range errChan {
			if err != nil {
				t.Errorf("Concurrent scanner error: %v", err)
			}
		}
	})

	t.Run("Mixed concurrent operations", func(t *testing.T) {
		numWorkers := 15
		var wg sync.WaitGroup
		errChan := make(chan error, numWorkers)

		// Start mixed operations concurrently
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				reader, err := NewRangeFileReader()
				if err != nil {
					errChan <- fmt.Errorf("worker %d: failed to create reader: %w", workerID, err)
					return
				}
				defer reader.Close()

				// Perform different operations based on worker ID
				switch workerID % 3 {
				case 0:
					// Read range file
					_, err := reader.ReadRangeFile(rangeFile)
					if err != nil {
						errChan <- fmt.Errorf("worker %d: failed to read range file: %w", workerID, err)
						return
					}
				case 1:
					// Scan directory
					_, err := reader.ScanRangeFiles(tempDir)
					if err != nil {
						errChan <- fmt.Errorf("worker %d: failed to scan directory: %w", workerID, err)
						return
					}
				case 2:
					// Read specific block
					_, err := reader.ReadBlockFromRangeFile(rangeFile, 7000)
					if err != nil {
						errChan <- fmt.Errorf("worker %d: failed to read block: %w", workerID, err)
						return
					}
				}
			}(i)
		}

		// Wait for all workers to complete
		wg.Wait()
		close(errChan)

		// Check for errors
		for err := range errChan {
			if err != nil {
				t.Errorf("Mixed concurrent operation error: %v", err)
			}
		}
	})
}

// TestRangeFileLargeFileHandling tests large file handling and performance
func TestRangeFileLargeFileHandling(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "rangefile_large_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	reader, err := NewRangeFileReader()
	if err != nil {
		t.Fatalf("Failed to create range file reader: %v", err)
	}
	defer reader.Close()

	t.Run("Large range file with many blocks", func(t *testing.T) {
		// Create a large range file with many blocks
		numBlocks := 1000
		startBlock := uint64(10000)

		var testRangeDiffs []RangeDiffs
		for i := 0; i < numBlocks; i++ {
			blockNum := startBlock + uint64(i)
			testRangeDiffs = append(testRangeDiffs, RangeDiffs{
				BlockNum: blockNum,
				Diffs: []*rpc.TransactionResult{
					{
						TxHash: fmt.Sprintf("0x%016x", blockNum),
						StateDiff: map[string]rpc.AccountDiff{
							fmt.Sprintf("0x%040x", blockNum): {
								Balance: map[string]interface{}{
									"*": map[string]interface{}{
										"from": fmt.Sprintf("0x%x", blockNum*1000),
										"to":   fmt.Sprintf("0x%x", blockNum*1001),
									},
								},
								Code:    "=",
								Nonce:   "=",
								Storage: map[string]interface{}{},
							},
						},
					},
				},
			})
		}

		// Marshal and compress
		jsonData, err := json.Marshal(testRangeDiffs)
		if err != nil {
			t.Fatalf("Failed to marshal large test data: %v", err)
		}

		encoder, err := utils.NewZstdEncoder()
		if err != nil {
			t.Fatalf("Failed to create encoder: %v", err)
		}
		defer encoder.Close()

		compressedData, err := encoder.Compress(jsonData)
		if err != nil {
			t.Fatalf("Failed to compress large data: %v", err)
		}

		// Write large file
		largeFile := filepath.Join(tempDir, fmt.Sprintf("%d_%d.json.zst", startBlock, startBlock+uint64(numBlocks-1)))
		err = os.WriteFile(largeFile, compressedData, 0o644)
		if err != nil {
			t.Fatalf("Failed to write large file: %v", err)
		}

		t.Logf("Created large range file: %s (Original: %d bytes, Compressed: %d bytes, Ratio: %.2f%%)",
			largeFile, len(jsonData), len(compressedData), utils.GetCompressionRatio(len(jsonData), len(compressedData)))

		// Test reading large file with timing
		start := time.Now()
		rangeDiffs, err := reader.ReadRangeFile(largeFile)
		readDuration := time.Since(start)

		if err != nil {
			t.Fatalf("Failed to read large range file: %v", err)
		}

		if len(rangeDiffs) != numBlocks {
			t.Errorf("Expected %d blocks, got %d", numBlocks, len(rangeDiffs))
		}

		t.Logf("Read %d blocks in %v (%.2f blocks/ms)", numBlocks, readDuration,
			float64(numBlocks)/float64(readDuration.Nanoseconds())*1000000)

		// Test extracting specific blocks
		testBlocks := []uint64{startBlock, startBlock + 100, startBlock + 500, startBlock + 999}
		for _, blockNum := range testBlocks {
			start := time.Now()
			blockData, err := reader.ExtractBlockFromRange(rangeDiffs, blockNum)
			extractDuration := time.Since(start)

			if err != nil {
				t.Errorf("Failed to extract block %d: %v", blockNum, err)
				continue
			}

			if len(blockData) != 1 {
				t.Errorf("Expected 1 transaction for block %d, got %d", blockNum, len(blockData))
				continue
			}

			t.Logf("Extracted block %d in %v", blockNum, extractDuration)
		}
	})

	t.Run("Large file with complex state diffs", func(t *testing.T) {
		// Create range file with complex state diffs
		numBlocks := 50
		startBlock := uint64(20000)

		var testRangeDiffs []RangeDiffs
		for i := 0; i < numBlocks; i++ {
			blockNum := startBlock + uint64(i)

			// Create multiple transactions with complex state diffs
			var diffs []*rpc.TransactionResult
			for j := 0; j < 10; j++ { // 10 transactions per block
				stateDiff := make(map[string]rpc.AccountDiff)

				// Add multiple accounts with storage changes
				for k := 0; k < 5; k++ { // 5 accounts per transaction
					accountAddr := fmt.Sprintf("0x%040x", blockNum*1000+uint64(j*10+k))
					storage := make(map[string]interface{})

					// Add storage slots
					for s := 0; s < 20; s++ { // 20 storage slots per account
						slotKey := fmt.Sprintf("0x%064x", s)
						storage[slotKey] = map[string]interface{}{
							"*": map[string]interface{}{
								"from": fmt.Sprintf("0x%064x", blockNum*1000+uint64(s)),
								"to":   fmt.Sprintf("0x%064x", blockNum*1000+uint64(s+1)),
							},
						}
					}

					stateDiff[accountAddr] = rpc.AccountDiff{
						Balance: map[string]interface{}{
							"*": map[string]interface{}{
								"from": fmt.Sprintf("0x%x", blockNum*1000+uint64(j*10+k)),
								"to":   fmt.Sprintf("0x%x", blockNum*1000+uint64(j*10+k+1)),
							},
						},
						Code:    "=",
						Nonce:   "=",
						Storage: storage,
					}
				}

				diffs = append(diffs, &rpc.TransactionResult{
					TxHash:    fmt.Sprintf("0x%064x", blockNum*1000+uint64(j)),
					StateDiff: stateDiff,
				})
			}

			testRangeDiffs = append(testRangeDiffs, RangeDiffs{
				BlockNum: blockNum,
				Diffs:    diffs,
			})
		}

		// Marshal and compress
		jsonData, err := json.Marshal(testRangeDiffs)
		if err != nil {
			t.Fatalf("Failed to marshal complex test data: %v", err)
		}

		encoder, err := utils.NewZstdEncoder()
		if err != nil {
			t.Fatalf("Failed to create encoder: %v", err)
		}
		defer encoder.Close()

		compressedData, err := encoder.Compress(jsonData)
		if err != nil {
			t.Fatalf("Failed to compress complex data: %v", err)
		}

		// Write complex file
		complexFile := filepath.Join(tempDir, fmt.Sprintf("%d_%d.json.zst", startBlock, startBlock+uint64(numBlocks-1)))
		err = os.WriteFile(complexFile, compressedData, 0o644)
		if err != nil {
			t.Fatalf("Failed to write complex file: %v", err)
		}

		t.Logf("Created complex range file: %s (Original: %d bytes, Compressed: %d bytes, Ratio: %.2f%%)",
			complexFile, len(jsonData), len(compressedData), utils.GetCompressionRatio(len(jsonData), len(compressedData)))

		// Test reading complex file with timing
		start := time.Now()
		rangeDiffs, err := reader.ReadRangeFile(complexFile)
		readDuration := time.Since(start)

		if err != nil {
			t.Fatalf("Failed to read complex range file: %v", err)
		}

		if len(rangeDiffs) != numBlocks {
			t.Errorf("Expected %d blocks, got %d", numBlocks, len(rangeDiffs))
		}

		t.Logf("Read %d complex blocks in %v", numBlocks, readDuration)

		// Verify content integrity
		firstBlock := rangeDiffs[0]
		if len(firstBlock.Diffs) != 10 {
			t.Errorf("Expected 10 transactions in first block, got %d", len(firstBlock.Diffs))
		}

		// Check first transaction has correct number of accounts
		if len(firstBlock.Diffs[0].StateDiff) != 5 {
			t.Errorf("Expected 5 accounts in first transaction, got %d", len(firstBlock.Diffs[0].StateDiff))
		}

		// Test extracting from complex data
		start = time.Now()
		blockData, err := reader.ExtractBlockFromRange(rangeDiffs, startBlock+25)
		extractDuration := time.Since(start)

		if err != nil {
			t.Fatalf("Failed to extract from complex data: %v", err)
		}

		if len(blockData) != 10 {
			t.Errorf("Expected 10 transactions, got %d", len(blockData))
		}

		t.Logf("Extracted complex block in %v", extractDuration)
	})

	t.Run("Memory usage with large files", func(t *testing.T) {
		// Create a separate temp directory for this test to avoid conflicts
		subDir := filepath.Join(tempDir, "memory_test")
		err := os.MkdirAll(subDir, 0o755)
		if err != nil {
			t.Fatalf("Failed to create subdirectory: %v", err)
		}

		// Create multiple large files and test memory usage
		numFiles := 10
		blocksPerFile := 100

		var files []string
		for i := 0; i < numFiles; i++ {
			startBlock := uint64(i * 1000)
			endBlock := startBlock + uint64(blocksPerFile-1)

			var testRangeDiffs []RangeDiffs
			for j := 0; j < blocksPerFile; j++ {
				blockNum := startBlock + uint64(j)
				testRangeDiffs = append(testRangeDiffs, RangeDiffs{
					BlockNum: blockNum,
					Diffs: []*rpc.TransactionResult{
						{
							TxHash: fmt.Sprintf("0x%016x", blockNum),
							StateDiff: map[string]rpc.AccountDiff{
								fmt.Sprintf("0x%040x", blockNum): {
									Balance: map[string]interface{}{
										"*": map[string]interface{}{
											"from": fmt.Sprintf("0x%x", blockNum*1000),
											"to":   fmt.Sprintf("0x%x", blockNum*1001),
										},
									},
									Code:    "=",
									Nonce:   "=",
									Storage: map[string]interface{}{},
								},
							},
						},
					},
				})
			}

			// Create file
			jsonData, err := json.Marshal(testRangeDiffs)
			if err != nil {
				t.Fatalf("Failed to marshal data for file %d: %v", i, err)
			}

			encoder, err := utils.NewZstdEncoder()
			if err != nil {
				t.Fatalf("Failed to create encoder for file %d: %v", i, err)
			}
			defer encoder.Close()

			compressedData, err := encoder.Compress(jsonData)
			if err != nil {
				t.Fatalf("Failed to compress data for file %d: %v", i, err)
			}

			fileName := fmt.Sprintf("%d_%d.json.zst", startBlock, endBlock)
			filePath := filepath.Join(subDir, fileName)
			err = os.WriteFile(filePath, compressedData, 0o644)
			if err != nil {
				t.Fatalf("Failed to write file %d: %v", i, err)
			}

			files = append(files, filePath)
		}

		// Test reading all files sequentially
		start := time.Now()
		totalBlocks := 0
		for i, filePath := range files {
			rangeDiffs, err := reader.ReadRangeFile(filePath)
			if err != nil {
				t.Errorf("Failed to read file %d: %v", i, err)
				continue
			}
			totalBlocks += len(rangeDiffs)
		}
		totalDuration := time.Since(start)

		t.Logf("Read %d files with %d total blocks in %v (%.2f files/sec)",
			numFiles, totalBlocks, totalDuration, float64(numFiles)/totalDuration.Seconds())

		// Test scanning all files
		start = time.Now()
		rangeFileMap, err := reader.ScanRangeFiles(subDir)
		scanDuration := time.Since(start)

		if err != nil {
			t.Fatalf("Failed to scan files: %v", err)
		}

		expectedBlocks := numFiles * blocksPerFile
		if len(rangeFileMap) != expectedBlocks {
			t.Logf("Expected %d blocks in map, got %d", expectedBlocks, len(rangeFileMap))
			// Check some of the mappings to debug
			for i := 0; i < 5 && i < len(files); i++ {
				startBlock := uint64(i * 1000)
				endBlock := startBlock + uint64(blocksPerFile-1)
				t.Logf("File %d: %d_%d.json.zst", i, startBlock, endBlock)
			}
		}

		t.Logf("Scanned %d files creating mapping for %d blocks in %v",
			numFiles, len(rangeFileMap), scanDuration)
	})
}
