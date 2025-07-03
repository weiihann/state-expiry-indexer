package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewFileStoreWithCompression(t *testing.T) {
	// Create temporary directory for testing
	tempDir := t.TempDir()

	// Test with compression enabled
	fs, err := NewFileStoreWithCompression(tempDir, true)
	if err != nil {
		t.Fatalf("Failed to create FileStore with compression: %v", err)
	}
	defer fs.Close()

	if fs.encoder == nil {
		t.Error("Encoder should be initialized when compression is enabled")
	}

	// Test with compression disabled
	fs2, err := NewFileStoreWithCompression(tempDir, false)
	if err != nil {
		t.Fatalf("Failed to create FileStore without compression: %v", err)
	}
	defer fs2.Close()

	if fs2.encoder != nil {
		t.Error("Encoder should not be initialized when compression is disabled")
	}
}

func TestSaveCompressed(t *testing.T) {
	// Create temporary directory for testing
	tempDir := t.TempDir()

	// Create FileStore with compression enabled
	fs, err := NewFileStoreWithCompression(tempDir, true)
	if err != nil {
		t.Fatalf("Failed to create FileStore with compression: %v", err)
	}
	defer fs.Close()

	// Test data
	testData := []byte(`{"test": "data", "block": 12345, "state": {"account": "0x123", "balance": "1000000000000000000"}}`)
	filename := "test_block.json"

	// Save compressed file
	err = fs.SaveCompressed(filename, testData)
	if err != nil {
		t.Fatalf("Failed to save compressed file: %v", err)
	}

	// Check that compressed file exists
	compressedPath := filepath.Join(tempDir, filename+".zst")
	if _, err := os.Stat(compressedPath); os.IsNotExist(err) {
		t.Fatalf("Compressed file was not created: %s", compressedPath)
	}

	// Check that original file was not created
	originalPath := filepath.Join(tempDir, filename)
	if _, err := os.Stat(originalPath); !os.IsNotExist(err) {
		t.Errorf("Original file should not exist: %s", originalPath)
	}

	// Verify compressed file is smaller than original
	compressedInfo, err := os.Stat(compressedPath)
	if err != nil {
		t.Fatalf("Failed to stat compressed file: %v", err)
	}

	if compressedInfo.Size() >= int64(len(testData)) {
		t.Errorf("Compressed file should be smaller than original. Original: %d bytes, Compressed: %d bytes",
			len(testData), compressedInfo.Size())
	}
}

func TestSaveCompressedWithCompressionDisabled(t *testing.T) {
	// Create temporary directory for testing
	tempDir := t.TempDir()

	// Create FileStore with compression disabled
	fs, err := NewFileStoreWithCompression(tempDir, false)
	if err != nil {
		t.Fatalf("Failed to create FileStore without compression: %v", err)
	}
	defer fs.Close()

	// Test data
	testData := []byte(`{"test": "data"}`)
	filename := "test_block.json"

	// Try to save compressed file - should fail
	err = fs.SaveCompressed(filename, testData)
	if err == nil {
		t.Fatal("SaveCompressed should fail when compression is disabled")
	}

	expectedError := "compression is not enabled for this FileStore"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}
}

func TestSaveCompressedEmptyData(t *testing.T) {
	// Create temporary directory for testing
	tempDir := t.TempDir()

	// Create FileStore with compression enabled
	fs, err := NewFileStoreWithCompression(tempDir, true)
	if err != nil {
		t.Fatalf("Failed to create FileStore with compression: %v", err)
	}
	defer fs.Close()

	// Try to save empty data - should fail
	err = fs.SaveCompressed("test.json", []byte{})
	if err == nil {
		t.Fatal("SaveCompressed should fail with empty data")
	}

	expectedError := "cannot compress empty data"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}
}

func TestSaveCompressedBackwardCompatibility(t *testing.T) {
	// Create temporary directory for testing
	tempDir := t.TempDir()

	// Create FileStore with compression enabled
	fs, err := NewFileStoreWithCompression(tempDir, true)
	if err != nil {
		t.Fatalf("Failed to create FileStore with compression: %v", err)
	}
	defer fs.Close()

	// Test data
	testData := []byte(`{"test": "data"}`)
	filename := "test_block.json"

	// Test that regular Save still works
	err = fs.Save(filename, testData)
	if err != nil {
		t.Fatalf("Failed to save uncompressed file: %v", err)
	}

	// Check that uncompressed file exists
	originalPath := filepath.Join(tempDir, filename)
	if _, err := os.Stat(originalPath); os.IsNotExist(err) {
		t.Fatalf("Uncompressed file was not created: %s", originalPath)
	}

	// Verify file content
	content, err := os.ReadFile(originalPath)
	if err != nil {
		t.Fatalf("Failed to read saved file: %v", err)
	}

	if string(content) != string(testData) {
		t.Errorf("File content mismatch. Expected: %s, Got: %s", string(testData), string(content))
	}
}

func TestFileStoreClose(t *testing.T) {
	// Create temporary directory for testing
	tempDir := t.TempDir()

	// Test closing FileStore with compression enabled
	fs, err := NewFileStoreWithCompression(tempDir, true)
	if err != nil {
		t.Fatalf("Failed to create FileStore with compression: %v", err)
	}

	// Close should not error
	err = fs.Close()
	if err != nil {
		t.Fatalf("Failed to close FileStore: %v", err)
	}

	// Test closing FileStore without compression
	fs2, err := NewFileStoreWithCompression(tempDir, false)
	if err != nil {
		t.Fatalf("Failed to create FileStore without compression: %v", err)
	}

	// Close should not error
	err = fs2.Close()
	if err != nil {
		t.Fatalf("Failed to close FileStore: %v", err)
	}
}
