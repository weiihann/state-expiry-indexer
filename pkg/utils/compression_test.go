package utils

import (
	"encoding/json"
	"strings"
	"testing"
)

// Sample realistic state diff JSON data for testing
var sampleStateDiffJSON = `[
  {
    "stateDiff": {
      "0x1234567890abcdef1234567890abcdef12345678": {
        "balance": {
          "from": "0x1bc16d674ec80000",
          "to": "0x1bc16d674ec90000"
        },
        "nonce": {
          "from": "0x1",
          "to": "0x2"
        },
        "storage": {
          "0x0000000000000000000000000000000000000000000000000000000000000001": {
            "from": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "to": "0x0000000000000000000000000000000000000000000000000000000000000001"
          },
          "0x0000000000000000000000000000000000000000000000000000000000000002": {
            "from": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "to": "0x00000000000000000000000000000000000000000000000000000000000000ff"
          }
        }
      },
      "0xabcdef1234567890abcdef1234567890abcdef12": {
        "balance": {
          "from": "0x0",
          "to": "0x1bc16d674ec80000"
        },
        "code": {
          "from": "0x",
          "to": "0x608060405234801561001057600080fd5b50"
        }
      }
    },
    "transactionHash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12"
  }
]`

func TestCompressJSON(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		expectError bool
	}{
		{
			name:        "valid JSON data",
			data:        []byte(sampleStateDiffJSON),
			expectError: false,
		},
		{
			name:        "small JSON data",
			data:        []byte(`{"key": "value"}`),
			expectError: false,
		},
		{
			name:        "empty data",
			data:        []byte{},
			expectError: true,
		},
		{
			name:        "nil data",
			data:        nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := CompressJSON(tt.data)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if len(compressed) == 0 {
				t.Errorf("compressed data is empty")
				return
			}

			// Verify the compressed data is smaller (for reasonable-sized inputs)
			if len(tt.data) > 100 && len(compressed) >= len(tt.data) {
				t.Errorf("compression didn't reduce size: original=%d, compressed=%d",
					len(tt.data), len(compressed))
			}
		})
	}
}

func TestCompressJSONDefaultLevel(t *testing.T) {
	data := []byte(sampleStateDiffJSON)

	compressed, err := CompressJSON(data)
	if err != nil {
		t.Fatalf("compression failed: %v", err)
	}

	if len(compressed) == 0 {
		t.Errorf("compressed data is empty")
		return
	}

	// Verify compression actually reduced size for reasonable-sized inputs
	if len(data) > 100 && len(compressed) >= len(data) {
		t.Errorf("compression didn't reduce size: original=%d, compressed=%d",
			len(data), len(compressed))
	}

	ratio := GetCompressionRatio(len(data), len(compressed))
	t.Logf("Default compression: Original=%d bytes, Compressed=%d bytes, Ratio=%.2f%%",
		len(data), len(compressed), ratio)

	// Verify we can decompress it back
	decompressed, err := DecompressJSON(compressed)
	if err != nil {
		t.Fatalf("failed to decompress: %v", err)
	}

	if string(decompressed) != string(data) {
		t.Errorf("round trip failed: data was modified")
	}
}

func TestDecompressJSON(t *testing.T) {
	originalData := []byte(sampleStateDiffJSON)

	// First compress the data
	compressed, err := CompressJSON(originalData)
	if err != nil {
		t.Fatalf("failed to compress test data: %v", err)
	}

	tests := []struct {
		name        string
		data        []byte
		expected    []byte
		expectError bool
	}{
		{
			name:        "valid compressed data",
			data:        compressed,
			expected:    originalData,
			expectError: false,
		},
		{
			name:        "empty data",
			data:        []byte{},
			expected:    nil,
			expectError: true,
		},
		{
			name:        "invalid compressed data",
			data:        []byte("not compressed data"),
			expected:    nil,
			expectError: true,
		},
		{
			name:        "nil data",
			data:        nil,
			expected:    nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decompressed, err := DecompressJSON(tt.data)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if string(decompressed) != string(tt.expected) {
				t.Errorf("decompressed data doesn't match original")
				t.Errorf("Expected: %s", string(tt.expected))
				t.Errorf("Got: %s", string(decompressed))
			}
		})
	}
}

func TestCompressionRoundTrip(t *testing.T) {
	testData := [][]byte{
		[]byte(sampleStateDiffJSON),
		[]byte(`{"simple": "json"}`),
		[]byte(`[]`), // empty array
		[]byte(`{}`), // empty object
		[]byte(strings.Repeat(`{"key": "value"},`, 1000)), // large repetitive data
	}

	for i, data := range testData {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			// Compress
			compressed, err := CompressJSON(data)
			if err != nil {
				t.Fatalf("compression failed: %v", err)
			}

			// Decompress
			decompressed, err := DecompressJSON(compressed)
			if err != nil {
				t.Fatalf("decompression failed: %v", err)
			}

			// Verify round trip
			if string(decompressed) != string(data) {
				t.Errorf("round trip failed: data was modified")
			}

			// Log compression statistics
			ratio := GetCompressionRatio(len(data), len(compressed))
			t.Logf("Original: %d bytes, Compressed: %d bytes, Ratio: %.2f%%",
				len(data), len(compressed), ratio)
		})
	}
}

func TestGetCompressionRatio(t *testing.T) {
	tests := []struct {
		name           string
		originalSize   int
		compressedSize int
		expected       float64
	}{
		{"50% compression", 100, 50, 50.0},
		{"90% compression", 1000, 100, 90.0},
		{"no compression", 100, 100, 0.0},
		{"expansion", 100, 150, -50.0}, // negative means expansion
		{"zero original", 0, 50, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ratio := GetCompressionRatio(tt.originalSize, tt.compressedSize)
			if ratio != tt.expected {
				t.Errorf("expected %.2f, got %.2f", tt.expected, ratio)
			}
		})
	}
}

func TestValidateCompressedData(t *testing.T) {
	// Create valid compressed data
	validData := []byte(sampleStateDiffJSON)
	compressed, err := CompressJSON(validData)
	if err != nil {
		t.Fatalf("failed to create test compressed data: %v", err)
	}

	tests := []struct {
		name        string
		data        []byte
		expectError bool
	}{
		{
			name:        "valid compressed data",
			data:        compressed,
			expectError: false,
		},
		{
			name:        "empty data",
			data:        []byte{},
			expectError: true,
		},
		{
			name:        "invalid data",
			data:        []byte("this is not compressed"),
			expectError: true,
		},
		{
			name:        "nil data",
			data:        nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCompressedData(tt.data)

			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}

			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestCompressionWithRealStateDataStructure(t *testing.T) {
	// Create a realistic state diff structure that matches the actual rpc.TransactionResult
	type AccountDiff struct {
		Balance interface{} `json:"balance,omitempty"`
		Code    interface{} `json:"code,omitempty"`
		Nonce   interface{} `json:"nonce,omitempty"`
		Storage interface{} `json:"storage,omitempty"`
	}

	type TransactionResult struct {
		StateDiff map[string]AccountDiff `json:"stateDiff"`
		TxHash    string                 `json:"transactionHash"`
	}

	// Create realistic test data
	stateDiff := []TransactionResult{
		{
			StateDiff: map[string]AccountDiff{
				"0x742d35Cc6081C0532895eC4Ff8f2F2cFa22eA29e": {
					Balance: map[string]string{
						"from": "0x1bc16d674ec80000",
						"to":   "0x1bc16d674ec90000",
					},
					Nonce: map[string]string{
						"from": "0x1",
						"to":   "0x2",
					},
					Storage: map[string]map[string]string{
						"0x0000000000000000000000000000000000000000000000000000000000000001": {
							"from": "0x0000000000000000000000000000000000000000000000000000000000000000",
							"to":   "0x0000000000000000000000000000000000000000000000000000000000000001",
						},
					},
				},
			},
			TxHash: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12",
		},
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(stateDiff)
	if err != nil {
		t.Fatalf("failed to marshal test data: %v", err)
	}

	// Test compression and decompression
	compressed, err := CompressJSON(jsonData)
	if err != nil {
		t.Fatalf("compression failed: %v", err)
	}

	decompressed, err := DecompressJSON(compressed)
	if err != nil {
		t.Fatalf("decompression failed: %v", err)
	}

	// Verify the JSON structure is preserved
	var result []TransactionResult
	if err := json.Unmarshal(decompressed, &result); err != nil {
		t.Fatalf("failed to unmarshal decompressed data: %v", err)
	}

	if len(result) != 1 {
		t.Errorf("expected 1 transaction result, got %d", len(result))
	}

	if result[0].TxHash != stateDiff[0].TxHash {
		t.Errorf("transaction hash mismatch: expected %s, got %s",
			stateDiff[0].TxHash, result[0].TxHash)
	}

	// Log compression statistics
	ratio := GetCompressionRatio(len(jsonData), len(compressed))
	t.Logf("Real state diff compression: Original=%d bytes, Compressed=%d bytes, Ratio=%.2f%%",
		len(jsonData), len(compressed), ratio)
}
