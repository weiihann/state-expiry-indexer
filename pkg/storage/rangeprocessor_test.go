package storage

import (
	"testing"
)

func TestRangeProcessor_GetRangeNumber(t *testing.T) {
	// Create a range processor with range size 1000
	rp := &RangeProcessor{rangeSize: 1000}

	tests := []struct {
		blockNumber uint64
		expected    uint64
	}{
		{0, 0},        // Genesis
		{1, 0},        // First block
		{1000, 0},     // End of first range
		{1001, 1},     // Start of second range
		{2000, 1},     // End of second range
		{2001, 2},     // Start of third range
		{10000, 9},    // Block 10000
		{999999, 999}, // Large block number
	}

	for _, test := range tests {
		result := rp.GetRangeNumber(test.blockNumber)
		if result != test.expected {
			t.Errorf("GetRangeNumber(%d) = %d, expected %d", test.blockNumber, result, test.expected)
		}
	}
}

func TestRangeProcessor_GetRangeBlockNumbers(t *testing.T) {
	// Create a range processor with range size 1000
	rp := &RangeProcessor{rangeSize: 1000}

	tests := []struct {
		rangeNumber   uint64
		expectedStart uint64
		expectedEnd   uint64
	}{
		{0, 0, 0},         // Genesis range
		{1, 1, 1000},      // First range (blocks 1-1000)
		{2, 1001, 2000},   // Second range (blocks 1001-2000)
		{3, 2001, 3000},   // Third range (blocks 2001-3000)
		{10, 9001, 10000}, // Tenth range (blocks 9001-10000)
	}

	for _, test := range tests {
		start, end := rp.GetRangeBlockNumbers(test.rangeNumber)
		if start != test.expectedStart || end != test.expectedEnd {
			t.Errorf("GetRangeBlockNumbers(%d) = (%d, %d), expected (%d, %d)",
				test.rangeNumber, start, end, test.expectedStart, test.expectedEnd)
		}
	}
}

func TestRangeProcessor_GetRangeFilePath(t *testing.T) {
	// Create a range processor with range size 1000
	rp := &RangeProcessor{
		dataDir:   "/test/data",
		rangeSize: 1000,
	}

	tests := []struct {
		rangeNumber  uint64
		expectedPath string
	}{
		{0, ""},                                // Genesis has no file
		{1, "/test/data/1_1000.json.zst"},      // First range
		{2, "/test/data/1001_2000.json.zst"},   // Second range
		{10, "/test/data/9001_10000.json.zst"}, // Tenth range
	}

	for _, test := range tests {
		result := rp.GetRangeFilePath(test.rangeNumber)
		if result != test.expectedPath {
			t.Errorf("GetRangeFilePath(%d) = %s, expected %s", test.rangeNumber, result, test.expectedPath)
		}
	}
}

func TestRangeProcessor_DifferentRangeSize(t *testing.T) {
	// Test with different range size
	rp := &RangeProcessor{rangeSize: 500}

	// Test range number calculation
	tests := []struct {
		blockNumber uint64
		expected    uint64
	}{
		{0, 0},    // Genesis
		{1, 0},    // First block
		{500, 0},  // End of first range
		{501, 1},  // Start of second range
		{1000, 1}, // End of second range
		{1001, 2}, // Start of third range
	}

	for _, test := range tests {
		result := rp.GetRangeNumber(test.blockNumber)
		if result != test.expected {
			t.Errorf("GetRangeNumber(%d) with range size 500 = %d, expected %d",
				test.blockNumber, result, test.expected)
		}
	}

	// Test range block numbers
	start, end := rp.GetRangeBlockNumbers(1)
	if start != 1 || end != 500 {
		t.Errorf("GetRangeBlockNumbers(1) with range size 500 = (%d, %d), expected (1, 500)", start, end)
	}

	start, end = rp.GetRangeBlockNumbers(2)
	if start != 501 || end != 1000 {
		t.Errorf("GetRangeBlockNumbers(2) with range size 500 = (%d, %d), expected (501, 1000)", start, end)
	}
}
