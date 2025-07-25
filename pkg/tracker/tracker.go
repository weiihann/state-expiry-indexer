package tracker

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	LastDownloadedBlockFile = ".last_downloaded_block" // For RPC caller tracking
)

// DownloadTracker tracks the last successfully downloaded block
type DownloadTracker struct {
	filePath string
}

func NewDownloadTracker(dir string) *DownloadTracker {
	return &DownloadTracker{filePath: filepath.Join(dir, LastDownloadedBlockFile)}
}

func (t *DownloadTracker) GetLastDownloadedBlock() (uint64, error) {
	data, err := os.ReadFile(t.filePath)
	if os.IsNotExist(err) {
		return 0, nil // If file doesn't exist, start from block 0
	}
	if err != nil {
		return 0, err
	}
	lastBlock, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, err
	}
	return lastBlock, nil
}

func (t *DownloadTracker) SetLastDownloadedBlock(blockNumber uint64) error {
	data := []byte(strconv.FormatUint(blockNumber, 10))
	return os.WriteFile(t.filePath, data, 0o644)
}
