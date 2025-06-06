package tracker

import (
	"os"
	"strconv"
	"strings"
)

const lastProcessedBlockFile = "data/.last_processed_block"

type Tracker struct{}

func NewTracker() *Tracker {
	return &Tracker{}
}

func (t *Tracker) GetLastProcessedBlock() (uint64, error) {
	data, err := os.ReadFile(lastProcessedBlockFile)
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

func (t *Tracker) SetLastProcessedBlock(blockNumber uint64) error {
	data := []byte(strconv.FormatUint(blockNumber, 10))
	return os.WriteFile(lastProcessedBlockFile, data, 0o644)
}
