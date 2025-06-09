package caller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/logger"
	"github.com/weiihann/state-expiry-indexer/pkg/rpc"
	"github.com/weiihann/state-expiry-indexer/pkg/storage"
	"github.com/weiihann/state-expiry-indexer/pkg/tracker"
)

const (
	FinalizedBlockOffset = 64
	rpcTimeout           = 1 * time.Minute
)

// Service handles RPC calls and file storage for state diffs
type Service struct {
	client          *rpc.Client
	fileStore       *storage.FileStore
	downloadTracker *tracker.DownloadTracker
	config          internal.Config
	log             *slog.Logger
}

func NewService(client *rpc.Client, fileStore *storage.FileStore, config internal.Config) *Service {
	log := logger.GetLogger("rpc-caller")
	return &Service{
		client:          client,
		fileStore:       fileStore,
		downloadTracker: tracker.NewDownloadTracker(),
		config:          config,
		log:             log,
	}
}

// Run starts the RPC caller workflow that downloads and saves state diffs
func (s *Service) Run(ctx context.Context) error {
	s.log.Info("Starting RPC caller workflow",
		"poll_interval", s.config.PollInterval,
		"finalized_block_offset", FinalizedBlockOffset)

	pollInterval := time.Duration(s.config.PollInterval) * time.Second

	for {
		select {
		case <-ctx.Done():
			s.log.Info("RPC caller workflow stopped")
			return nil
		default:
			// Continue with download logic
		}

		if err := s.downloadNewBlocks(ctx); err != nil {
			s.log.Warn("Download cycle failed, retrying...",
				"error", err,
				"retry_interval", pollInterval)
			time.Sleep(pollInterval)
			continue
		}

		// Wait before next polling cycle
		time.Sleep(pollInterval)
	}
}

// downloadNewBlocks downloads state diffs for new blocks
func (s *Service) downloadNewBlocks(ctx context.Context) error {
	lastDownloadedBlock, err := s.downloadTracker.GetLastDownloadedBlock()
	if err != nil {
		return fmt.Errorf("could not get last downloaded block: %w", err)
	}

	latestBlock, err := s.client.GetLatestBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("could not get latest block number: %w", err)
	}

	finalizedBlock := latestBlock.Uint64() - FinalizedBlockOffset

	if lastDownloadedBlock >= finalizedBlock {
		s.log.Debug("Caught up to finalized block, waiting for new blocks...",
			"finalized_block", finalizedBlock,
			"last_downloaded", lastDownloadedBlock)
		return nil
	}

	s.log.Info("Downloading block range",
		"from_block", lastDownloadedBlock+1,
		"to_block", finalizedBlock,
		"total_blocks", finalizedBlock-lastDownloadedBlock)

	lastProgressTime := time.Now()
	lastProgressBlock := lastDownloadedBlock

	for i := lastDownloadedBlock + 1; i <= finalizedBlock; i++ {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err := s.downloadBlock(ctx, i); err != nil {
			return fmt.Errorf("failed to download block %d: %w", i, err)
		}

		// Update progress tracker
		if err := s.downloadTracker.SetLastDownloadedBlock(i); err != nil {
			s.log.Error("Could not update last downloaded block",
				"block_number", i,
				"error", err)
		}

		// Show simple progress every 1000 blocks or 8 seconds
		now := time.Now()
		blocksSinceProgress := i - lastProgressBlock
		timeSinceProgress := now.Sub(lastProgressTime).Seconds()

		if blocksSinceProgress >= 1000 || timeSinceProgress >= 8 {
			s.log.Info("Download progress",
				"current_block", i,
				"target_block", finalizedBlock,
				"remaining", finalizedBlock-i)
			lastProgressTime = now
			lastProgressBlock = i
		}

		s.log.Debug("Successfully downloaded block",
			"block_number", i)
	}

	s.log.Info("Completed block range download",
		"from_block", lastDownloadedBlock+1,
		"to_block", finalizedBlock,
		"downloaded_blocks", finalizedBlock-lastDownloadedBlock)

	return nil
}

// downloadBlock downloads and saves state diff for a single block
func (s *Service) downloadBlock(ctx context.Context, blockNumber uint64) error {
	blockNum := big.NewInt(int64(blockNumber))

	// Create a timeout context for the RPC call
	timeoutCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	// Download state diff from RPC with timeout
	stateDiff, err := s.client.GetStateDiff(timeoutCtx, blockNum)
	if err != nil {
		return fmt.Errorf("could not get state diff: %w", err)
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(stateDiff, "", "  ")
	if err != nil {
		return fmt.Errorf("could not marshal state diff: %w", err)
	}

	// Save to file
	filename := fmt.Sprintf("%d.json", blockNumber)
	if err := s.fileStore.Save(filename, data); err != nil {
		return fmt.Errorf("could not save state diff file %s: %w", filename, err)
	}

	return nil
}

// GetLastDownloadedBlock returns the last successfully downloaded block
func (s *Service) GetLastDownloadedBlock() (uint64, error) {
	return s.downloadTracker.GetLastDownloadedBlock()
}
