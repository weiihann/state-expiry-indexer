package caller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"os"
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
	client          []*rpc.Client
	fileStore       *storage.FileStore
	downloadTracker *tracker.DownloadTracker
	config          internal.Config
	log             *slog.Logger
	startBlock      uint64
}

func NewService(client []*rpc.Client, fileStore *storage.FileStore, config internal.Config) *Service {
	log := logger.GetLogger("rpc-caller")

	// Filter out nil clients and validate we have at least one
	validClients := make([]*rpc.Client, 0, len(client))
	for _, c := range client {
		if c != nil {
			validClients = append(validClients, c)
		}
	}

	if len(validClients) == 0 {
		log.Error("No valid RPC clients provided")
		return nil
	}

	log.Info("RPC caller service initialized", "valid_clients", len(validClients))

	return &Service{
		client:          validClients,
		fileStore:       fileStore,
		downloadTracker: tracker.NewDownloadTracker(),
		config:          config,
		log:             log,
		startBlock:      config.StartBlock,
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
	if s.startBlock == 0 {
		latestBlock, err := s.client[0].GetLatestBlockNumber(ctx)
		if err != nil {
			return fmt.Errorf("could not get latest block number: %w", err)
		}

		s.startBlock = latestBlock.Uint64()
	}

	count := 0
	logged := time.Now()
	logProgress := func() {
		count++
		if time.Since(logged) > 5*time.Second {
			s.log.Info("Download progress", "cycle_count", count, "elapsed", time.Since(logged).Seconds())
			logged = time.Now()
			count = 0
		}
	}

	for i := s.startBlock; ; i-- {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Skip if the block file already exists
		if _, err := os.Stat(blockFile(i)); err == nil {
			continue
		}

		if err := s.downloadBlock(ctx, i); err != nil {
			return fmt.Errorf("failed to download block %d: %w", i, err)
		}

		logProgress()

		if i == 0 {
			break
		}
	}

	return nil
}

// downloadBlock downloads and saves state diff for a single block
func (s *Service) downloadBlock(ctx context.Context, blockNumber uint64) error {
	blockNum := big.NewInt(int64(blockNumber))

	// Download state diff from RPC with per-call timeout
	var err error
	var stateDiff []rpc.TransactionResult
	for _, client := range s.client {
		// Skip nil clients as a defensive measure
		if client == nil {
			s.log.Warn("Skipping nil RPC client", "block_number", blockNumber)
			continue
		}

		// Create a fresh timeout context for each RPC call
		timeoutCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
		stateDiff, err = client.GetStateDiff(timeoutCtx, blockNum)
		cancel() // Always cancel to release resources

		if err == nil {
			break
		}
		s.log.Debug("RPC call failed, trying next client", "error", err, "block_number", blockNumber)
	}

	if err != nil {
		return fmt.Errorf("could not get state diff: %w", err)
	}

	// Check if we actually got any results (all clients might have failed)
	if stateDiff == nil {
		return fmt.Errorf("no state diff obtained - all RPC clients failed or unavailable")
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

func blockFile(blockNumber uint64) string {
	return fmt.Sprintf("%d.json", blockNumber)
}

// GetLastDownloadedBlock returns the last successfully downloaded block
func (s *Service) GetLastDownloadedBlock() (uint64, error) {
	return s.downloadTracker.GetLastDownloadedBlock()
}
