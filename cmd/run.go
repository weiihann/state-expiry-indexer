package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/api"
	"github.com/weiihann/state-expiry-indexer/internal/database"
	"github.com/weiihann/state-expiry-indexer/internal/indexer"
	"github.com/weiihann/state-expiry-indexer/internal/logger"
	"github.com/weiihann/state-expiry-indexer/internal/repository"
	"github.com/weiihann/state-expiry-indexer/pkg/rpc"
	"github.com/weiihann/state-expiry-indexer/pkg/storage"
	"github.com/weiihann/state-expiry-indexer/pkg/tracker"
)

const (
	FinalizedBlockOffset = 64
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the state expiry indexer with API server",
	Long:  `This command starts the state expiry indexer and API server concurrently. The indexer polls for new blocks, saves their state diffs, processes them, and updates the database. The API server serves queries from the same database.`,
	Run: func(cmd *cobra.Command, args []string) {
		log := logger.GetLogger("run-cmd")

		// Load configuration with enhanced validation
		config, err := internal.LoadConfig("./configs")
		if err != nil {
			log.Error("Configuration validation failed", "error", err)
			os.Exit(1)
		}

		log.Info("Configuration loaded successfully",
			"environment", config.Environment,
			"api_port", config.APIPort,
			"api_host", config.APIHost,
			"data_dir", config.DataDir,
			"block_batch_size", config.BlockBatchSize,
			"poll_interval", config.PollInterval,
			"db_max_conns", config.DBMaxConns,
			"db_min_conns", config.DBMinConns)

		// Run database migrations
		log.Info("Checking database migrations...")
		if err := RunMigrationsUp(config); err != nil {
			log.Error("Failed to run database migrations", "error", err)
			os.Exit(1)
		}

		ctx := context.Background()

		// Initialize database connection pool
		log.Info("Initializing database connection...")
		db, err := database.Connect(ctx, config)
		if err != nil {
			log.Error("Failed to connect to database", "error", err)
			os.Exit(1)
		}
		defer db.Close()

		// Initialize repository
		repo := repository.NewStateRepository(db)

		// Initialize RPC client
		log.Info("Initializing RPC client...", "rpc_url", config.RPCURL, "timeout", config.RPCTimeout)
		client, err := rpc.NewClient(ctx, config.RPCURL)
		if err != nil {
			log.Error("Failed to create RPC client", "error", err, "rpc_url", config.RPCURL)
			os.Exit(1)
		}

		// Initialize file storage using config paths
		log.Info("Initializing file storage...", "path", config.DataDir)
		fileStore, err := storage.NewFileStore(config.DataDir)
		if err != nil {
			log.Error("Failed to create file store", "error", err, "path", config.DataDir)
			os.Exit(1)
		}

		// Initialize block tracker
		blockTracker := tracker.NewTracker()

		// Initialize indexer service using config paths
		indexerSvc := indexer.NewService(config.DataDir, repo, 0)

		// Initialize API server
		log.Info("Initializing API server...", "host", config.APIHost, "port", config.APIPort)
		apiServer := api.NewServer(repo)

		// Setup graceful shutdown
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		var wg sync.WaitGroup

		// Start API server in goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()

			if err := apiServer.Run(ctx, config.APIPort); err != nil {
				log.Error("API server error", "error", err, "port", config.APIPort)
			}
		}()

		// Start indexer workflow in goroutine with config
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("Starting indexer workflow...",
				"poll_interval", config.PollInterval,
				"block_batch_size", config.BlockBatchSize)

			if err := runIndexerWorkflow(ctx, config, client, fileStore, blockTracker, indexerSvc); err != nil {
				log.Error("Indexer workflow error", "error", err)
			}
			log.Info("Indexer workflow stopped")
		}()

		log.Info("All services started successfully",
			"api_port", config.APIPort,
			"api_url", fmt.Sprintf("http://%s:%d", config.APIHost, config.APIPort),
			"environment", config.Environment,
			"data_dir", config.DataDir)
		log.Info("Services running", "api_available", true, "indexer_running", true)
		log.Info("Press Ctrl+C to stop all services")

		// Wait for shutdown signal
		<-sigChan
		log.Info("Received shutdown signal, stopping all services...")
		cancel()

		// Wait for all goroutines to complete
		wg.Wait()
		log.Info("All services stopped gracefully")
	},
}

// runIndexerWorkflow handles the main indexing loop
func runIndexerWorkflow(ctx context.Context, config internal.Config, client *rpc.Client, fileStore *storage.FileStore, blockTracker *tracker.Tracker, indexerSvc *indexer.Service) error {
	log := logger.GetLogger("indexer-workflow")

	// Use config values instead of constants
	pollInterval := time.Duration(config.PollInterval) * time.Second

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			// Continue with indexing logic
		}

		lastProcessedBlock, err := blockTracker.GetLastProcessedBlock()
		if err != nil {
			log.Warn("Could not get last processed block, retrying...",
				"error", err,
				"retry_interval", pollInterval)
			time.Sleep(pollInterval)
			continue
		}

		latestBlock, err := client.GetLatestBlockNumber(ctx)
		if err != nil {
			log.Warn("Could not get latest block number, retrying...",
				"error", err,
				"retry_interval", pollInterval)
			time.Sleep(pollInterval)
			continue
		}

		finalizedBlock := latestBlock.Uint64() - FinalizedBlockOffset

		if lastProcessedBlock >= finalizedBlock {
			log.Debug("Caught up to finalized block, waiting for new blocks...",
				"finalized_block", finalizedBlock,
				"last_processed", lastProcessedBlock,
				"poll_interval", pollInterval)
			time.Sleep(pollInterval)
			continue
		}

		log.Info("Processing block range",
			"from_block", lastProcessedBlock+1,
			"to_block", finalizedBlock,
			"total_blocks", finalizedBlock-lastProcessedBlock)

		for i := lastProcessedBlock + 1; i <= finalizedBlock; i++ {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			blockNum := big.NewInt(int64(i))

			// Download state diff
			stateDiff, err := client.GetStateDiff(ctx, blockNum)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				log.Warn("Could not get state diff for block, retrying...",
					"block_number", i,
					"error", err,
					"retry_interval", pollInterval)
				time.Sleep(pollInterval)
				i-- // Retry the same block
				continue
			}

			// Save state diff to file
			data, err := json.MarshalIndent(stateDiff, "", "  ")
			if err != nil {
				log.Error("Could not marshal state diff for block, skipping",
					"block_number", i,
					"error", err)
				continue
			}

			filename := fmt.Sprintf("%d.json", i)
			if err := fileStore.Save(filename, data); err != nil {
				log.Warn("Could not save state diff for block, retrying...",
					"block_number", i,
					"filename", filename,
					"error", err,
					"retry_interval", pollInterval)
				time.Sleep(pollInterval)
				i-- // Retry the same block
				continue
			}

			// Process state diff through indexer
			if err := indexerSvc.ProcessBlock(ctx, uint64(i)); err != nil {
				log.Warn("Could not process block, retrying...",
					"block_number", i,
					"error", err,
					"retry_interval", pollInterval)
				time.Sleep(pollInterval)
				i-- // Retry the same block
				continue
			}

			// Update last processed block
			if err := blockTracker.SetLastProcessedBlock(uint64(i)); err != nil {
				log.Error("Could not update last processed block",
					"block_number", i,
					"error", err)
				continue
			}

			log.Debug("Successfully processed block",
				"block_number", i,
				"filename", filename)
		}

		log.Info("Completed block range processing",
			"from_block", lastProcessedBlock+1,
			"to_block", finalizedBlock,
			"processed_blocks", finalizedBlock-lastProcessedBlock)
	}
}

func init() {
	rootCmd.AddCommand(runCmd)
}
