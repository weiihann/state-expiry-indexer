package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"strconv"
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
	PollInterval         = 10 * time.Second
	DefaultAPIPort       = 8080
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the state expiry indexer with API server",
	Long:  `This command starts the state expiry indexer and API server concurrently. The indexer polls for new blocks, saves their state diffs, processes them, and updates the database. The API server serves queries from the same database.`,
	Run: func(cmd *cobra.Command, args []string) {
		log := logger.GetLogger("run-cmd")

		// Load configuration
		config, err := internal.LoadConfig("./configs")
		if err != nil {
			log.Error("Failed to load configuration", "error", err)
			os.Exit(1)
		}

		// Set default API port if not configured
		if config.APIPort == 0 {
			config.APIPort = DefaultAPIPort
		}

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
		log.Info("Initializing RPC client...", "rpc_url", config.RPCURL)
		client, err := rpc.NewClient(ctx, config.RPCURL)
		if err != nil {
			log.Error("Failed to create RPC client", "error", err, "rpc_url", config.RPCURL)
			os.Exit(1)
		}

		// Initialize file storage
		log.Info("Initializing file storage...", "path", "data/statediffs")
		fileStore, err := storage.NewFileStore("data/statediffs")
		if err != nil {
			log.Error("Failed to create file store", "error", err, "path", "data/statediffs")
			os.Exit(1)
		}

		// Initialize block tracker
		blockTracker := tracker.NewTracker()

		// Initialize indexer service
		indexerSvc := indexer.NewService("data", repo, 0)

		// Initialize API server
		log.Info("Initializing API server...", "port", config.APIPort)
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
			log.Info("Starting API server", "port", config.APIPort)

			if err := apiServer.Run(config.APIPort); err != nil {
				log.Error("API server error", "error", err, "port", config.APIPort)
			}
		}()

		// Start indexer workflow in goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("Starting indexer workflow...")

			if err := runIndexerWorkflow(ctx, client, fileStore, blockTracker, indexerSvc); err != nil {
				log.Error("Indexer workflow error", "error", err)
			}
			log.Info("Indexer workflow stopped")
		}()

		log.Info("All services started successfully",
			"api_port", config.APIPort,
			"api_url", "http://localhost:"+strconv.Itoa(config.APIPort))
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
func runIndexerWorkflow(ctx context.Context, client *rpc.Client, fileStore *storage.FileStore, blockTracker *tracker.Tracker, indexerSvc *indexer.Service) error {
	log := logger.GetLogger("indexer-workflow")

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
				"retry_interval", PollInterval)
			time.Sleep(PollInterval)
			continue
		}

		latestBlock, err := client.GetLatestBlockNumber(ctx)
		if err != nil {
			log.Warn("Could not get latest block number, retrying...",
				"error", err,
				"retry_interval", PollInterval)
			time.Sleep(PollInterval)
			continue
		}

		finalizedBlock := latestBlock.Uint64() - FinalizedBlockOffset

		if lastProcessedBlock >= finalizedBlock {
			log.Debug("Caught up to finalized block, waiting for new blocks...",
				"finalized_block", finalizedBlock,
				"last_processed", lastProcessedBlock)
			time.Sleep(PollInterval)
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
					"retry_interval", PollInterval)
				time.Sleep(PollInterval)
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
					"retry_interval", PollInterval)
				time.Sleep(PollInterval)
				i-- // Retry the same block
				continue
			}

			// Process the saved file through indexer
			if err := indexerSvc.ProcessBlock(context.Background(), i); err != nil {
				log.Warn("Could not process block through indexer, retrying...",
					"block_number", i,
					"error", err,
					"retry_interval", PollInterval)
				time.Sleep(PollInterval)
				i-- // Retry the same block
				continue
			}

			// Update block tracker
			if err := blockTracker.SetLastProcessedBlock(i); err != nil {
				log.Warn("Could not set last processed block, retrying...",
					"block_number", i,
					"error", err,
					"retry_interval", PollInterval)
				time.Sleep(PollInterval)
				i-- // Retry the same block
				continue
			}

			log.Info("Successfully processed and indexed block",
				"block_number", i)
		}
	}
}

func init() {
	rootCmd.AddCommand(runCmd)
}
