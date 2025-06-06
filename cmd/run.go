package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/api"
	"github.com/weiihann/state-expiry-indexer/internal/caller"
	"github.com/weiihann/state-expiry-indexer/internal/database"
	"github.com/weiihann/state-expiry-indexer/internal/indexer"
	"github.com/weiihann/state-expiry-indexer/internal/logger"
	"github.com/weiihann/state-expiry-indexer/internal/repository"
	"github.com/weiihann/state-expiry-indexer/pkg/rpc"
	"github.com/weiihann/state-expiry-indexer/pkg/storage"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the state expiry indexer with separated RPC caller and processor workflows",
	Long:  `This command starts the separated RPC caller, indexer processor, and API server concurrently. The RPC caller downloads state diffs, the processor indexes them into the database, and the API server serves queries.`,
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

		// Initialize services with separated workflows
		rpcCallerSvc := caller.NewService(client, fileStore, config)
		indexerSvc := indexer.NewService(config.DataDir, repo, config)

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

		// Start RPC caller workflow in goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("Starting RPC caller workflow...",
				"poll_interval", config.PollInterval)

			if err := rpcCallerSvc.Run(ctx); err != nil {
				log.Error("RPC caller workflow error", "error", err)
			}
			log.Info("RPC caller workflow stopped")
		}()

		// Start indexer processor workflow in goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("Starting indexer processor workflow...",
				"poll_interval", config.PollInterval,
				"block_batch_size", config.BlockBatchSize)

			if err := indexerSvc.RunProcessor(ctx); err != nil {
				log.Error("Indexer processor workflow error", "error", err)
			}
			log.Info("Indexer processor workflow stopped")
		}()

		log.Info("All services started successfully",
			"api_port", config.APIPort,
			"api_url", fmt.Sprintf("http://%s:%d", config.APIHost, config.APIPort),
			"environment", config.Environment,
			"data_dir", config.DataDir)
		log.Info("Services running",
			"api_available", true,
			"rpc_caller_running", true,
			"indexer_processor_running", true)
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

func init() {
	rootCmd.AddCommand(runCmd)
}
