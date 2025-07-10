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
	"github.com/weiihann/state-expiry-indexer/internal/indexer"
	"github.com/weiihann/state-expiry-indexer/internal/logger"
	"github.com/weiihann/state-expiry-indexer/internal/repository"
	"github.com/weiihann/state-expiry-indexer/pkg/rpc"
	"github.com/weiihann/state-expiry-indexer/pkg/storage"
)

var archiveMode bool

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the state expiry indexer with separated RPC caller and processor workflows",
	Long:  `This command starts the separated RPC caller, indexer processor, and API server concurrently. The RPC caller downloads state diffs, the processor indexes them into the database, and the API server serves queries. Use --archive to enable ClickHouse archive mode with complete state history.`,
	Run:   run,
}

func init() {
	runCmd.Flags().BoolVar(&archiveMode, "archive", false, "Enable archive mode with ClickHouse for complete state access history")
	rootCmd.AddCommand(runCmd)
}

func run(cmd *cobra.Command, args []string) {
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
	)

	// Run database migrations
	log.Info("Checking database migrations...")
	migrationPath := "db/migrations"
	if err := RunMigrationsUp(config, migrationPath); err != nil {
		log.Error("Failed to run database migrations", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	// Initialize repository based on archive mode using factory
	repo, err := repository.NewRepository(ctx, config)
	if err != nil {
		log.Error("Failed to initialize repository", "error", err)
		os.Exit(1)
	}

	log.Info("Repository initialized successfully")

	// Initialize RPC clients
	var clients []*rpc.Client
	for _, rpcURL := range config.RPCURLS {
		log.Info("Initializing RPC client...", "rpc_url", rpcURL)
		client, err := rpc.NewClient(ctx, rpcURL)
		if err != nil {
			log.Error("Failed to create RPC client", "error", err, "rpc_url", rpcURL)
			os.Exit(1)
		}
		clients = append(clients, client)
	}

	// Initialize file storage using config paths
	log.Info("Initializing file storage...", "path", config.DataDir, "compression_enabled", config.CompressionEnabled)
	fileStore, err := storage.NewFileStoreWithCompression(config.DataDir, config.CompressionEnabled)
	if err != nil {
		log.Error("Failed to create file store", "error", err, "path", config.DataDir)
		os.Exit(1)
	}
	defer fileStore.Close()

	// Initialize services only if not in download-only mode
	var indexerSvc *indexer.Service
	var apiServer *api.Server

	// Use the first RPC client for the indexer (could be enhanced to use multiple clients)
	var rpcClient *rpc.Client
	if len(clients) > 0 {
		rpcClient = clients[0]
	}

	indexerSvc = indexer.NewService(repo, rpcClient, config)
	if indexerSvc == nil {
		log.Error("Failed to create indexer service")
		os.Exit(1)
	}
	defer indexerSvc.Close()

	// Initialize API server
	log.Info("Initializing API server...", "host", config.APIHost, "port", config.APIPort)
	apiServer = api.NewServer(repo, rpcClient, uint64(config.RangeSize))

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	// Start API server in goroutine (only if not in download-only mode)
	wg.Add(1)
	go func() {
		defer wg.Done()

		if err := apiServer.Run(ctx, config.APIHost, config.APIPort); err != nil {
			log.Error("API server error", "error", err, "host", config.APIHost, "port", config.APIPort)
		}
	}()

	// Start indexer processor workflow in goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("Starting range-based indexer processor workflow...",
			"poll_interval", config.PollInterval,
			"range_size", config.RangeSize)

		if err := indexerSvc.RunProcessor(ctx); err != nil {
			log.Error("Range-based indexer processor workflow error", "error", err)
		}
		log.Info("Range-based indexer processor workflow stopped")
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
}
