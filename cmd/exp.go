package cmd

import (
	"context"
	"os"

	"github.com/spf13/cobra"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/indexer"
	"github.com/weiihann/state-expiry-indexer/internal/logger"
	"github.com/weiihann/state-expiry-indexer/internal/repository"
	"github.com/weiihann/state-expiry-indexer/pkg/rpc"
	"github.com/weiihann/state-expiry-indexer/pkg/storage"
)

var expCmd = &cobra.Command{
	Use: "exp",
	Run: exp,
}

func exp(cmd *cobra.Command, args []string) {
	log := logger.GetLogger("exp-cmd")

	config, err := internal.LoadConfig("./configs")
	if err != nil {
		logger.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

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

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := indexerSvc.ProcessRangeDebug(ctx, 4901); err != nil {
		log.Error("Failed to process range", "error", err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(expCmd)
}
