package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/logger"
)

var (
	startBlock uint64
	endBlock   uint64
)

var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Iterate through all the files in the data directory and verify that the sequence is correct",
	Run:   verify,
}

func verify(cmd *cobra.Command, args []string) {
	log := logger.GetLogger("verify")

	// Load configuration with enhanced validation
	config, err := internal.LoadConfig("./configs")
	if err != nil {
		log.Error("Configuration validation failed", "error", err)
		os.Exit(1)
	}

	log.Info("Configuration loaded successfully", "data_dir", config.DataDir)

	missingBlocks := []uint64{}
	for i := startBlock; i <= endBlock; i++ {
		if _, err := os.Stat(filepath.Join(config.DataDir, fmt.Sprintf("%d.json", i))); os.IsNotExist(err) {
			missingBlocks = append(missingBlocks, i)
			log.Error("State diff not found", "block", i)
		} else if err != nil {
			log.Error("Failed to check state diff file", "error", err, "block", i)
			os.Exit(1)
		}
	}

	log.Info("Verification completed successfully", "missing_count", len(missingBlocks), "missing_blocks", missingBlocks)
}

func init() {
	verifyCmd.Flags().Uint64Var(&startBlock, "start-block", 0, "Start block")
	verifyCmd.Flags().Uint64Var(&endBlock, "end-block", 0, "End block")
	rootCmd.AddCommand(verifyCmd)
}
