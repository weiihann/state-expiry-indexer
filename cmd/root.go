package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/logger"
)

var (
	logLevel  string
	logFormat string
	noColor   bool
)

var rootCmd = &cobra.Command{
	Use:   "state-expiry-indexer",
	Short: "A CLI for the State Expiry Indexer",
	Long:  `state-expiry-indexer is a tool to index Ethereum state and identify expired accounts.`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		config, err := internal.LoadConfig("config.yaml")
		if err != nil {
			logger.Error("Failed to load config", "error", err)
			os.Exit(1)
		}
		// Initialize logger with CLI flags
		logger.Initialize(logger.Config{
			Level:        logger.LogLevel(strings.ToLower(config.LogLevel)),
			Format:       config.LogFormat,
			EnableColors: !noColor,
		})
	},
}

func init() {
	rootCmd.PersistentFlags().BoolVar(&noColor, "no-color", false, "Disable colored output")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
