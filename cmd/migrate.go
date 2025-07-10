package cmd

import (
	"fmt"
	"os"
	"strconv"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/clickhouse"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/logger"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Database migration commands",
	Long:  `Run database migrations using golang-migrate`,
}

var migrateUpCmd = &cobra.Command{
	Use:   "up [N]",
	Short: "Apply all or N ClickHouse up migrations",
	Long:  `Apply all pending ClickHouse migrations or specify a number to apply only N migrations`,
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		log := logger.GetLogger("migrate-ch-up")

		m := setupClickHouseMigrate()
		defer m.Close()

		if len(args) == 0 {
			// Apply all pending migrations
			if err := m.Up(); err != nil {
				if err == migrate.ErrNoChange {
					log.Info("No pending ClickHouse migrations to apply")
					return
				}
				log.Error("ClickHouse migration up failed", "error", err)
				os.Exit(1)
			}
			log.Info("All ClickHouse migrations applied successfully")
		} else {
			// Apply N migrations
			n, err := strconv.Atoi(args[0])
			if err != nil {
				log.Error("Invalid number of migrations", "error", err, "input", args[0])
				os.Exit(1)
			}
			if err := m.Steps(n); err != nil {
				if err == migrate.ErrNoChange {
					log.Info("No ClickHouse migrations to apply")
					return
				}
				log.Error("ClickHouse migration steps failed", "error", err, "steps", n)
				os.Exit(1)
			}
			log.Info("Applied ClickHouse migrations successfully", "count", n)
		}
	},
}

var migrateDownCmd = &cobra.Command{
	Use:   "down [N]",
	Short: "Apply all or N ClickHouse down migrations",
	Long:  `Apply all down ClickHouse migrations or specify a number to rollback N migrations`,
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		log := logger.GetLogger("migrate-ch-down")

		m := setupClickHouseMigrate()
		defer m.Close()

		if len(args) == 0 {
			// Apply all down migrations
			if err := m.Down(); err != nil {
				if err == migrate.ErrNoChange {
					log.Info("No ClickHouse migrations to roll back")
					return
				}
				log.Error("ClickHouse migration down failed", "error", err)
				os.Exit(1)
			}
			log.Info("All ClickHouse migrations rolled back successfully")
		} else {
			// Apply N down migrations
			n, err := strconv.Atoi(args[0])
			if err != nil {
				log.Error("Invalid number of migrations", "error", err, "input", args[0])
				os.Exit(1)
			}
			if err := m.Steps(-n); err != nil {
				if err == migrate.ErrNoChange {
					log.Info("No ClickHouse migrations to roll back")
					return
				}
				log.Error("ClickHouse migration steps failed", "error", err, "steps", -n)
				os.Exit(1)
			}
			log.Info("Rolled back ClickHouse migrations successfully", "count", n)
		}
	},
}

var migrateStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show ClickHouse migration status",
	Long:  `Display the current ClickHouse migration version and status`,
	Run: func(cmd *cobra.Command, args []string) {
		log := logger.GetLogger("migrate-ch-status")

		m := setupClickHouseMigrate()
		defer m.Close()

		version, dirty, err := m.Version()
		if err != nil {
			if err == migrate.ErrNilVersion {
				log.Info("ClickHouse Migration Status: No migrations applied")
				return
			}
			log.Error("Could not get ClickHouse migration status", "error", err)
			os.Exit(1)
		}

		status := "CLEAN"
		if dirty {
			status = "DIRTY (migration failed, manual intervention required)"
		}

		log.Info("ClickHouse migration status",
			"current_version", version,
			"status", status)
	},
}

var migrateVersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print current ClickHouse migration version",
	Long:  `Print the current ClickHouse migration version number`,
	Run: func(cmd *cobra.Command, args []string) {
		log := logger.GetLogger("migrate-ch-version")

		m := setupClickHouseMigrate()
		defer m.Close()

		version, _, err := m.Version()
		if err != nil {
			if err == migrate.ErrNilVersion {
				log.Info("No ClickHouse migrations applied")
				return
			}
			log.Error("Could not get ClickHouse migration version", "error", err)
			os.Exit(1)
		}

		log.Info("Current ClickHouse migration version", "version", version)
	},
}

var migrateForceCmd = &cobra.Command{
	Use:   "force VERSION",
	Short: "Force set ClickHouse migration version without running migration (fixes dirty state)",
	Long:  `Set the ClickHouse migration version without running the migration. This is used to fix dirty database state when a migration fails partway through.`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		log := logger.GetLogger("migrate-ch-force")

		m := setupClickHouseMigrate()
		defer m.Close()

		version, err := strconv.Atoi(args[0])
		if err != nil {
			log.Error("Invalid version number", "error", err, "input", args[0])
			os.Exit(1)
		}

		if err := m.Force(version); err != nil {
			log.Error("Failed to force ClickHouse migration version", "error", err, "version", version)
			os.Exit(1)
		}

		log.Info("ClickHouse migration version forced successfully", "version", version)
		log.Warn("IMPORTANT: Verify that the ClickHouse database state matches the expected state for this version")
	},
}

func setupClickHouseMigrate() *migrate.Migrate {
	log := logger.GetLogger("migrate-ch-setup")

	config, err := internal.LoadConfig("./configs")
	if err != nil {
		log.Error("Could not load config", "error", err)
		os.Exit(1)
	}

	// Get ClickHouse connection string
	connectionString := config.GetClickHouseConnectionString(true)
	p := &clickhouse.ClickHouse{}
	d, err := p.Open(connectionString)
	if err != nil {
		log.Error("Could not open ClickHouse connection", "error", err, "connection_string", connectionString)
		os.Exit(1)
	}
	// Remove defer d.Close() - let the migrate instance manage the connection

	// Create migrate instance with ClickHouse
	m, err := migrate.NewWithDatabaseInstance(
		"file://db/migrations",
		config.ClickHouseDatabase,
		d,
	)
	if err != nil {
		log.Error("Could not create ClickHouse migrate instance", "error", err, "connection_string", connectionString)
		os.Exit(1)
	}

	return m
}

func init() {
	migrateCmd.AddCommand(migrateUpCmd)
	migrateCmd.AddCommand(migrateDownCmd)
	migrateCmd.AddCommand(migrateStatusCmd)
	migrateCmd.AddCommand(migrateVersionCmd)
	migrateCmd.AddCommand(migrateForceCmd)
	rootCmd.AddCommand(migrateCmd)
}

// RunClickHouseMigrationsUp runs all pending ClickHouse migrations programmatically
func RunMigrationsUp(config internal.Config, path string) error {
	log := logger.GetLogger("migrate-ch-auto")

	// Get ClickHouse connection string
	connectionString := config.GetClickHouseConnectionString(true)

	// Create migrate instance with ClickHouse
	m, err := migrate.New(
		"file://"+path,
		connectionString)
	if err != nil {
		return fmt.Errorf("could not create ClickHouse migrate instance: %w", err)
	}
	defer m.Close()

	// Apply all pending migrations
	if err := m.Up(); err != nil {
		if err == migrate.ErrNoChange {
			log.Info("ClickHouse migrations are up to date")
			return nil
		}
		return fmt.Errorf("ClickHouse migration failed: %w", err)
	}

	log.Info("ClickHouse migrations applied successfully")
	return nil
}
