package cmd

import (
	"fmt"
	"os"
	"strconv"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/database"
	"github.com/weiihann/state-expiry-indexer/internal/logger"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Database migration commands",
	Long:  `Run database migrations using golang-migrate`,
}

var migrateUpCmd = &cobra.Command{
	Use:   "up [N]",
	Short: "Apply all or N up migrations",
	Long:  `Apply all pending migrations or specify a number to apply only N migrations`,
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		log := logger.GetLogger("migrate-up")

		m := setupMigrate()
		defer m.Close()

		if len(args) == 0 {
			// Apply all pending migrations
			if err := m.Up(); err != nil {
				if err == migrate.ErrNoChange {
					log.Info("No pending migrations to apply")
					return
				}
				log.Error("Migration up failed", "error", err)
				os.Exit(1)
			}
			log.Info("All migrations applied successfully")
		} else {
			// Apply N migrations
			n, err := strconv.Atoi(args[0])
			if err != nil {
				log.Error("Invalid number of migrations", "error", err, "input", args[0])
				os.Exit(1)
			}
			if err := m.Steps(n); err != nil {
				if err == migrate.ErrNoChange {
					log.Info("No migrations to apply")
					return
				}
				log.Error("Migration steps failed", "error", err, "steps", n)
				os.Exit(1)
			}
			log.Info("Applied migrations successfully", "count", n)
		}
	},
}

var migrateDownCmd = &cobra.Command{
	Use:   "down [N]",
	Short: "Apply all or N down migrations",
	Long:  `Apply all down migrations or specify a number to rollback N migrations`,
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		log := logger.GetLogger("migrate-down")

		m := setupMigrate()
		defer m.Close()

		if len(args) == 0 {
			// Apply all down migrations
			if err := m.Down(); err != nil {
				if err == migrate.ErrNoChange {
					log.Info("No migrations to roll back")
					return
				}
				log.Error("Migration down failed", "error", err)
				os.Exit(1)
			}
			log.Info("All migrations rolled back successfully")
		} else {
			// Apply N down migrations
			n, err := strconv.Atoi(args[0])
			if err != nil {
				log.Error("Invalid number of migrations", "error", err, "input", args[0])
				os.Exit(1)
			}
			if err := m.Steps(-n); err != nil {
				if err == migrate.ErrNoChange {
					log.Info("No migrations to roll back")
					return
				}
				log.Error("Migration steps failed", "error", err, "steps", -n)
				os.Exit(1)
			}
			log.Info("Rolled back migrations successfully", "count", n)
		}
	},
}

var migrateStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show migration status",
	Long:  `Display the current migration version and status`,
	Run: func(cmd *cobra.Command, args []string) {
		log := logger.GetLogger("migrate-status")

		m := setupMigrate()
		defer m.Close()

		version, dirty, err := m.Version()
		if err != nil {
			if err == migrate.ErrNilVersion {
				log.Info("Migration Status: No migrations applied")
				return
			}
			log.Error("Could not get migration status", "error", err)
			os.Exit(1)
		}

		status := "CLEAN"
		if dirty {
			status = "DIRTY (migration failed, manual intervention required)"
		}

		log.Info("Migration status",
			"current_version", version,
			"status", status)
	},
}

var migrateVersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print current migration version",
	Long:  `Print the current migration version number`,
	Run: func(cmd *cobra.Command, args []string) {
		log := logger.GetLogger("migrate-version")

		m := setupMigrate()
		defer m.Close()

		version, _, err := m.Version()
		if err != nil {
			if err == migrate.ErrNilVersion {
				log.Info("No migrations applied")
				return
			}
			log.Error("Could not get migration version", "error", err)
			os.Exit(1)
		}

		log.Info("Current migration version", "version", version)
	},
}

func setupMigrate() *migrate.Migrate {
	log := logger.GetLogger("migrate-setup")

	config, err := internal.LoadConfig("./configs")
	if err != nil {
		log.Error("Could not load config", "error", err)
		os.Exit(1)
	}

	// Create database connection
	db, err := database.ConnectSQL(config)
	if err != nil {
		log.Error("Could not connect to database", "error", err)
		os.Exit(1)
	}

	// Create postgres driver instance
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		log.Error("Could not create postgres driver", "error", err)
		os.Exit(1)
	}

	// Create migrate instance
	m, err := migrate.NewWithDatabaseInstance(
		"file://db/migrations",
		"postgres", driver)
	if err != nil {
		log.Error("Could not create migrate instance", "error", err)
		os.Exit(1)
	}

	return m
}

func init() {
	migrateCmd.AddCommand(migrateUpCmd)
	migrateCmd.AddCommand(migrateDownCmd)
	migrateCmd.AddCommand(migrateStatusCmd)
	migrateCmd.AddCommand(migrateVersionCmd)
	rootCmd.AddCommand(migrateCmd)
}

// RunMigrationsUp runs all pending migrations programmatically
// Used by the run command to ensure database is up to date before starting services
func RunMigrationsUp(config internal.Config) error {
	log := logger.GetLogger("migrate-auto")

	// Create database connection
	db, err := database.ConnectSQL(config)
	if err != nil {
		return fmt.Errorf("could not connect to database: %w", err)
	}
	defer db.Close()

	// Create postgres driver instance
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("could not create postgres driver: %w", err)
	}

	// Create migrate instance
	m, err := migrate.NewWithDatabaseInstance(
		"file://db/migrations",
		"postgres", driver)
	if err != nil {
		return fmt.Errorf("could not create migrate instance: %w", err)
	}
	defer m.Close()

	// Apply all pending migrations
	if err := m.Up(); err != nil {
		if err == migrate.ErrNoChange {
			log.Info("Database migrations are up to date")
			return nil
		}
		return fmt.Errorf("migration failed: %w", err)
	}

	log.Info("Database migrations applied successfully")
	return nil
}
