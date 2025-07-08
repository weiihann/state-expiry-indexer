package cmd

import (
	"fmt"
	"os"
	"strconv"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/clickhouse"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/weiihann/state-expiry-indexer/db"
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
	Short: "Apply all or N up migrations",
	Long:  `Apply all pending PostgreSQL migrations or specify a number to apply only N migrations`,
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
	Long:  `Apply all down PostgreSQL migrations or specify a number to rollback N migrations`,
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
	Short: "Show PostgreSQL migration status",
	Long:  `Display the current PostgreSQL migration version and status`,
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

		log.Info("PostgreSQL migration status",
			"current_version", version,
			"status", status)
	},
}

var migrateVersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print current PostgreSQL migration version",
	Long:  `Print the current PostgreSQL migration version number`,
	Run: func(cmd *cobra.Command, args []string) {
		log := logger.GetLogger("migrate-version")

		m := setupMigrate()
		defer m.Close()

		version, _, err := m.Version()
		if err != nil {
			if err == migrate.ErrNilVersion {
				log.Info("No PostgreSQL migrations applied")
				return
			}
			log.Error("Could not get migration version", "error", err)
			os.Exit(1)
		}

		log.Info("Current PostgreSQL migration version", "version", version)
	},
}

var migrateForceCmd = &cobra.Command{
	Use:   "force VERSION",
	Short: "Force set PostgreSQL migration version without running migration (fixes dirty state)",
	Long:  `Set the PostgreSQL migration version without running the migration. This is used to fix dirty database state when a migration fails partway through.`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		log := logger.GetLogger("migrate-force")

		m := setupMigrate()
		defer m.Close()

		version, err := strconv.Atoi(args[0])
		if err != nil {
			log.Error("Invalid version number", "error", err, "input", args[0])
			os.Exit(1)
		}

		if err := m.Force(version); err != nil {
			log.Error("Failed to force migration version", "error", err, "version", version)
			os.Exit(1)
		}

		log.Info("PostgreSQL migration version forced successfully", "version", version)
		log.Warn("IMPORTANT: Verify that the database state matches the expected state for this version")
	},
}

// ClickHouse migration commands (new)
var migrateChCmd = &cobra.Command{
	Use:   "ch",
	Short: "ClickHouse migration commands",
	Long:  `Run ClickHouse database migrations for archive mode`,
}

var migrateChUpCmd = &cobra.Command{
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

var migrateChDownCmd = &cobra.Command{
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

var migrateChStatusCmd = &cobra.Command{
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

var migrateChVersionCmd = &cobra.Command{
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

var migrateChForceCmd = &cobra.Command{
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

func setupMigrate() *migrate.Migrate {
	log := logger.GetLogger("migrate-setup")

	config, err := internal.LoadConfig("./configs")
	if err != nil {
		log.Error("Could not load config", "error", err)
		os.Exit(1)
	}

	// Create database connection
	db, err := db.ConnectSQL(config)
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
	defer d.Close()

	// Create migrate instance with ClickHouse
	m, err := migrate.NewWithDatabaseInstance(
		"file://db/ch-migrations",
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
	// PostgreSQL migration commands
	migrateCmd.AddCommand(migrateUpCmd)
	migrateCmd.AddCommand(migrateDownCmd)
	migrateCmd.AddCommand(migrateStatusCmd)
	migrateCmd.AddCommand(migrateVersionCmd)
	migrateCmd.AddCommand(migrateForceCmd)

	// ClickHouse migration commands
	migrateChCmd.AddCommand(migrateChUpCmd)
	migrateChCmd.AddCommand(migrateChDownCmd)
	migrateChCmd.AddCommand(migrateChStatusCmd)
	migrateChCmd.AddCommand(migrateChVersionCmd)
	migrateChCmd.AddCommand(migrateChForceCmd)

	migrateCmd.AddCommand(migrateChCmd)
	rootCmd.AddCommand(migrateCmd)
}

// RunMigrationsUp runs all pending migrations programmatically
// Used by the run command to ensure database is up to date before starting services
func RunMigrationsUp(config internal.Config) error {
	log := logger.GetLogger("migrate-auto")

	if config.ArchiveMode {
		return RunClickHouseMigrationsUp(config)
	}

	// Create database connection
	db, err := db.ConnectSQL(config)
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

// RunClickHouseMigrationsUp runs all pending ClickHouse migrations programmatically
// Used by the run command with --archive flag to ensure ClickHouse database is up to date
func RunClickHouseMigrationsUp(config internal.Config) error {
	log := logger.GetLogger("migrate-ch-auto")

	// Get ClickHouse connection string
	connectionString := config.GetClickHouseConnectionString(true)

	// Create migrate instance with ClickHouse
	m, err := migrate.New(
		"file://db/ch-migrations",
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
