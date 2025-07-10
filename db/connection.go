package db

import (
	"database/sql"
	"fmt"

	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/internal/logger"
)

// ConnectClickHouseSQL creates a standard database/sql ClickHouse connection for golang-migrate
func ConnectClickHouseSQL(config internal.Config) (*sql.DB, error) {
	log := logger.GetLogger("clickhouse-migration")
	connStr := config.GetClickHouseConnectionString(false)

	// Open ClickHouse connection using database/sql interface
	db, err := sql.Open("clickhouse", connStr)
	if err != nil {
		return nil, fmt.Errorf("could not open ClickHouse connection: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("could not ping ClickHouse database: %w", err)
	}

	log.Info("ClickHouse migration connection established",
		"connection_string", connStr,
		"host", config.ClickHouseHost,
		"port", config.ClickHousePort,
		"database", config.ClickHouseDatabase)

	return db, nil
}
