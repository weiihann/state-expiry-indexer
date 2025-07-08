package repository

import (
	"github.com/weiihann/state-expiry-indexer/internal"
)

// getTestDBConfig returns test database configuration matching testdb package
// This avoids import cycle by not importing testdb while using the same config
func getTestDBConfig() internal.Config {
	return internal.Config{
		DBHost:      "localhost",
		DBPort:      "15432",
		DBUser:      "test",
		DBPassword:  "test",
		DBName:      "test",
		DBMaxConns:  5,
		DBMinConns:  1,
		Environment: "test",
		ArchiveMode: false,
		RPCURLS:     []string{"http://localhost:8545"}, // Required for validation
	}
}

// getTestClickHouseConfig returns test ClickHouse configuration matching testdb package
func getTestClickHouseConfig() internal.Config {
	return internal.Config{
		ClickHouseHost:     "localhost",
		ClickHousePort:     "19010",
		ClickHouseUser:     "test_user",
		ClickHousePassword: "test_password",
		ClickHouseDatabase: "test_state_expiry",
		ClickHouseMaxConns: 5,
		ClickHouseMinConns: 1,
		Environment:        "test",
		ArchiveMode:        true,
		RPCURLS:            []string{"http://localhost:8545"}, // Required for validation
	}
}
