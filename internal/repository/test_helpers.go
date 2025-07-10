package repository

import (
	"github.com/weiihann/state-expiry-indexer/internal"
)

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
		RPCURLS:            []string{"http://localhost:8545"}, // Required for validation
	}
}
