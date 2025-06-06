package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/weiihann/state-expiry-indexer/pkg/utils"
)

type Contract struct {
	Address   string `json:"address"`
	SlotCount int    `json:"slot_count"`
}

type StateRepository struct {
	db *pgxpool.Pool
}

func NewStateRepository(db *pgxpool.Pool) *StateRepository {
	return &StateRepository{db: db}
}

func (r *StateRepository) GetLastIndexedBlock(ctx context.Context) (uint64, error) {
	var value string
	err := r.db.QueryRow(ctx, "SELECT value FROM metadata WHERE key = 'last_indexed_block'").Scan(&value)
	if err != nil {
		if err == pgx.ErrNoRows {
			// This can happen if the metadata table is empty. Assume we start from 0.
			return 0, nil
		}
		return 0, fmt.Errorf("could not get last indexed block: %w", err)
	}

	var blockNumber uint64
	if _, err := fmt.Sscanf(value, "%d", &blockNumber); err != nil {
		return 0, fmt.Errorf("could not parse last indexed block value '%s': %w", value, err)
	}

	return blockNumber, nil
}

func (r *StateRepository) UpdateBlockDataInTx(ctx context.Context, blockNumber uint64, accounts map[string]bool, storage map[string]map[string]bool) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := r.upsertAccessedAccountsInTx(ctx, tx, blockNumber, accounts); err != nil {
		return err
	}

	if err := r.upsertAccessedStorageInTx(ctx, tx, blockNumber, storage); err != nil {
		return err
	}

	sql := `UPDATE metadata SET value = $1 WHERE key = 'last_indexed_block'`
	if _, err := tx.Exec(ctx, sql, fmt.Sprintf("%d", blockNumber)); err != nil {
		return fmt.Errorf("could not update last indexed block in tx: %w", err)
	}

	return tx.Commit(ctx)
}

func (r *StateRepository) upsertAccessedAccountsInTx(ctx context.Context, tx pgx.Tx, blockNumber uint64, accounts map[string]bool) error {
	if len(accounts) == 0 {
		return nil
	}

	var values []interface{}
	var placeholders []string
	i := 1
	for acc := range accounts {
		addr, err := utils.HexToBytes(acc)
		if err != nil {
			continue
		}
		placeholders = append(placeholders, fmt.Sprintf("($%d, $%d)", i, i+1))
		values = append(values, addr, blockNumber)
		i += 2
	}

	if len(placeholders) == 0 {
		return nil
	}

	sql := `
		INSERT INTO accounts_current (address, last_access_block)
		VALUES %s
		ON CONFLICT (address) DO UPDATE
		SET last_access_block = EXCLUDED.last_access_block
		WHERE accounts_current.last_access_block < EXCLUDED.last_access_block;
	`
	query := fmt.Sprintf(sql, strings.Join(placeholders, ","))

	_, err := tx.Exec(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("could not upsert accessed accounts in tx: %w", err)
	}

	return nil
}

func (r *StateRepository) upsertAccessedStorageInTx(ctx context.Context, tx pgx.Tx, blockNumber uint64, storage map[string]map[string]bool) error {
	if len(storage) == 0 {
		return nil
	}

	var values []interface{}
	var placeholders []string
	i := 1
	for addr, slots := range storage {
		for slot := range slots {
			addressBytes, err := utils.HexToBytes(addr)
			if err != nil {
				continue
			}
			slotBytes, err := utils.HexToBytes(slot)
			if err != nil {
				continue
			}
			placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d)", i, i+1, i+2))
			values = append(values, addressBytes, slotBytes, blockNumber)
			i += 3
		}
	}

	if len(placeholders) == 0 {
		return nil
	}

	sql := `
		INSERT INTO storage_current (address, slot_key, last_access_block)
		VALUES %s
		ON CONFLICT (address, slot_key) DO UPDATE
		SET last_access_block = EXCLUDED.last_access_block
		WHERE storage_current.last_access_block < EXCLUDED.last_access_block;
	`
	query := fmt.Sprintf(sql, strings.Join(placeholders, ","))

	_, err := tx.Exec(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("could not upsert accessed storage in tx: %w", err)
	}

	return nil
}

func (r *StateRepository) GetExpiredStateCount(ctx context.Context, expiryBlock uint64) (int, error) {
	var accountCount int
	accountQuery := `SELECT COUNT(*) FROM accounts_current WHERE last_access_block < $1;`
	err := r.db.QueryRow(ctx, accountQuery, expiryBlock).Scan(&accountCount)
	if err != nil {
		return 0, fmt.Errorf("could not get expired account count: %w", err)
	}

	var storageCount int
	storageQuery := `SELECT COUNT(*) FROM storage_current WHERE last_access_block < $1;`
	err = r.db.QueryRow(ctx, storageQuery, expiryBlock).Scan(&storageCount)
	if err != nil {
		return 0, fmt.Errorf("could not get expired storage count: %w", err)
	}

	return accountCount + storageCount, nil
}

func (r *StateRepository) GetTopNExpiredContracts(ctx context.Context, expiryBlock uint64, n int) ([]Contract, error) {
	query := `
		SELECT
			address,
			COUNT(slot_key) as slot_count
		FROM
			storage_current
		WHERE
			last_access_block < $1
		GROUP BY
			address
		ORDER BY
			slot_count DESC
		LIMIT $2;
	`

	rows, err := r.db.Query(ctx, query, expiryBlock, n)
	if err != nil {
		return nil, fmt.Errorf("could not query for top expired contracts: %w", err)
	}
	defer rows.Close()

	var contracts []Contract
	for rows.Next() {
		var contract Contract
		var addressBytes []byte
		if err := rows.Scan(&addressBytes, &contract.SlotCount); err != nil {
			return nil, fmt.Errorf("could not scan contract row: %w", err)
		}
		contract.Address = utils.BytesToHex(addressBytes)
		contracts = append(contracts, contract)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over contract rows: %w", err)
	}

	return contracts, nil
}

func (r *StateRepository) GetStateLastAccessedBlock(ctx context.Context, address string, slot *string) (uint64, error) {
	var lastAccessBlock uint64
	var err error

	addressBytes, err := utils.HexToBytes(address)
	if err != nil {
		return 0, fmt.Errorf("invalid address hex: %w", err)
	}

	if slot == nil {
		// Query for account
		query := `SELECT last_access_block FROM accounts_current WHERE address = $1;`
		err = r.db.QueryRow(ctx, query, addressBytes).Scan(&lastAccessBlock)
	} else {
		// Query for storage slot
		slotBytes, err := utils.HexToBytes(*slot)
		if err != nil {
			return 0, fmt.Errorf("invalid slot hex: %w", err)
		}
		query := `SELECT last_access_block FROM storage_current WHERE address = $1 AND slot_key = $2;`
		err = r.db.QueryRow(ctx, query, addressBytes, slotBytes).Scan(&lastAccessBlock)
	}

	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil // Not found
		}
		return 0, fmt.Errorf("could not get last access block: %w", err)
	}

	return lastAccessBlock, nil
}
