package indexer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weiihann/state-expiry-indexer/internal"
	"github.com/weiihann/state-expiry-indexer/pkg/rpc"
)

func TestStateAccess(t *testing.T) {
	t.Run("newStateAccess creates empty state", func(t *testing.T) {
		sa := newStateAccess()
		assert.Equal(t, 0, sa.count)
		assert.Equal(t, 0, len(sa.accounts))
		assert.Equal(t, 0, len(sa.accountType))
		assert.Equal(t, 0, len(sa.storage))
	})

	t.Run("addAccount adds new account", func(t *testing.T) {
		sa := newStateAccess()
		address := "0x1234567890abcdef1234567890abcdef12345678"
		blockNumber := uint64(100)
		isContract := true

		sa.addAccount(address, blockNumber, isContract)

		assert.Equal(t, 1, sa.count)
		assert.Equal(t, blockNumber, sa.accounts[address])
		assert.Equal(t, isContract, sa.accountType[address])
	})

	t.Run("addAccount updates existing account", func(t *testing.T) {
		sa := newStateAccess()
		address := "0x1234567890abcdef1234567890abcdef12345678"

		// Add account first time
		sa.addAccount(address, 100, false)
		assert.Equal(t, 1, sa.count)

		// Update same account
		sa.addAccount(address, 200, true)
		assert.Equal(t, 1, sa.count) // Count should not increase
		assert.Equal(t, uint64(200), sa.accounts[address])
		assert.Equal(t, true, sa.accountType[address])
	})

	t.Run("addStorage adds new storage slot", func(t *testing.T) {
		sa := newStateAccess()
		address := "0x1234567890abcdef1234567890abcdef12345678"
		slot := "0x0000000000000000000000000000000000000000000000000000000000000001"
		blockNumber := uint64(150)

		sa.addStorage(address, slot, blockNumber)

		assert.Equal(t, 1, sa.count)
		assert.Equal(t, blockNumber, sa.storage[address][slot])
	})

	t.Run("addStorage handles multiple slots for same address", func(t *testing.T) {
		sa := newStateAccess()
		address := "0x1234567890abcdef1234567890abcdef12345678"
		slot1 := "0x0000000000000000000000000000000000000000000000000000000000000001"
		slot2 := "0x0000000000000000000000000000000000000000000000000000000000000002"

		sa.addStorage(address, slot1, 100)
		sa.addStorage(address, slot2, 200)

		assert.Equal(t, 2, sa.count)
		assert.Equal(t, uint64(100), sa.storage[address][slot1])
		assert.Equal(t, uint64(200), sa.storage[address][slot2])
	})

	t.Run("addStorage updates existing slot", func(t *testing.T) {
		sa := newStateAccess()
		address := "0x1234567890abcdef1234567890abcdef12345678"
		slot := "0x0000000000000000000000000000000000000000000000000000000000000001"

		// Add slot first time
		sa.addStorage(address, slot, 100)
		assert.Equal(t, 1, sa.count)

		// Update same slot
		sa.addStorage(address, slot, 200)
		assert.Equal(t, 1, sa.count) // Count should not increase
		assert.Equal(t, uint64(200), sa.storage[address][slot])
	})

	t.Run("reset clears all data", func(t *testing.T) {
		sa := newStateAccess()
		
		// Add some data
		sa.addAccount("0x1234567890abcdef1234567890abcdef12345678", 100, true)
		sa.addStorage("0x1234567890abcdef1234567890abcdef12345678", "0x0000000000000000000000000000000000000000000000000000000000000001", 150)
		
		assert.Greater(t, sa.count, 0)
		assert.Greater(t, len(sa.accounts), 0)
		assert.Greater(t, len(sa.storage), 0)

		// Reset
		sa.reset()

		assert.Equal(t, 0, sa.count)
		assert.Equal(t, 0, len(sa.accounts))
		assert.Equal(t, 0, len(sa.accountType))
		assert.Equal(t, 0, len(sa.storage))
	})
}

func TestIndexer_DetermineAccountType(t *testing.T) {
	config := internal.Config{}
	indexer := NewIndexer(nil, nil, config)

	t.Run("identifies contract with code", func(t *testing.T) {
		diff := rpc.AccountDiff{
			Code: map[string]interface{}{
				"from": nil,
				"to":   "0x6080604052",
			},
		}

		isContract := indexer.determineAccountType(diff)
		assert.True(t, isContract)
	})

	t.Run("identifies EOA without code", func(t *testing.T) {
		diff := rpc.AccountDiff{
			Balance: map[string]interface{}{
				"from": "0x0",
				"to":   "0x100",
			},
		}

		isContract := indexer.determineAccountType(diff)
		assert.False(t, isContract)
	})

	t.Run("handles nil code field", func(t *testing.T) {
		diff := rpc.AccountDiff{
			Balance: map[string]interface{}{
				"from": "0x0",
				"to":   "0x100",
			},
			Code: nil,
		}

		isContract := indexer.determineAccountType(diff)
		assert.False(t, isContract)
	})

	t.Run("handles non-map code field", func(t *testing.T) {
		diff := rpc.AccountDiff{
			Code: "invalid_type",
		}

		isContract := indexer.determineAccountType(diff)
		assert.False(t, isContract)
	})
}