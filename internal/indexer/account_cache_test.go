package indexer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAccountCache(t *testing.T) {
	cache := NewAccountCache()
	cache.Set("0x123", true)
	isContract, ok := cache.Get("0x123")
	assert.True(t, isContract)
	assert.True(t, ok)

	isContract, ok = cache.Get("0x456")
	assert.False(t, isContract)
	assert.False(t, ok)
}
