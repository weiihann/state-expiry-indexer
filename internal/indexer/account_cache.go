package indexer

import (
	"github.com/VictoriaMetrics/fastcache"
)

const (
	accountCacheSize = 5 * 1024 * 1024 * 1024 // 15GB
)

type AccountCache struct {
	cache *fastcache.Cache
}

func NewAccountCache() *AccountCache {
	return &AccountCache{
		cache: fastcache.New(accountCacheSize),
	}
}

func (c *AccountCache) Get(addr string) (bool, bool) {
	val := c.cache.Get(nil, []byte(addr))
	if len(val) == 0 {
		return false, false
	}
	return val[0] == 1, true
}

func (c *AccountCache) Set(addr string, isContract bool) {
	bytes := []byte{0}
	if isContract {
		bytes = []byte{1}
	}
	c.cache.Set([]byte(addr), bytes)
}
