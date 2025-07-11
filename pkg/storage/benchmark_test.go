package storage

import (
	"fmt"
	"testing"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/allegro/bigcache/v3"
	"github.com/hashicorp/golang-lru/v2"
)

func BenchmarkMap(b *testing.B) {
	cache := make(map[string]bool, 10000000)

	// Fill up the cache
	for i := 0; i < 10000000; i++ {
		cache[fmt.Sprintf("key%019d", i)] = true
	}

	// Benchmark
	for i := 0; i < b.N; i++ {
		_ = cache[fmt.Sprintf("key%019d", i%10000000)]
	}
}

func BenchmarkLRU(b *testing.B) {
	cache, _ := lru.New[string, bool](1000000)

	// Fill up the cache
	for i := 0; i < 5000000; i++ {
		cache.Add(fmt.Sprintf("key%019d", i), true)
	}

	// Benchmark
	for i := 0; i < b.N; i++ {
		cache.Get(fmt.Sprintf("key%019d", i%5000000))
	}
}

func TestFastCache(t *testing.T) {
	cache := fastcache.New(1024)

	key := fmt.Sprintf("0x%040x", 1000000)
	cache.Set([]byte(key), []byte{0})

	val := cache.Get(nil, []byte(key))
	fmt.Println(val)
}

func BenchmarkBigCache(b *testing.B) {
	cache, err := bigcache.New(b.Context(), bigcache.DefaultConfig(10000000))
	if err != nil {
		b.Fatal(err)
	}

	// Fill up the cache
	for i := 0; i < 10000000; i++ {
		cache.Set(fmt.Sprintf("key%019d", i), []byte{0})
	}

	// Benchmark
	key := fmt.Sprintf("key%019d", 10000000)
	for i := 0; i < b.N; i++ {
		cache.Get(key)
	}
}
