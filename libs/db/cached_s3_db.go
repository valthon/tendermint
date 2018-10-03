package db

import (
	"github.com/karlseguin/ccache"
	"time"
)

// Simple Caching wrapper around S3DB using karlseguin/ccache
type CachedS3DB struct {
	*S3DB
	item_cache *ccache.Cache
	key_cache  *ccache.Cache
}

var _ DB = (*CachedS3DB)(nil)

// Register S3DBBackend
func init() {
	dbCreator := func(name string, dir string) (DB, error) {
		return NewCachedS3DB(name, dir)
	}
	registerDBCreator(S3DBBackend, dbCreator, true)
}

// Initialize a new DB instance wrapping an S3DB instance and two LRU caches
// item_cache is for caching the key values (Get)
// key_cache is for caching key presence (Has)
func NewCachedS3DB(name string, dir string) (*CachedS3DB, error) {
	db, err := NewS3DB(name, dir)
	if err != nil {
		return nil, err
	}
	// TODO: profile and tune cache sizes and timeouts
	item_cache := ccache.New(ccache.Configure().MaxSize(5000).ItemsToPrune(200))
	key_cache := ccache.New(ccache.Configure().MaxSize(25000).ItemsToPrune(500))
	return &CachedS3DB{S3DB: db, item_cache: item_cache, key_cache: key_cache}, nil
}

// We want a very long cache timeout, since we don't expect the data to change outside of this node
const CACHE_TIMEOUT = time.Hour * 12

// Implements DB.
// Checks key_cache for positive response.
// Checks S3DB if positive response not cached.
// Does not use Fetch because we never want to cache a negative response.
func (db *CachedS3DB) Get(key []byte) []byte {
	item := db.item_cache.Get(string(key))
	if item == nil {
		lookup := db.S3DB.Get(key)
		if lookup != nil {
			// Only store non-nil value in cache
			db.item_cache.Set(string(key), lookup, CACHE_TIMEOUT)
		}
		return lookup
	} else {
		return item.Value().([]byte)
	}
}

// Implements DB.
// Checks key_cache for positive response.
// Checks S3DB if positive response not cached.
// Does not use Fetch because we never want to cache a negative response.
func (db *CachedS3DB) Has(key []byte) bool {
	item := db.key_cache.Get(string(key))
	if item == nil || !item.Value().(bool) {
		lookup := db.S3DB.Has(key)
		if lookup {
			// Only store positive presence in cache
			db.key_cache.Set(string(key), true, CACHE_TIMEOUT)
		}
		return lookup
	} else {
		return true
	}
}

// Implements DB.
// Proxies to S3DB, then sets cache values
func (db *CachedS3DB) Set(key []byte, value []byte) {
	db.S3DB.Set(key, value)
	db.key_cache.Set(string(key), true, CACHE_TIMEOUT)
	db.item_cache.Set(string(key), value, CACHE_TIMEOUT)
}

// Implements DB.
// Purges cache, then proxies to S3DB
func (db *CachedS3DB) Delete(key []byte) {
	db.key_cache.Delete(string(key))
	db.item_cache.Delete(string(key))
	db.S3DB.Delete(key)
}
