package reader

import (
	"fmt"
	"github.com/dgraph-io/ristretto"
	"packx/models"
	"sync"
	"time"
)

const (
	// Cache configuration
	maxCacheSize = 1 << 30 // 1GB

	numCounters = 1e7 // Number of keys to track frequency

	bufferItems = 64 // Number of keys per Get buffer

	cacheExpiration = 1000 * time.Minute
)

type Cache struct {
	cache *ristretto.Cache

	once sync.Once
}

var (
	globalCache *Cache

	cacheMutex sync.RWMutex
)

// GetCache returns a singleton instance of the cache
func GetCache() (*Cache, error) {

	cacheMutex.Lock()

	defer cacheMutex.Unlock()

	if globalCache == nil {

		cache := &Cache{}

		if err := cache.init(); err != nil {
			return nil, err
		}

		globalCache = cache

	}

	return globalCache, nil
}

// init initializes the Ristretto cache with configured parameters
func (c *Cache) init() error {

	var initErr error

	c.once.Do(func() {

		config := &ristretto.Config{

			NumCounters: numCounters,

			MaxCost: maxCacheSize,

			BufferItems: bufferItems,
		}

		cache, err := ristretto.NewCache(config)

		if err != nil {

			initErr = fmt.Errorf("failed to initialize cache: %v", err)

			return

		}

		c.cache = cache

	})

	return initErr

}

// generateCacheKey creates a unique key for caching
func generateCacheKey(objectID uint32, counterId uint16, from, to uint32) string {

	return fmt.Sprintf("%d:%d:%d:%d", objectID, counterId, from, to)

}

// Get retrieves data points from cache
func (c *Cache) Get(objectID uint32, counterId uint16, from, to uint32) ([]models.DataPoint, bool) {

	key := generateCacheKey(objectID, counterId, from, to)

	if val, found := c.cache.Get(key); found {

		if dataPoints, ok := val.([]models.DataPoint); ok {
			return dataPoints, true
		}

	}

	return nil, false
}

// Set stores data points in cache
func (c *Cache) Set(objectID uint32, counterId uint16, from, to uint32, data []models.DataPoint) bool {

	key := generateCacheKey(objectID, counterId, from, to)

	return c.cache.SetWithTTL(key, data, int64(len(data)), cacheExpiration)

}

// Clear removes all entries from the cache
func (c *Cache) Clear() {

	c.cache.Clear()
	
}
