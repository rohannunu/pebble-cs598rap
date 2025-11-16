package lrucache

import (
	"container/list"

	"github.com/rohannunu/pebble-cs598rap/cache"
)

type LRUCache struct {
	cache    *cache.Cache
	order    *list.List
	elements map[string]*list.Element
	capacity int
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		cache:    cache.CreateCacheAndPebble(capacity),
		order:    list.New(),
		elements: make(map[string]*list.Element),
		capacity: capacity,
	}
}

func (lru *LRUCache) Exists(key []byte) (bool, error) {
	return lru.elements[string(key)] != nil, nil
}

func (lru *LRUCache) updateOrder(key string) {
	// Move the accessed element to the front of the order list
	// it is now most recently used
	if elem, ok := lru.elements[key]; ok {
		lru.order.MoveToFront(elem)
	}
}

func (lru *LRUCache) Get(key []byte) ([]byte, bool, error) {
	// Check if the key exists in the cache
	exists, err := lru.Exists(key)
	if err != nil {
		return nil, false, err
	}
	if exists {
		value, found, err := lru.cache.Get(key)
		if err != nil {
			return nil, false, err
		}
		if found {
			lru.updateOrder(string(key))
			return value, true, nil
		}
	} else {
		// if its not, try to get it from pebble directly and promote it to the cache
		value, found, err := lru.cache.Get(key)
		if err != nil {
			return nil, false, err
		}
		if found {
			// promote to cache
			_, err := lru.Set(key, value, true)
			if err != nil {
				return nil, false, err
			}
			return value, true, nil
		}
	}
	return nil, false, nil
}

func (lru *LRUCache) Set(key []byte, value []byte, to_cache bool) (bool, error) {
	// If the key already exists, update the value and move it to the front
	if elem, ok := lru.elements[string(key)]; ok {
		lru.cache.Set(key, value, true)
		lru.order.MoveToFront(elem)
		return true, nil
	}

	// If the cache is at capacity, remove the least recently used item
	if lru.order.Len() >= lru.capacity && to_cache {
		backElem := lru.order.Back()
		if backElem != nil {
			_, err := lru.cache.Evict([]byte(backElem.Value.(string)))
			if err != nil {
				return false, err
			}
			delete(lru.elements, backElem.Value.(string))
			lru.order.Remove(backElem)
		}
	}
	// Add the new key-value pair to the cache and mark it as most recently used
	cached, err := lru.cache.Set(key, value, to_cache)
	if err != nil {
		return false, err
	}

	if !cached {
		return false, nil
	}

	elem := lru.order.PushFront(string(key))
	lru.elements[string(key)] = elem
	return true, nil
}
