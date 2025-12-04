package lfucache

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/rohannunu/pebble-cs598rap/cache"
)

type LFUStats struct {
	Hits       uint64
	Misses     uint64
	Promotions uint64
	Evictions  uint64
}

type lfuEntry struct {
	key  string
	freq int
	elem *list.Element
}

type LFUCache struct {
	cache     *cache.Cache
	capacity  int
	elements  map[string]*lfuEntry
	freqLists map[int]*list.List
	minFreq   int
	stats     *LFUStats
	mu        sync.RWMutex
}

func (s *LFUStats) RecordHit()     { atomic.AddUint64(&s.Hits, 1) }
func (s *LFUStats) RecordMiss()    { atomic.AddUint64(&s.Misses, 1) }
func (s *LFUStats) RecordPromote() { atomic.AddUint64(&s.Promotions, 1) }
func (s *LFUStats) RecordEvict()   { atomic.AddUint64(&s.Evictions, 1) }
func (s *LFUStats) Snapshot() LFUStats {
	return LFUStats{
		Hits:       atomic.LoadUint64(&s.Hits),
		Misses:     atomic.LoadUint64(&s.Misses),
		Promotions: atomic.LoadUint64(&s.Promotions),
		Evictions:  atomic.LoadUint64(&s.Evictions),
	}
}

func NewLFUCache(capacity int) *LFUCache {
	return &LFUCache{
		cache:     cache.CreateCacheAndPebble(capacity),
		capacity:  capacity,
		elements:  make(map[string]*lfuEntry),
		freqLists: make(map[int]*list.List),
		stats:     &LFUStats{},
	}
}

func (lfu *LFUCache) Stats() LFUStats {
	return lfu.stats.Snapshot()
}

func (lfu *LFUCache) Exists(key []byte) (bool, error) {
	lfu.mu.RLock()
	_, ok := lfu.elements[string(key)]
	lfu.mu.RUnlock()
	return ok, nil
}

func (lfu *LFUCache) getOrCreateList(freq int) *list.List {
	lst, ok := lfu.freqLists[freq]
	if !ok {
		lst = list.New()
		lfu.freqLists[freq] = lst
	}
	return lst
}

func (lfu *LFUCache) incrementFrequency(entry *lfuEntry) {
	lst := lfu.freqLists[entry.freq]
	if lst != nil && entry.elem != nil {
		lst.Remove(entry.elem)
		entry.elem = nil
		if lst.Len() == 0 {
			delete(lfu.freqLists, entry.freq)
			if lfu.minFreq == entry.freq {
				lfu.minFreq++
			}
		}
	}

	entry.freq++
	newList := lfu.getOrCreateList(entry.freq)
	entry.elem = newList.PushFront(entry)
}

// promoteEntryIfCurrent increments the frequency of the provided entry if it is
// still the current instance for the key. This helper expects that no locks are
// held and will take the write lock as needed.
func (lfu *LFUCache) promoteEntryIfCurrent(key string, snapshot *lfuEntry) {
	if snapshot == nil {
		return
	}

	lfu.mu.Lock()
	defer lfu.mu.Unlock()

	current, ok := lfu.elements[key]
	if !ok || current != snapshot {
		return
	}
	lfu.incrementFrequency(current)
}

func (lfu *LFUCache) recalculateMinFreq() {
	min := 0
	for freq, lst := range lfu.freqLists {
		if lst == nil || lst.Len() == 0 {
			continue
		}
		if min == 0 || freq < min {
			min = freq
		}
	}
	lfu.minFreq = min
}

// trackStats toggles whether this eviction should be reflected in the user visible stats.
func (lfu *LFUCache) evictLFU(trackStats bool, async bool) error {
	// lfu.mu must be held by the caller.
	if len(lfu.elements) == 0 || lfu.capacity == 0 {
		return nil
	}

	lst := lfu.freqLists[lfu.minFreq]
	if lst == nil || lst.Len() == 0 {
		lfu.recalculateMinFreq()
		lst = lfu.freqLists[lfu.minFreq]
		if lst == nil || lst.Len() == 0 {
			return nil
		}
	}

	backElem := lst.Back()
	if backElem == nil {
		return nil
	}
	entry := backElem.Value.(*lfuEntry)
	lst.Remove(backElem)
	delete(lfu.elements, entry.key)
	if lst.Len() == 0 {
		delete(lfu.freqLists, entry.freq)
		lfu.recalculateMinFreq()
	}

	if trackStats {
		lfu.stats.RecordEvict()
	}
	_, err := lfu.cache.Evict([]byte(entry.key), async)
	return err
}

func (lfu *LFUCache) Get(key []byte, async bool) ([]byte, bool, error) {
	keyStr := string(key)

	lfu.mu.RLock()
	entry, ok := lfu.elements[keyStr]
	if ok {
		lfu.stats.RecordHit()
		value, found, err := lfu.cache.Get(key)
		lfu.mu.RUnlock()

		if err != nil {
			return nil, false, err
		}
		if found {
			lfu.promoteEntryIfCurrent(keyStr, entry)
			return value, true, nil
		}
		return nil, false, nil
	}
	lfu.mu.RUnlock()

	lfu.stats.RecordMiss()
	value, found, err := lfu.cache.Get(key)
	if err != nil {
		return nil, false, err
	}
	if found {
		lfu.stats.RecordPromote()
		lfu.mu.Lock()
		_, err := lfu.setInternal(key, value, true, false, async)
		lfu.mu.Unlock()
		if err != nil {
			return nil, false, err
		}
		return value, true, nil
	}

	return nil, false, nil
}

func (lfu *LFUCache) Set(key, value []byte, toCache bool, async bool) (bool, error) {
	lfu.mu.Lock()
	defer lfu.mu.Unlock()

	return lfu.setInternal(key, value, toCache, true, async)
}

func (lfu *LFUCache) setInternal(key, value []byte, toCache bool, trackEviction bool, async bool) (bool, error) {
	keyStr := string(key)
	if entry, ok := lfu.elements[keyStr]; ok {
		_, err := lfu.cache.Set(key, value, true, async)
		if err != nil {
			return false, err
		}
		lfu.incrementFrequency(entry)
		return true, nil
	}

	if !toCache || lfu.capacity == 0 {
		return lfu.cache.Set(key, value, false, async)
	}

	if len(lfu.elements) >= lfu.capacity {
		if err := lfu.evictLFU(trackEviction, async); err != nil {
			return false, err
		}
	}

	cached, err := lfu.cache.Set(key, value, toCache, async)
	if err != nil {
		return false, err
	}
	if !cached {
		return false, nil
	}

	entry := &lfuEntry{
		key:  keyStr,
		freq: 1,
	}

	list := lfu.getOrCreateList(1)
	entry.elem = list.PushFront(entry)
	lfu.elements[keyStr] = entry
	lfu.minFreq = 1
	return true, nil
}
