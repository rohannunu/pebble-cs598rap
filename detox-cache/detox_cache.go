package detoxcache

import (
	"math"
	"sync/atomic"

	"github.com/rohannunu/pebble-cs598rap/cache"
)

var globalAccess uint64

// -------------------- Stats --------------------

type DeToXStats struct {
	Hits        uint64
	Misses      uint64
	Admissions  uint64
	Evictions   uint64
	Prefetches  uint64
	Transactions uint64
}

func (s *DeToXStats) RecordHit()        { atomic.AddUint64(&s.Hits, 1) }
func (s *DeToXStats) RecordMiss()       { atomic.AddUint64(&s.Misses, 1) }
func (s *DeToXStats) RecordAdmission()  { atomic.AddUint64(&s.Admissions, 1) }
func (s *DeToXStats) RecordEviction()   { atomic.AddUint64(&s.Evictions, 1) }
func (s *DeToXStats) RecordPrefetch()   { atomic.AddUint64(&s.Prefetches, 1) }
func (s *DeToXStats) RecordTxn()        { atomic.AddUint64(&s.Transactions, 1) }

func (s *DeToXStats) Snapshot() DeToXStats {
	return DeToXStats{
		Hits:         atomic.LoadUint64(&s.Hits),
		Misses:       atomic.LoadUint64(&s.Misses),
		Admissions:   atomic.LoadUint64(&s.Admissions),
		Evictions:    atomic.LoadUint64(&s.Evictions),
		Prefetches:   atomic.LoadUint64(&s.Prefetches),
		Transactions: atomic.LoadUint64(&s.Transactions),
	}
}

// -------------------- Existing structs --------------------

type Level struct {
	Keys      []string
	Timestamp uint64
}

type Transaction struct {
	Levels    []Level
	Timestamp uint64
}

type KeyMetadata struct {
	TotalScore   float64
	Frequency    uint64
	Size         int
	LastAccess   uint64
	Transactions []string
}

type GroupScore struct {
	Keys      []string
	Score     float64
	Frequency uint64
	LengthRed int
}

type DependencySet struct {
	Keys      []string
	Frequency uint64
}

type DeToXCache struct {
	cache       *cache.Cache
	capacity    int
	metadata    map[string]*KeyMetadata
	keys        []string
	agingFactor float64
	depSets     map[string][]*DependencySet
	txnHistory  []*Transaction
	closed      bool
	stats *DeToXStats
}

func NewDeToXCache(capacity int) *DeToXCache {
	return &DeToXCache{
		cache:      cache.CreateCacheAndPebble(capacity),
		capacity:   capacity,
		metadata:   make(map[string]*KeyMetadata),
		keys:       make([]string, 0, capacity),
		depSets:    make(map[string][]*DependencySet),
		txnHistory: make([]*Transaction, 0),
		stats:      &DeToXStats{},
	}
}

// Expose stats to wrappers (e.g., YCSB creator).
func (dc *DeToXCache) Stats() DeToXStats {
	return dc.stats.Snapshot()
}

func (dc *DeToXCache) scoreGroup(keys []string, lengthReduction int) float64 {
	if len(keys) == 0 {
		return 0
	}

	minFreq := uint64(math.MaxUint64)
	totalSize := 0

	for _, k := range keys {
		if meta, ok := dc.metadata[k]; ok {
			if meta.Frequency < minFreq {
				minFreq = meta.Frequency
			}
			totalSize += meta.Size
		} else {
			minFreq = 1
			totalSize += 1
		}
	}

	if totalSize == 0 {
		totalSize = 1
	}

	return float64(minFreq*uint64(lengthReduction)) / float64(totalSize)
}

func (dc *DeToXCache) updateKeyScore(key string, instanceScore float64) {
	meta, ok := dc.metadata[key]
	if !ok {
		meta = &KeyMetadata{
			TotalScore:   0,
			Frequency:    0,
			Size:         1,
			LastAccess:   atomic.AddUint64(&globalAccess, 1),
			Transactions: make([]string, 0),
		}
		dc.metadata[key] = meta
	}

	meta.Frequency++
	meta.TotalScore += instanceScore
	meta.LastAccess = atomic.AddUint64(&globalAccess, 1)
}

func (dc *DeToXCache) getKeyScore(key string) float64 {
	meta, ok := dc.metadata[key]
	if !ok {
		return dc.agingFactor
	}

	if meta.Frequency == 0 {
		return dc.agingFactor
	}

	avgScore := meta.TotalScore / float64(meta.Frequency)
	return avgScore + dc.agingFactor
}

func (dc *DeToXCache) scoreTransaction(levels []Level) {
	if len(levels) == 0 {
		return
	}

	criticalLength := len(levels)
	groupScores := make([]*GroupScore, 0)

	for i := 0; i < len(levels); i++ {
		keys := levels[i].Keys
		if len(keys) == 0 {
			continue
		}

		lengthReduction := 1
		if i == 0 {
			lengthReduction = 1
		}

		score := dc.scoreGroup(keys, lengthReduction)
		groupScores = append(groupScores, &GroupScore{
			Keys:      keys,
			Score:     score,
			Frequency: 0,
			LengthRed: lengthReduction,
		})
	}

	for _, group := range groupScores {
		for _, key := range group.Keys {
			dc.updateKeyScore(key, group.Score)
		}
	}

	_ = criticalLength
}

func (dc *DeToXCache) Get(key []byte) ([]byte, bool, error) {
	k := string(key)

	val, found, err := dc.cache.Get(key)
	if err != nil {
		return nil, false, err
	}

	if found {
		// Logical hit in DeToX if we already track metadata
		if meta, ok := dc.metadata[k]; ok {
			dc.stats.RecordHit()
			meta.Frequency++
			meta.LastAccess = atomic.AddUint64(&globalAccess, 1)
		} else {
			// Value exists but DeToX has never seen this key => miss + admission candidate
			dc.stats.RecordMiss()
			dc.metadata[k] = &KeyMetadata{
				TotalScore:   0,
				Frequency:    1,
				Size:         len(val),
				LastAccess:   atomic.AddUint64(&globalAccess, 1),
				Transactions: make([]string, 0),
			}
			dc.keys = append(dc.keys, k)
			dc.stats.RecordAdmission()
		}
		if !dc.closed {
			dc.Prefetch(key)
		}
	} else {
		// not found at all
		dc.stats.RecordMiss()
	}

	return val, found, nil
}

func (dc *DeToXCache) Set(key, value []byte, toCache bool) (bool, error) {
	if !toCache {
		// direct write-through to Pebble
		return dc.cache.Set(key, value, false)
	}

	k := string(key)

	if meta, ok := dc.metadata[k]; ok {
		// existing DeToX-tracked key: treat as a hit/update
		dc.stats.RecordHit()
		meta.Frequency++
		meta.LastAccess = atomic.AddUint64(&globalAccess, 1)
		meta.Size = len(value)
		return dc.cache.Set(key, value, true)
	}

	// new key: we may need to evict someone
	if len(dc.metadata) >= dc.capacity {
		if err := dc.evictVictim(); err != nil {
			return false, err
		}
	}

	cached, err := dc.cache.Set(key, value, true)
	if err != nil || !cached {
		return cached, err
	}

	meta := &KeyMetadata{
		TotalScore:   0,
		Frequency:    1,
		Size:         len(value),
		LastAccess:   atomic.AddUint64(&globalAccess, 1),
		Transactions: make([]string, 0),
	}
	dc.metadata[k] = meta
	dc.keys = append(dc.keys, k)
	dc.stats.RecordAdmission()

	return true, nil
}

func (dc *DeToXCache) evictVictim() error {
	if len(dc.keys) == 0 {
		return nil
	}

	victimIdx := -1
	lowestScore := math.MaxFloat64

	for i, k := range dc.keys {
		score := dc.getKeyScore(k)
		if score < lowestScore {
			lowestScore = score
			victimIdx = i
		}
	}

	if victimIdx == -1 {
		return nil
	}

	victimKey := dc.keys[victimIdx]

	_, err := dc.cache.Evict([]byte(victimKey))
	if err != nil {
		return err
	}

	// eviction tracked here
	dc.stats.RecordEviction()

	dc.agingFactor = lowestScore

	last := len(dc.keys) - 1
	dc.keys[victimIdx] = dc.keys[last]
	dc.keys = dc.keys[:last]
	delete(dc.metadata, victimKey)

	return nil
}

func (dc *DeToXCache) ExecuteTransaction(levels [][][]byte) (map[string][]byte, error) {
	now := atomic.AddUint64(&globalAccess, 1)
	results := make(map[string][]byte)
	txnLevels := make([]Level, 0)

	for _, levelKeys := range levels {
		level := Level{
			Keys:      make([]string, 0),
			Timestamp: now,
		}

		for _, key := range levelKeys {
			k := string(key)
			level.Keys = append(level.Keys, k)

			val, found, err := dc.Get(key)
			if err != nil {
				return nil, err
			}

			if found {
				results[k] = val
			}
		}

		if len(level.Keys) > 0 {
			txnLevels = append(txnLevels, level)
		}
	}

	dc.scoreTransaction(txnLevels)

	txn := &Transaction{
		Levels:    txnLevels,
		Timestamp: now,
	}
	dc.txnHistory = append(dc.txnHistory, txn)

	// track a transaction-level counter
	dc.stats.RecordTxn()

	return results, nil
}

func (dc *DeToXCache) recordDependency(sourceKey string, dependentKeys []string) {
	if _, ok := dc.depSets[sourceKey]; !ok {
		dc.depSets[sourceKey] = make([]*DependencySet, 0)
	}

	found := false
	for _, depSet := range dc.depSets[sourceKey] {
		if len(depSet.Keys) == len(dependentKeys) {
			match := true
			for i, k := range depSet.Keys {
				if k != dependentKeys[i] {
					match = false
					break
				}
			}
			if match {
				depSet.Frequency++
				found = true
				break
			}
		}
	}

	if !found {
		dc.depSets[sourceKey] = append(dc.depSets[sourceKey], &DependencySet{
			Keys:      dependentKeys,
			Frequency: 1,
		})
	}
}

func (dc *DeToXCache) Prefetch(sourceKey []byte) {
	// record one prefetch request (not per key) for now
	dc.stats.RecordPrefetch()
	go dc.prefetchAsync(sourceKey)
}

func (dc *DeToXCache) prefetchAsync(sourceKey []byte) {
	if dc.closed {
		return
	}

	k := string(sourceKey)
	depSets, ok := dc.depSets[k]
	if !ok || len(depSets) == 0 {
		return
	}

	var mostFrequent *DependencySet
	maxFreq := uint64(0)
	for _, depSet := range depSets {
		if depSet.Frequency > maxFreq {
			maxFreq = depSet.Frequency
			mostFrequent = depSet
		}
	}

	if mostFrequent == nil {
		return
	}

	prefetchKeys := make([][]byte, 0)
	for _, key := range mostFrequent.Keys {
		prefetchKeys = append(prefetchKeys, []byte(key))
	}

	if !dc.closed {
		dc.cache.Prefetch(prefetchKeys)
	}
}

func (dc *DeToXCache) Close() error {
	dc.closed = true
	return dc.cache.Close()
}
