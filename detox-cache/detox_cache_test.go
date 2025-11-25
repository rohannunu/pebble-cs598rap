package detoxcache

import (
	"testing"
)

func TestDeToXCache_BasicSetGet(t *testing.T) {
	dc := NewDeToXCache(10)
	defer dc.Close()

	key := []byte("key1")
	value := []byte("value1")

	_, err := dc.Set(key, value, true)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	got, found, err := dc.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatalf("Key not found")
	}
	if string(got) != string(value) {
		t.Fatalf("Got %q, want %q", got, value)
	}
}

func TestDeToXCache_TransactionExecution(t *testing.T) {
	dc := NewDeToXCache(20)
	defer dc.Close()

	keys := [][]byte{
		[]byte("account"),
		[]byte("savings"),
		[]byte("checking"),
	}
	values := [][]byte{
		[]byte("acc_data"),
		[]byte("sav_data"),
		[]byte("chk_data"),
	}

	for i := 0; i < len(keys); i++ {
		_, err := dc.Set(keys[i], values[i], true)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	levels := [][][]byte{
		{keys[0]},
		{keys[1], keys[2]},
	}

	results, err := dc.ExecuteTransaction(levels)
	if err != nil {
		t.Fatalf("ExecuteTransaction failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}
}

func TestDeToXCache_Eviction(t *testing.T) {
	dc := NewDeToXCache(3)
	defer dc.Close()

	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
	}
	values := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
		[]byte("value4"),
	}

	for i := 0; i < 3; i++ {
		_, err := dc.Set(keys[i], values[i], true)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	_, err := dc.Set(keys[3], values[3], true)
	if err != nil {
		t.Fatalf("Set with eviction failed: %v", err)
	}

	if len(dc.metadata) > dc.capacity {
		t.Fatalf("Cache exceeded capacity: %d > %d", len(dc.metadata), dc.capacity)
	}
}

func TestDeToXCache_GroupScoring(t *testing.T) {
	dc := NewDeToXCache(100)
	defer dc.Close()

	hotKey := []byte("hot")
	coldKey := []byte("cold")

	for i := 0; i < 100; i++ {
		dc.Set(hotKey, []byte("hot_value"), true)
	}

	for i := 0; i < 5; i++ {
		dc.Set(coldKey, []byte("cold_value"), true)
	}

	levels := [][][]byte{
		{hotKey, coldKey},
	}

	_, err := dc.ExecuteTransaction(levels)
	if err != nil {
		t.Fatalf("ExecuteTransaction failed: %v", err)
	}

	hotMeta := dc.metadata[string(hotKey)]
	coldMeta := dc.metadata[string(coldKey)]

	if hotMeta == nil || coldMeta == nil {
		t.Fatalf("Metadata not found for keys")
	}

	if hotMeta.Frequency < 90 {
		t.Fatalf("Hot key frequency too low: %d (expected >= 90)", hotMeta.Frequency)
	}

	if coldMeta.Frequency < 5 {
		t.Fatalf("Cold key frequency too low: %d (expected >= 5)", coldMeta.Frequency)
	}
}

func TestDeToXCache_Prefetching(t *testing.T) {
	dc := NewDeToXCache(20)
	defer dc.Close()

	primaryKey := []byte("primary")
	dep1 := []byte("dep1")
	dep2 := []byte("dep2")

	dc.Set(primaryKey, []byte("primary_value"), true)
	dc.Set(dep1, []byte("dep1_value"), true)
	dc.Set(dep2, []byte("dep2_value"), true)

	dc.recordDependency(string(primaryKey), []string{string(dep1), string(dep2)})
}

func TestDeToXCache_ContaminationScenario(t *testing.T) {
	dc := NewDeToXCache(5)
	defer dc.Close()

	hotKeys := [][]byte{
		[]byte("hot1"),
		[]byte("hot2"),
		[]byte("hot3"),
	}
	coldKeys := [][]byte{
		[]byte("cold1"),
		[]byte("cold2"),
	}

	for i := 0; i < 50; i++ {
		for _, k := range hotKeys {
			dc.Set(k, []byte("hot_val"), true)
		}
	}

	for i := 0; i < 2; i++ {
		for _, k := range coldKeys {
			dc.Set(k, []byte("cold_val"), true)
		}
	}

	levels := [][][]byte{
		{hotKeys[0], coldKeys[0]},
	}

	_, err := dc.ExecuteTransaction(levels)
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	hot1Score := dc.getKeyScore(string(hotKeys[0]))
	hot2Score := dc.getKeyScore(string(hotKeys[1]))

	if hot2Score <= hot1Score {
		t.Logf("Contamination detected: hot1 (with cold) score=%f, hot2 (alone) score=%f",
			hot1Score, hot2Score)
	}
}

func TestDeToXCache_MultiLevelTransaction(t *testing.T) {
	dc := NewDeToXCache(15)
	defer dc.Close()

	level0 := [][]byte{[]byte("account")}
	level1 := [][]byte{[]byte("savings"), []byte("checking")}
	level2 := [][]byte{[]byte("transaction_history")}

	for _, k := range level0 {
		dc.Set(k, []byte("data"), true)
	}
	for _, k := range level1 {
		dc.Set(k, []byte("data"), true)
	}
	for _, k := range level2 {
		dc.Set(k, []byte("data"), true)
	}

	levels := [][][]byte{level0, level1, level2}

	results, err := dc.ExecuteTransaction(levels)
	if err != nil {
		t.Fatalf("Multi-level transaction failed: %v", err)
	}

	expectedKeys := 4
	if len(results) != expectedKeys {
		t.Fatalf("Expected %d keys in results, got %d", expectedKeys, len(results))
	}
}

