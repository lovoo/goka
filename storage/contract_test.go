package storage_test

// The Storage contract, as storage.go documents it, run against every backend.
// Each case states the invariant it proves with temper.Covers, for the backend
// it runs against, so the matrix shows every backend against every guarantee.

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/lovoo/goka/storage"
	gokaredis "github.com/lovoo/goka/storage/redis"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/turbolytics/temper"
	redis "gopkg.in/redis.v5"
)

const (
	getReturnsSet   = "storage.get.returns_set"
	deleteRemoves   = "storage.delete.removes"
	offsetRoundtrip = "storage.offset.roundtrip"
	visitsEveryKey  = "storage.iterator.visits_every_key"
	rangeHalfOpen   = "storage.range.half_open"
	seekForward     = "storage.seek.never_backward"
)

// opener returns a new, empty storage for one case.
type opener func(t *testing.T) storage.Storage

func key(i int) string { return fmt.Sprintf("key%05d", i) }

func fill(t *testing.T, s storage.Storage, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := s.Set(key(i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Set(%s): %v", key(i), err)
		}
	}
	if err := s.SetOffset(42); err != nil {
		t.Fatalf("SetOffset: %v", err)
	}
}

func collect(t *testing.T, it storage.Iterator) []string {
	t.Helper()
	defer it.Release()
	var out []string
	for it.Next() {
		out = append(out, string(it.Key()))
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator: %v", err)
	}
	return out
}

func span(from, to int) []string {
	var out []string
	for i := from; i < to; i++ {
		out = append(out, key(i))
	}
	return out
}

// sameKeys compares as sets: the contract does not promise an order.
func sameKeys(got, want []string) bool {
	seen := map[string]int{}
	for _, k := range got {
		seen[k]++
	}
	if len(got) != len(want) {
		return false
	}
	for _, k := range want {
		if seen[k] != 1 {
			return false
		}
	}
	return true
}

func describe(keys []string) string {
	if len(keys) == 0 {
		return "no keys"
	}
	return fmt.Sprintf("%d keys (%s ... %s)", len(keys), keys[0], keys[len(keys)-1])
}

var cases = []struct {
	invariant string
	run       func(t *testing.T, open opener)
}{
	{getReturnsSet, func(t *testing.T, open opener) {
		s := open(t)
		if err := s.Set("k", []byte("v")); err != nil {
			t.Fatal(err)
		}
		got, err := s.Get("k")
		if err != nil || !bytes.Equal(got, []byte("v")) {
			t.Fatalf("Get(k) = %q, %v; want \"v\"", got, err)
		}
		missing, err := s.Get("missing")
		if err != nil || missing != nil {
			t.Fatalf("Get(missing) = %q, %v; want nil", missing, err)
		}
	}},
	{deleteRemoves, func(t *testing.T, open opener) {
		s := open(t)
		if err := s.Set("k", []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete("k"); err != nil {
			t.Fatal(err)
		}
		if has, err := s.Has("k"); err != nil || has {
			t.Fatalf("Has(k) after Delete = %v, %v; want false", has, err)
		}
		if got, err := s.Get("k"); err != nil || got != nil {
			t.Fatalf("Get(k) after Delete = %q, %v; want nil", got, err)
		}
	}},
	{offsetRoundtrip, func(t *testing.T, open opener) {
		s := open(t)
		if err := s.SetOffset(1234); err != nil {
			t.Fatal(err)
		}
		if got, err := s.GetOffset(-1); err != nil || got != 1234 {
			t.Fatalf("GetOffset = %d, %v; want 1234", got, err)
		}
	}},
	// A small table and a larger one: a defect that appears only past some
	// size passes every test that writes a handful of keys.
	{visitsEveryKey, func(t *testing.T, open opener) {
		for _, n := range []int{10, 500, 5000} {
			s := open(t)
			fill(t, s, n)
			it, err := s.Iterator()
			if err != nil {
				t.Fatal(err)
			}
			if got := collect(t, it); !sameKeys(got, span(0, n)) {
				t.Errorf("with %d keys stored, Iterator yielded %s", n, describe(got))
			}
		}
	}},
	{rangeHalfOpen, func(t *testing.T, open opener) {
		s := open(t)
		fill(t, s, 300)
		it, err := s.IteratorWithRange([]byte(key(100)), []byte(key(200)))
		if err != nil {
			t.Fatal(err)
		}
		if got := collect(t, it); !sameKeys(got, span(100, 200)) {
			t.Fatalf("IteratorWithRange(%s, %s) yielded %s; want the 100 keys %s ... %s",
				key(100), key(200), describe(got), key(100), key(199))
		}
	}},
	{seekForward, func(t *testing.T, open opener) {
		s := open(t)
		fill(t, s, 300)
		it, err := s.Iterator()
		if err != nil {
			t.Fatal(err)
		}
		defer it.Release()
		it.Seek([]byte(key(250)))
		if it.Next() {
			if got := string(it.Key()); got < key(250) {
				t.Fatalf("after Seek(%s), the next key is %s", key(250), got)
			}
		}
	}},
}

// contract runs every case except the invariants the subject is exempt from.
func contract(t *testing.T, subject string, open opener, exempt ...string) {
	skip := map[string]bool{}
	for _, inv := range exempt {
		skip[inv] = true
	}
	for _, c := range cases {
		if skip[c.invariant] {
			continue
		}
		c := c
		t.Run(c.invariant, func(t *testing.T) {
			temper.Covers(t, c.invariant, temper.Subject(subject))
			c.run(t, open)
		})
	}
}

func TestStorageContract_Memory(t *testing.T) {
	temper.Covers(t, "storage.memory")
	contract(t, "storage.memory", func(*testing.T) storage.Storage { return storage.NewMemory() })
}

func TestStorageContract_LevelDB(t *testing.T) {
	temper.Covers(t, "storage.leveldb")
	contract(t, "storage.leveldb", func(t *testing.T) storage.Storage {
		db, err := leveldb.OpenFile(t.TempDir(), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		s, err := storage.New(db)
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

var nullExempt = []string{getReturnsSet, offsetRoundtrip, visitsEveryKey, rangeHalfOpen}

func TestStorageContract_Null(t *testing.T) {
	temper.Covers(t, "storage.null")
	contract(t, "storage.null", func(*testing.T) storage.Storage { return storage.NewNull() }, nullExempt...)
}

// Null discards everything by design, so it is exempt from every invariant
// about reading back what was stored. This proves it does discard.
func TestNullStorage_KeepsNothing(t *testing.T) {
	for _, inv := range nullExempt {
		temper.Exempts(t, inv, temper.Subject("storage.null"))
	}
	s := storage.NewNull()
	if err := s.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Get("k"); got != nil {
		t.Fatalf("Get after Set = %q; Null kept a value", got)
	}
	if err := s.SetOffset(7); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.GetOffset(-1); got != -1 {
		t.Fatalf("GetOffset = %d; Null kept an offset", got)
	}
	for _, open := range []func() (storage.Iterator, error){
		s.Iterator,
		func() (storage.Iterator, error) { return s.IteratorWithRange(nil, nil) },
	} {
		it, err := open()
		if err != nil {
			t.Fatal(err)
		}
		if got := collect(t, it); !reflect.DeepEqual(got, []string(nil)) {
			t.Fatalf("Null yielded %v", got)
		}
	}
}

var redisHashes atomic.Int64

// The Redis backend needs a server, so it runs at the integration level:
// GOKA_REDIS_ADDR=host:port go test -run '^TestIntegration' ./storage/...
func TestIntegrationStorageContract_Redis(t *testing.T) {
	temper.Covers(t, "storage.redis")
	addr := os.Getenv("GOKA_REDIS_ADDR")
	if addr == "" {
		t.Skip("needs a Redis server: set GOKA_REDIS_ADDR=host:port")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { client.Close() })
	contract(t, "storage.redis", func(t *testing.T) storage.Storage {
		hash := fmt.Sprintf("goka-contract-%d-%d", os.Getpid(), redisHashes.Add(1))
		t.Cleanup(func() { client.Del(hash) })
		s, err := gokaredis.New(client, hash)
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}
