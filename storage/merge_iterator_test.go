package storage

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"
)

// TeardownFunc cleans up state after tests.
type TeardownFunc func(*testing.T)

// TempDir creates a new temporary directory and returns an associatd clean up
// function that should be called after the test.
func TempDir(t *testing.T) (string, TeardownFunc) {
	t.Helper()

	path, err := os.MkdirTemp(os.TempDir(), "goka_test_")
	if err != nil {
		t.Fatalf("error creating temporary directory: %v", err)
	}

	return path, func(t *testing.T) {
		t.Helper()

		if err := os.RemoveAll(path); err != nil {
			t.Errorf("error removing temporary directory: %v", err)
		}
	}

}

// NewLevelDB returns a ready to use LevelDB storage instance and an
// associated teardown function that should be called after the test to close the
// database and clean up the temporary files.
func NewLevelDB(t *testing.T) (Storage, TeardownFunc) {
	t.Helper()

	path, cleanTemp := TempDir(t)

	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		cleanTemp(t)
		t.Fatalf("error opening leveldb: %v", err)
	}

	cleanUp := func(t *testing.T) {
		t.Helper()

		if err := db.Close(); err != nil {
			t.Errorf("error closing leveldb: %v", err)
		}

		cleanTemp(t)
	}

	st, err := New(db)
	if err != nil {
		cleanUp(t)
		t.Fatalf("error creating storage: %v", err)
	}

	if err := st.MarkRecovered(); err != nil {
		cleanUp(t)
		t.Fatalf("error marking storage recovered: %v", err)
	}

	return st, cleanUp
}

// SetState sets the storages state to match the given offset and key-values.
func SetState(t *testing.T, st Storage, offset int64, state map[string]string) {
	t.Helper()

	iter, err := st.Iterator()
	if err != nil {
		t.Fatalf("error iterating: %v", err)
	}
	defer iter.Release()

	for iter.Next() {
		if err := st.Delete(string(iter.Key())); err != nil {
			t.Fatalf("error deleting previous state: %v", err)
		}
	}

	for key, value := range state {
		if err := st.Set(key, []byte(value)); err != nil {
			t.Fatalf("error setting value: %v", err)
		}
	}

	if err := st.SetOffset(offset); err != nil {
		t.Fatalf("error setting offset: %v", err)
	}
}

func TestLevelDBMergeIterator(t *testing.T) {
	type State map[string]string
	type Partitions []State

	parts := Partitions{
		State{
			"key-0": "val-0",
			"key-3": "val-3",
		},
		State{
			"key-1": "val-1",
			"key-4": "val-4",
			"key-6": "val-6",
			"key-7": "val-7",
			"key-8": "val-8",
		},
		State{},
		State{
			"key-2": "val-2",
			"key-5": "val-5",
			"key-9": "val-9",
		},
	}

	iters := []Iterator{}
	for _, state := range parts {
		st, cleanUp := NewLevelDB(t)
		defer cleanUp(t)

		SetState(t, st, int64(len(state)), state)

		iter, err := st.Iterator()
		if err != nil {
			t.Fatalf("error creating iterator: %v", err)
		}
		defer iter.Release()

		iters = append(iters, iter)
	}

	MergeIteratorSuite(t, NewMultiIterator(iters))
}

func MergeIteratorSuite(t *testing.T, iter Iterator) {
	t.Run("Ordered Iteration", func(t *testing.T) {
		testOrderedIteration(t, iter)
	})

	t.Run("Seek Exhausted", func(t *testing.T) {
		if !iter.Seek([]byte("key-0")) {
			t.Fatalf("expected seek to succeed")
		}

		testOrderedIteration(t, iter)
	})
}

func TestEmptyMergeIterator(t *testing.T) {
	iter := NewMultiIterator(nil)
	if iter.Next() {
		t.Fatalf("expected an empty merge iterator to return false on next")
	}

	if err := iter.Err(); err != nil {
		t.Fatalf("expected an empty merge iterator to return no error but got %q", err)
	}

	if iter.Seek(nil) {
		t.Fatalf("expected an empty merge iterator failing to seek")
	}
}

func testOrderedIteration(t *testing.T, iter Iterator) {
	counter := 0
	for ; iter.Next(); counter++ {
		key := iter.Key()
		expected := fmt.Sprintf("key-%v", counter)
		if bytes.Compare(key, []byte(expected)) != 0 {
			t.Errorf("received wrong key %s, expected: %s", string(key), expected)
		}

		expectedVal := fmt.Sprintf("val-%v", counter)
		if val, err := iter.Value(); err != nil {
			t.Errorf("error getting value for key %q: %q", string(key), err)
		} else if bytes.Compare(val, []byte(expectedVal)) != 0 {
			t.Errorf("received wrong value for key %s, expected: %q, actual: %q", string(key), expectedVal, string(val))
		}
	}

	if iter.Next() != false {
		t.Fatalf("expected iterator to be exhausted but it was not")
	} else if err := iter.Err(); err != nil {
		t.Fatalf("error iterating: %v", err)
	} else if counter < 10 {
		t.Fatalf("expected to have iterated 10 pairs, actually got %v", counter)
	}
}
