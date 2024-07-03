package storagetest

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/lovoo/goka/storage"
)

// TeardownFn cleans up state after tests.
type TeardownFn func() error

// StorageBuilderFn creates a storage and the teardown function.
type StorageBuilderFn func(t *testing.T) (storage.Storage, TeardownFn)

type StorageTestSuite struct {
	suite.Suite

	storageBuilderFn StorageBuilderFn
	teardownFn       TeardownFn
	storage          storage.Storage
}

func NewStorageTestSuite(storageBuilderFn StorageBuilderFn) *StorageTestSuite {
	return &StorageTestSuite{
		storageBuilderFn: storageBuilderFn,
	}
}

func (s *StorageTestSuite) SetupTest() {
	storage, teardownFn := s.storageBuilderFn(s.T())
	s.storage = storage
	s.teardownFn = teardownFn
}

func (s *StorageTestSuite) TeardownTest() {
	err := s.teardownFn()
	require.NoError(s.T(), err)
	s.teardownFn = nil
}

func (s *StorageTestSuite) TestHas() {
	st := s.storage

	var (
		err    error
		hasKey bool
	)

	hasKey, err = st.Has("test-key")
	require.NoError(s.T(), err)
	require.False(s.T(), hasKey)

	err = st.Set("test-key", []byte("test"))
	require.NoError(s.T(), err)

	hasKey, err = st.Has("test-key")
	require.NoError(s.T(), err)
	require.True(s.T(), hasKey)
}

func (s *StorageTestSuite) TestSetGet() {
	st := s.storage

	value, err := st.Get("example1")
	require.NoError(s.T(), err)
	require.Nil(s.T(), value)

	err = st.Set("example1", []byte("example-message"))
	require.NoError(s.T(), err)

	value, err = st.Get("example1")
	require.NoError(s.T(), err)
	require.Equal(s.T(), []byte("example-message"), value)
}

func (s *StorageTestSuite) TestDelete() {
	st := s.storage

	err := st.Set("example1", []byte("example-message"))
	require.NoError(s.T(), err)

	require.NoError(s.T(), st.Delete("example1"))

	hasKey, err := st.Has("example1")
	require.NoError(s.T(), err)
	require.False(s.T(), hasKey)

	value, err := st.Get("example1")
	require.NoError(s.T(), err)
	require.Nil(s.T(), value)
}

func (s *StorageTestSuite) TestOffsetSetGet() {
	st := s.storage

	offset, err := st.GetOffset(0)
	require.NoError(s.T(), err)
	require.Equal(s.T(), int64(0), offset)

	require.NoError(s.T(), st.SetOffset(100))

	offset, err = st.GetOffset(0)
	require.NoError(s.T(), err)
	require.Equal(s.T(), int64(100), offset)
}

func (s *StorageTestSuite) TestIterator() {
	st := s.storage

	kv := map[string]string{
		"key-1": "val-1",
		"key-2": "val-2",
		"key-3": "val-3",
	}

	for k, v := range kv {
		require.Nil(s.T(), st.Set(k, []byte(v)))
	}

	require.Nil(s.T(), st.SetOffset(777))

	iter, err := st.Iterator()
	require.Nil(s.T(), err)
	defer iter.Release()
	count := 0

	// accessing iterator before Next should only return nils
	val, err := iter.Value()
	require.True(s.T(), val == nil)
	require.Nil(s.T(), err)

	for iter.Next() {
		count++
		key := string(iter.Key())
		expected, ok := kv[key]
		if !ok {
			s.T().Fatalf("unexpected key from iterator: %s", key)
		}

		val, err := iter.Value()
		require.Nil(s.T(), err)
		require.Equal(s.T(), expected, string(val))
	}
	require.Equal(s.T(), count, len(kv))
}
