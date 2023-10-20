package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type StorageTestSuite struct {
	suite.Suite
	storage Storage
}

func NewStorageTestSuite(storage Storage) *StorageTestSuite {
	return &StorageTestSuite{
		storage: storage,
	}
}

func (s *StorageTestSuite) TeardownSuite() {
	s.storage.Close()
}

func (s *StorageTestSuite) TestGetHas() {
	storage := NewMemory()

	var (
		err    error
		hasKey bool
	)

	hasKey, err = storage.Has("test-key")
	require.NoError(s.T(), err)
	require.False(s.T(), hasKey)

	value, err := storage.Get("test-key")
	require.True(s.T(), value == nil)
	require.NoError(s.T(), err)

	err = storage.Set("test-key", []byte("test"))
	require.NoError(s.T(), err)

	hasKey, err = storage.Has("test-key")
	require.NoError(s.T(), err)
	require.True(s.T(), hasKey)

	value, err = storage.Get("test-key")
	require.NoError(s.T(), err)
	require.Equal(s.T(), []byte("test"), value)

	hasKey, err = storage.Has("nil-value")
	require.NoError(s.T(), err)
	require.False(s.T(), hasKey)

	err = storage.Set("nil-value", nil)
	require.Error(s.T(), err)
}

func TestSuiteMemory(t *testing.T) {
	suite.Run(t, NewStorageTestSuite(NewMemory()))
}

func TestSuiteLevelDB(t *testing.T) {
	suite.Run(t, NewStorageTestSuite(NewLevelDB()))
}
