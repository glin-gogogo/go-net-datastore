package netds

import (
	"context"
	"github.com/glin-gogogo/go-net-datastore/utils"
	ds "github.com/ipfs/go-datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

// MockBackendDataStore is a mock type for the BackendDataStore
type MockBackendDataStore struct {
	mock.Mock
}

// IsObjectExist is a mock method that simulates checking object existence
func (m *MockBackendDataStore) IsObjectExist(ctx context.Context, path string, options bool) (bool, error) {
	args := m.Called(ctx, path, options)
	return args.Bool(0), args.Error(1)
}

// CreateFolder is a mock method that simulates creating a folder
func (m *MockBackendDataStore) CreateFolder(ctx context.Context, path string, options bool) error {
	args := m.Called(ctx, path, options)
	return args.Error(0)
}

// ReadShardFunc is a mock method that simulates reading the shard function
func (m *MockBackendDataStore) ReadShardFunc(ctx context.Context, path string) (*ShardIdV1, error) {
	args := m.Called(ctx, path)
	return args.Get(0).(*ShardIdV1), args.Error(1)
}

// WriteShardFunc is a mock method that simulates writing the shard function
func (m *MockBackendDataStore) WriteShardFunc(ctx context.Context, path string, fun *ShardIdV1) error {
	args := m.Called(ctx, path, fun)
	return args.Error(0)
}

// Define other necessary mock methods for the BackendDataStore

// TestCreateOrOpen tests the CreateOrOpen function
func TestCreateOrOpen(t *testing.T) {
	ctx := context.Background()
	path := "cads/blocks"
	//fun := &ShardIdV1{}
	fun, _ := ParseShardFunc("/repo/netds/shard/v1/next-to-last/2")
	cfg := utils.DataStoreConfig{
		Name:          "obs",
		Region:        "cn-south-222",
		Endpoint:      "obs.cn-south-222.ai.pcl.cn",
		AccessKey:     "8B9WZNOZTRXM9CFEZZJV",
		SecretKey:     "jbR2ysrJa2jb0fzagiYRY0oo1zNXJvG3XcQHM3EM",
		Bucket:        "urchin-data",
		RootDirectory: "cads/blocks",
		Workers:       10,
	}

	t.Run("TestCreateOrOpen_WhenObjectDoesNotExist", func(t *testing.T) {
		mockBackendDs := new(MockBackendDataStore)

		mockBackendDs.On("IsObjectExist", ctx, path, true).Return(false, nil)
		mockBackendDs.On("CreateFolder", ctx, path, false).Return(nil)
		mockBackendDs.On("ReadShardFunc", ctx, path).Return(fun, nil)

		// Call the CreateOrOpen function with the mocked backend data store
		netDataStore, err := CreateOrOpen(ctx, path, fun, cfg)

		assert.NoError(t, err)
		assert.NotNil(t, netDataStore)
		// Add more assertions based on the expected behavior

		mockBackendDs.AssertExpectations(t)
	})

	t.Run("TestCreateOrOpen_WhenObjectExistsWithMatchingShardFunc", func(t *testing.T) {
		mockBackendDs := new(MockBackendDataStore)

		mockBackendDs.On("IsObjectExist", ctx, path, true).Return(true, nil)
		mockBackendDs.On("ReadShardFunc", ctx, path).Return(fun, nil)

		// Call the CreateOrOpen function with the mocked backend data store
		nds, err := CreateOrOpen(ctx, path, fun, cfg)

		validKey := ds.NewKey("/CIQILQ27JL3IOBXNVDXIPYYRVORV2EB6AZGV34TP5EVUMTCJJDA63LA")
		err = nds.Put(ctx, validKey, []byte("the actual is: value"))
		assert.NoError(t, err)
		// Test invalid key
		val2, err := nds.Get(ctx, validKey)
		assert.Error(t, err)
		assert.EqualError(t, err, "when getting '/validKey': ErrInvalidKey")
		assert.Equal(t, val2, []byte("the actual is: value"))
		assert.Error(t, err)
		assert.Equal(t, ErrDatastoreExists, err)
		// Add more assertions based on the expected behavior

		mockBackendDs.AssertExpectations(t)
	})

	// Add more test cases for different scenarios
}

// Define other necessary test cases for different scenarios
