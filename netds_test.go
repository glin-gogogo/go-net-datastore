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
	fun, _ := ParseShardFunc("/repo/netds/shard/v1/next-to-last/2")
	cfg := utils.DataStoreConfig{
		Name:          "obs",
		Region:        "cn-south-222",
		Endpoint:      "obs.cn-south.ccccccooooooommmmmm",
		AccessKey:     "xxxxxxxxxxxxxxx",
		SecretKey:     "yyyyyyyyyyyyyyy",
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
func TestCommit(t *testing.T) {
	ctx := context.Background()

	path := "cads/blocks"
	fun, _ := ParseShardFunc("/repo/netds/shard/v1/next-to-last/2")
	cfg := utils.DataStoreConfig{
		Name:          "obs",
		Region:        "cn-south-222",
		Endpoint:      "obs.cn-south.ccccccooooooommmmmm",
		AccessKey:     "xxxxxxxxxxxxxxx",
		SecretKey:     "yyyyyyyyyyyyyyy",
		Bucket:        "urchin-data",
		RootDirectory: "cads/blocks",
		//Workers:       10,
	}
	nds, err := CreateOrOpen(ctx, path, fun, cfg)

	// 创建NetDataStoreBatch实例
	n, _ := nds.Batch(ctx)
	//n := &NetDataStoreBatch{
	//	nds:        nds,
	//	ops:        make(map[string]batchOp),
	//	numWorkers: 2, // 设置工作线程数
	//}

	// 添加操作到ops映射中
	_ = n.Put(ctx, ds.NewKey("/KEY1111111111"), []byte("value1"))
	_ = n.Put(ctx, ds.NewKey("/KEY2222222222"), []byte("value2"))
	_ = n.Put(ctx, ds.NewKey("/KEY3333333333"), []byte("value3"))

	// 执行Commit方法
	err = n.Commit(ctx)

	_ = n.Put(ctx, ds.NewKey("/KEY4444444444"), []byte("value4"))
	_ = n.Put(ctx, ds.NewKey("/KEY5555555555"), []byte("value5"))
	err = n.Commit(ctx)

	_ = n.Put(ctx, ds.NewKey("/KEY6666666666"), []byte("value6"))
	_ = n.Put(ctx, ds.NewKey("/KEY7777777777"), []byte("value7"))
	_ = n.Put(ctx, ds.NewKey("/KEY8888888888"), []byte("value8"))
	_ = n.Put(ctx, ds.NewKey("/KEY9999999999"), []byte("value9"))
	err = n.Commit(ctx)

	// 验证Commit方法是否返回错误
	if err != nil {
		t.Errorf("Commit returned error: %v", err)
	}
}
