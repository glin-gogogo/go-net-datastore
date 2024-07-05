package datastore

import (
	"context"
	"github.com/ipfs/go-datastore"
	"io"
	"strings"
	"time"

	datastoreQuery "github.com/ipfs/go-datastore/query"
)

type dataStorageBlock interface {
	Put(ctx context.Context, k datastore.Key, value []byte) error
	Sync(ctx context.Context, prefix datastore.Key) error
	Get(ctx context.Context, k datastore.Key) ([]byte, error)
	Has(ctx context.Context, k datastore.Key) (exists bool, err error)
	GetSize(ctx context.Context, k datastore.Key) (size int, err error)
	Delete(ctx context.Context, k datastore.Key) error
	Query(ctx context.Context, q datastoreQuery.Query) (datastoreQuery.Results, error)
	Close() error
}

type dataStorageObject interface {
	GetBucketMetadata(ctx context.Context, bucketName string) (*BucketMetadata, error)
	CreateBucket(ctx context.Context, bucketName string) error
	DeleteBucket(ctx context.Context, bucketName string) error
	ListBucketMetadatas(ctx context.Context) ([]*BucketMetadata, error)
	IsBucketExist(ctx context.Context, bucketName string) (bool, error)
	GetObjectMetadata(ctx context.Context, bucketName, objectKey string) (*ObjectMetadata, bool, error)
	GetObject(ctx context.Context, bucketName, objectKey string) (io.ReadCloser, error)
	PutObject(ctx context.Context, bucketName, objectKey, digest string, reader io.Reader) error
	PutObjectWithTotalLength(ctx context.Context, bucketName, objectKey, digest string, totalLength int64, reader io.Reader) error
	DeleteObject(ctx context.Context, bucketName, objectKey string) error
	DelUncompletedDirtyObject(ctx context.Context, bucketName, objectKey string) error
	DeleteObjects(ctx context.Context, bucketName string, objects []*ObjectMetadata) error
	ListObjectMetadatas(ctx context.Context, bucketName, prefix, marker string, limit int64) ([]*ObjectMetadata, error)
	IsObjectExist(ctx context.Context, bucketName, objectKey string) (bool, error)
	GetSignURL(ctx context.Context, bucketName, objectKey string, method Method, expire time.Duration) (string, error)
	CreateFolder(ctx context.Context, bucketName, folderName string, isEmptyFolder bool) error
	ListFolderObjects(ctx context.Context, bucketName, prefix string) ([]*ObjectMetadata, error)
	GetFolderMetadata(ctx context.Context, bucketName, folderKey string) (*ObjectMetadata, bool, error)
	GetCoroutineCount(ctx context.Context) (int32, error)
}

type DataStorage interface {
	dataStorageBlock
	dataStorageObject

	Batch(_ context.Context) (datastore.Batch, error)
}

type DataStorageBatch interface {
	Put(ctx context.Context, key datastore.Key, value []byte) error
	Delete(ctx context.Context, k datastore.Key) error
	Commit(ctx context.Context) error
}

func ErrNotFound(err error) bool {
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "404") ||
		strings.Contains(errMsg, "not exists") ||
		strings.Contains(errMsg, "not exist") ||
		strings.Contains(errMsg, "nosuchkey")
}
