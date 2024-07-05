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

type DataStorageObject interface {
	GetObjectMetadata(ctx context.Context, objectKey string) (*ObjectMetadata, bool, error)
	GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error)
	PutObject(ctx context.Context, objectKey, digest string, reader io.Reader) error
	PutObjectWithTotalLength(ctx context.Context, objectKey, digest string, totalLength int64, reader io.Reader) error
	DeleteObject(ctx context.Context, objectKey string) error
	DelUncompletedDirtyObject(ctx context.Context, objectKey string) error
	DeleteObjects(ctx context.Context, objects []*ObjectMetadata) error
	ListObjectMetadatas(ctx context.Context, prefix, marker string, limit int64) ([]*ObjectMetadata, error)
	IsObjectExist(ctx context.Context, objectKey string, isFolder bool) (bool, error)
	GetSignURL(ctx context.Context, objectKey string, method Method, expire time.Duration) (string, error)
	CreateFolder(ctx context.Context, folderName string, isEmptyFolder bool) error
	ListFolderObjects(ctx context.Context, prefix string) ([]*ObjectMetadata, error)
	GetFolderMetadata(ctx context.Context, folderKey string) (*ObjectMetadata, bool, error)
}

type DataStorage interface {
	dataStorageBlock
	DataStorageObject

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
