package datastore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/glin-gogogo/go-net-datastore/utils"
	"github.com/go-http-utils/headers"
	ds "github.com/ipfs/go-datastore"
	dsQuery "github.com/ipfs/go-datastore/query"
	MinIO "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"math/rand"
	"path"
	"strings"
)

type Minio struct {
	utils.DataStoreConfig
	clients []*MinIO.Client
}

type MinioConfig struct {
	cfg utils.DataStoreConfig
}

type WithMinioOption struct{}

func (m *WithMinioOption) WithRegion(region string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.Region = region
		return nil
	}
}

func (m *WithMinioOption) WithEndpoint(endpoint string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.Endpoint = endpoint
		return nil
	}
}

func (m *WithMinioOption) WithAccessKey(accessKey string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.AccessKey = accessKey
		return nil
	}
}

func (m *WithMinioOption) WithSecretKey(secretKey string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.SecretKey = secretKey
		return nil
	}
}

func (m *WithMinioOption) WithBucket(bucket string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.Bucket = bucket

		if options.Bucket == "" {
			options.Bucket = utils.DefaultDataBucket
		}
		return nil
	}
}

func (m *WithMinioOption) WithRootDirectory(rootDirectory string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.RootDirectory = rootDirectory

		if options.RootDirectory == "" {
			options.RootDirectory = utils.DefaultRootDirectory
		}
		return nil
	}
}

func (m *WithMinioOption) WithWorkers(workers int) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.Workers = workers

		if options.Workers <= 0 {
			options.Workers = utils.MaxBatchWorkers
		}
		return nil
	}
}

func (m *Minio) dsPath(p string) string {
	return path.Join(m.RootDirectory, p)
}

func NewMinio(opts ...utils.WithOption) (DataStorage, error) {
	dsConfig := new(utils.DataStoreConfig)
	for _, o := range opts {
		if err := o(dsConfig); err != nil {
			return nil, err
		}
	}

	var clients []*MinIO.Client
	useSSL := false
	for i := 0; i < MaxInstanceNum; i++ {
		client, err := MinIO.New(dsConfig.Endpoint, &MinIO.Options{
			Creds:  credentials.NewStaticV4(dsConfig.AccessKey, dsConfig.SecretKey, ""),
			Secure: useSSL,
			// fix bug: too much idle connections exhaust system port resources
			//Transport: &http.Transport{
			//	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			//},
			Region: dsConfig.Region,
		})
		if err != nil {
			return nil, fmt.Errorf("new minio client failed: %s", err)
		}

		clients = append(clients, client)
	}

	return &Minio{
		DataStoreConfig: *dsConfig,
		clients:         clients,
	}, nil
}

func (m *Minio) RootDir() string {
	return m.RootDirectory
}

func (m *Minio) client() *MinIO.Client {
	return m.clients[rand.Intn(MaxInstanceNum)]
}

func (m *Minio) Put(ctx context.Context, k ds.Key, value []byte) error {
	_, err := m.client().PutObject(ctx, m.Bucket, m.dsPath(k.String()), bytes.NewReader(value), int64(len(value)), MinIO.PutObjectOptions{})
	return err
}

func (m *Minio) Sync(_ context.Context, _ ds.Key) error {
	return nil
}

func (m *Minio) Get(ctx context.Context, k ds.Key) ([]byte, error) {
	exist, err := m.IsObjectExist(ctx, m.dsPath(k.String()), false)
	if err != nil {
		return nil, err
	}

	if !exist {
		return nil, ds.ErrNotFound
	}

	resp, err := m.client().GetObject(ctx, m.Bucket, m.dsPath(k.String()), MinIO.GetObjectOptions{})
	if err != nil {
		if ErrNotFound(err) {
			return nil, ds.ErrNotFound
		}

		return nil, err
	}
	defer resp.Close()

	return io.ReadAll(resp)
}

func (m *Minio) Has(ctx context.Context, k ds.Key) (bool, error) {
	_, err := m.GetSize(ctx, k)
	if err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (m *Minio) GetSize(ctx context.Context, k ds.Key) (int, error) {
	metadata, err := m.client().StatObject(ctx, m.Bucket, m.dsPath(k.String()), MinIO.StatObjectOptions{})
	if err != nil {
		if ErrNotFound(err) {
			return -1, ds.ErrNotFound
		}

		return -1, err
	}

	return int(metadata.Size), nil
}

func (m *Minio) Delete(ctx context.Context, k ds.Key) error {
	err := m.client().RemoveObject(ctx, m.Bucket, m.dsPath(k.String()), MinIO.RemoveObjectOptions{})
	if ErrNotFound(err) {
		err = nil
	}

	return err
}

func (m *Minio) Query(ctx context.Context, q dsQuery.Query) (dsQuery.Results, error) {
	if q.Orders != nil || q.Filters != nil {
		return nil, fmt.Errorf("datastore minio: filters or orders are not supported")
	}

	q.Prefix = strings.TrimPrefix(q.Prefix, "/")
	limit := q.Offset + q.Limit
	if limit == 0 || limit > utils.DefaultListMax {
		limit = utils.DefaultListMax
	}

	objectCh := m.client().ListObjects(ctx, m.Bucket, MinIO.ListObjectsOptions{
		Prefix:       q.Prefix,
		MaxKeys:      limit,
		WithMetadata: true,
		Recursive:    true,
	})

	index := q.Offset
	nextValue := func() (dsQuery.Result, bool) {
		for object := range objectCh {
			if object.Err != nil {
				return dsQuery.Result{Error: object.Err}, false
			}

			if index <= 0 {
				dsKey, ok := utils.Decode(m.RootDirectory, object.Key)
				if !ok {
					return dsQuery.Result{Error: utils.ErrQueryBadData}, false
				}

				entry := dsQuery.Entry{
					Key:  dsKey.String(),
					Size: int(object.Size),
				}
				if !q.KeysOnly {
					resp, err := m.GetObject(ctx, object.Key)
					if err != nil {
						return dsQuery.Result{Error: err}, false
					}

					entry.Value, err = io.ReadAll(resp)
					resp.Close()
					if err != nil {
						return dsQuery.Result{Error: err}, false
					}
				}
				return dsQuery.Result{Entry: entry}, true
			}

			index--
		}

		return dsQuery.Result{}, false
	}

	return dsQuery.ResultsFromIterator(q, dsQuery.Iterator{
		Close: func() error {
			return nil
		},
		Next: nextValue,
	}), nil
}

func (m *Minio) Close() error {
	return nil
}

func (m *Minio) Batch(_ context.Context) (ds.Batch, error) {
	return nil, fmt.Errorf("datastore minio: batch is supported on upper class")
}

func (m *Minio) GetObjectMetadata(ctx context.Context, objectKey string) (*ObjectMetadata, bool, error) {
	resp, err := m.client().StatObject(ctx, m.Bucket, objectKey, MinIO.StatObjectOptions{})
	if err != nil {
		if ErrNotFound(err) {
			return nil, false, nil
		}

		return nil, false, err
	}

	return &ObjectMetadata{
		Key:                objectKey,
		ContentDisposition: resp.Metadata.Get(headers.ContentDisposition),
		ContentEncoding:    resp.Metadata.Get(headers.ContentEncoding),
		ContentLanguage:    resp.Metadata.Get(headers.ContentLanguage),
		ContentLength:      resp.Size,
		ContentType:        resp.ContentType,
		ETag:               resp.ETag,
		Digest:             resp.UserMetadata[MetaDigestUpper],
	}, true, nil
}

func (m *Minio) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	resp, err := m.client().GetObject(ctx, m.Bucket, objectKey, MinIO.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (m *Minio) PutObject(ctx context.Context, objectKey, digest string, totalLength int64, reader io.Reader) error {
	_, err := m.client().PutObject(ctx, m.Bucket, objectKey, reader, -1, MinIO.PutObjectOptions{})
	return err
}

func (m *Minio) DeleteObject(ctx context.Context, objectKey string) error {
	err := m.client().RemoveObject(ctx, m.Bucket, objectKey, MinIO.RemoveObjectOptions{})
	return err
}

func (m *Minio) DelUncompletedDirtyObject(_ context.Context, objectKey string) error {
	return nil
}

func (m *Minio) DeleteObjects(ctx context.Context, objects []*ObjectMetadata) error {
	objectsCh := make(chan MinIO.ObjectInfo)
	go func() {
		for _, obj := range objects {
			objectsCh <- MinIO.ObjectInfo{Key: obj.Key}
		}

		close(objectsCh)
	}()

	for rErr := range m.client().RemoveObjects(ctx, m.Bucket, objectsCh, MinIO.RemoveObjectsOptions{GovernanceBypass: true}) {
		return rErr.Err
	}

	return nil
}

func (m *Minio) ListFolderObjects(ctx context.Context, prefix string) ([]*ObjectMetadata, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var metadatas []*ObjectMetadata
	for object := range m.client().ListObjects(ctx, m.Bucket, MinIO.ListObjectsOptions{
		Prefix:       prefix,
		WithMetadata: true,
		Recursive:    true,
	}) {
		if object.Err != nil {
			return nil, object.Err
		}

		metadatas = append(metadatas, &ObjectMetadata{
			Key:           object.Key,
			ETag:          object.ETag,
			ContentLength: object.Size,
		})
	}

	return metadatas, nil
}

func (m *Minio) IsObjectExist(ctx context.Context, objectKey string, isFolder bool) (bool, error) {
	if isFolder {
		_, isExist, err := m.GetFolderMetadata(ctx, objectKey)
		return isExist, err
	} else {
		_, isExist, err := m.GetObjectMetadata(ctx, objectKey)
		return isExist, err
	}
}

func (m *Minio) CreateFolder(ctx context.Context, folderName string, isEmptyFolder bool) error {
	if !strings.HasSuffix(folderName, "/") {
		folderName += "/"
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	_, err := m.client().PutObject(ctx, m.Bucket, folderName, nil, 0, MinIO.PutObjectOptions{})
	if err != nil {
		return err
	}

	return nil
}

func (m *Minio) GetFolderMetadata(ctx context.Context, folderKey string) (*ObjectMetadata, bool, error) {
	if !strings.HasSuffix(folderKey, "/") {
		folderKey += "/"
	}

	meta, isExist, err := m.GetObjectMetadata(ctx, folderKey)
	if err != nil || isExist {
		return meta, isExist, err
	}

	exist := false
	for object := range m.client().ListObjects(ctx, m.Bucket, MinIO.ListObjectsOptions{
		Prefix:    folderKey,
		Recursive: false,
	}) {
		if object.Err != nil {
			return nil, false, object.Err
		}

		exist = true
		return &ObjectMetadata{
			Key: folderKey,
		}, exist, nil
	}

	return nil, exist, nil
}
