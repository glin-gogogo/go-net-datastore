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
	"io"
	"math/rand"
	"path"
	"strings"

	Paracloud "github.com/urchinfs/paracloud-sdk/paracloud"
)

type ParaCloud struct {
	utils.DataStoreConfig
	clients []Paracloud.Client
	region  string
}

type ParaCloudConfig struct {
	cfg utils.DataStoreConfig
}

type WithParaCloudOption struct{}

func (pc *WithParaCloudOption) WithRegion(region string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.Region = region
		return nil
	}
}

func (pc *WithParaCloudOption) WithEndpoint(endpoint string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.Endpoint = endpoint
		return nil
	}
}

func (pc *WithParaCloudOption) WithAccessKey(accessKey string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.AccessKey = accessKey
		return nil
	}
}

func (pc *WithParaCloudOption) WithSecretKey(secretKey string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.SecretKey = secretKey
		return nil
	}
}

func (pc *WithParaCloudOption) WithBucket(bucket string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.Bucket = bucket

		if options.Bucket == "" {
			options.Bucket = utils.DefaultDataBucket
		}
		return nil
	}
}

func (pc *WithParaCloudOption) WithRootDirectory(rootDirectory string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.RootDirectory = rootDirectory

		if options.RootDirectory == "" {
			options.RootDirectory = utils.DefaultRootDirectory
		}
		return nil
	}
}

func (pc *WithParaCloudOption) WithWorkers(workers int) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.Workers = workers

		if options.Workers <= 0 {
			options.Workers = utils.MaxBatchWorkers
		}
		return nil
	}
}

func (pc *ParaCloud) dsPath(p string) string {
	return path.Join(pc.RootDirectory, p)
}

func NewParaCloud(opts ...utils.WithOption) (DataStorage, error) {
	dsConfig := new(utils.DataStoreConfig)
	for _, o := range opts {
		if err := o(dsConfig); err != nil {
			return nil, err
		}
	}

	splits := strings.Split(dsConfig.Region, "|")
	if len(splits) != 2 {
		return nil, fmt.Errorf("new para cloud client failed, invalid region %s", dsConfig.Region)
	}

	var clients []Paracloud.Client
	for i := 0; i < MaxInstanceNum; i++ {
		client, err := Paracloud.New(dsConfig.AccessKey, dsConfig.SecretKey, dsConfig.Endpoint,
			splits[1], utils.RedisClusterIP, utils.RedisClusterPwd, false)
		if err != nil {
			return nil, fmt.Errorf("new para cloud client failed: %s", err)
		}

		clients = append(clients, client)
	}

	return &ParaCloud{
		DataStoreConfig: *dsConfig,
		clients:         clients,
		region:          splits[0],
	}, nil
}

func (pc *ParaCloud) RootDir() string {
	return pc.RootDirectory
}

func (pc *ParaCloud) client() Paracloud.Client {
	return pc.clients[rand.Intn(MaxInstanceNum)]
}

func (pc *ParaCloud) Put(ctx context.Context, k ds.Key, value []byte) error {
	return pc.client().UploadFile(ctx, pc.Bucket, pc.dsPath(k.String()), "", bytes.NewReader(value))
}

func (pc *ParaCloud) Sync(_ context.Context, prefix ds.Key) error {
	return nil
}

func (pc *ParaCloud) Get(ctx context.Context, k ds.Key) ([]byte, error) {
	exist, err := pc.IsObjectExist(ctx, pc.dsPath(k.String()), false)
	if err != nil {
		return nil, err
	}

	if !exist {
		return nil, ds.ErrNotFound
	}

	resp, err := pc.client().DownloadFile(ctx, pc.Bucket, pc.dsPath(k.String()))
	if err != nil {
		if ErrNotFound(err) {
			return nil, ds.ErrNotFound
		}

		return nil, err
	}
	defer resp.Close()

	return io.ReadAll(resp)
}

func (pc *ParaCloud) Has(ctx context.Context, k ds.Key) (bool, error) {
	_, err := pc.GetSize(ctx, k)
	if err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (pc *ParaCloud) GetSize(ctx context.Context, k ds.Key) (int, error) {
	metadata, err := pc.client().StatFile(ctx, pc.Bucket, pc.dsPath(k.String()))
	if err != nil {
		if ErrNotFound(err) {
			return -1, ds.ErrNotFound
		}

		return -1, err
	}

	return int(metadata.Size), nil
}

func (pc *ParaCloud) Delete(ctx context.Context, k ds.Key) error {
	err := pc.client().RemoveFile(ctx, pc.Bucket, pc.dsPath(k.String()))
	if ErrNotFound(err) {
		err = nil
	}

	return err
}

func (pc *ParaCloud) Query(ctx context.Context, q dsQuery.Query) (dsQuery.Results, error) {
	if q.Orders != nil || q.Filters != nil {
		return nil, fmt.Errorf("datastore minio: filters or orders are not supported")
	}

	q.Prefix = strings.TrimPrefix(q.Prefix, "/")
	limit := q.Offset + q.Limit
	if limit == 0 || limit > utils.DefaultListMax {
		limit = utils.DefaultListMax
	}

	resp, err := pc.client().ListDirFiles(ctx, pc.Bucket, q.Prefix)
	if err != nil {
		return nil, err
	}

	index := q.Offset
	nextValue := func() (dsQuery.Result, bool) {
		for index >= len(resp) {
			return dsQuery.Result{}, false
		}

		dsKey, ok := utils.Decode(pc.RootDirectory, resp[index].Key)
		if !ok {
			return dsQuery.Result{Error: utils.ErrQueryBadData}, false
		}

		entry := dsQuery.Entry{
			Key:  dsKey.String(),
			Size: int(resp[index].Size),
		}
		if !q.KeysOnly {
			resp, err := pc.GetObject(ctx, resp[index].Key)
			if err != nil {
				return dsQuery.Result{Error: err}, false
			}

			entry.Value, err = io.ReadAll(resp)
			if err != nil {
				return dsQuery.Result{Error: err}, false
			}
		}

		index++
		return dsQuery.Result{Entry: entry}, true
	}

	return dsQuery.ResultsFromIterator(q, dsQuery.Iterator{
		Close: func() error {
			return nil
		},
		Next: nextValue,
	}), nil
}

func (pc *ParaCloud) Close() error {
	return nil
}

func (pc *ParaCloud) Batch(_ context.Context) (ds.Batch, error) {
	return nil, fmt.Errorf("datastore para cloud: batch is supported on upper class")
}

func (pc *ParaCloud) GetObjectMetadata(ctx context.Context, objectKey string) (*ObjectMetadata, bool, error) {
	resp, err := pc.client().StatFile(ctx, pc.Bucket, objectKey)
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
	}, true, nil
}

func (pc *ParaCloud) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	resp, err := pc.client().DownloadFile(ctx, pc.Bucket, objectKey)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (pc *ParaCloud) PutObject(ctx context.Context, objectKey, digest string, totalLength int64, reader io.Reader) error {
	return pc.client().UploadFile(ctx, pc.Bucket, objectKey, digest, reader)
}

func (pc *ParaCloud) DeleteObject(ctx context.Context, objectKey string) error {
	return pc.client().RemoveFile(ctx, pc.Bucket, objectKey)
}

func (pc *ParaCloud) DelUncompletedDirtyObject(ctx context.Context, objectKey string) error {
	return pc.DeleteObject(ctx, objectKey)
}

func (pc *ParaCloud) DeleteObjects(ctx context.Context, objects []*ObjectMetadata) error {
	for _, obj := range objects {
		err := pc.DeleteObject(ctx, obj.Key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pc *ParaCloud) ListFolderObjects(ctx context.Context, prefix string) ([]*ObjectMetadata, error) {
	var metadatas []*ObjectMetadata

	resp, err := pc.client().ListDirFiles(ctx, pc.Bucket, prefix)
	if err != nil {
		return nil, err
	}

	for _, object := range resp {
		metadatas = append(metadatas, &ObjectMetadata{
			Key:           object.Key,
			ETag:          object.ETag,
			ContentLength: object.Size,
		})
	}
	return metadatas, nil
}

func (pc *ParaCloud) IsObjectExist(ctx context.Context, objectKey string, isFolder bool) (bool, error) {
	if isFolder {
		_, isExist, err := pc.GetFolderMetadata(ctx, objectKey)
		return isExist, err
	} else {
		_, isExist, err := pc.GetObjectMetadata(ctx, objectKey)
		return isExist, err
	}
}

func (pc *ParaCloud) CreateFolder(ctx context.Context, folderName string, isEmptyFolder bool) error {
	if !isEmptyFolder {
		return nil
	}

	err := pc.client().CreateDir(ctx, pc.Bucket, folderName)
	return err
}

func (pc *ParaCloud) GetFolderMetadata(ctx context.Context, folderKey string) (*ObjectMetadata, bool, error) {
	meta, isExist, err := pc.client().GetDirMetadata(ctx, pc.Bucket, folderKey)
	if err != nil {
		if ErrNotFound(err) {
			return nil, false, nil
		}

		return nil, false, err
	}

	if isExist && meta != nil {
		return &ObjectMetadata{
			Key:           meta.Key,
			ContentLength: meta.Size,
			ETag:          meta.ETag,
			ContentType:   meta.ContentType,
		}, isExist, nil
	}

	return nil, isExist, nil
}
