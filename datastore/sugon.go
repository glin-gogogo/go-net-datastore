package datastore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/glin-gogogo/go-net-datastore/utils"
	ds "github.com/ipfs/go-datastore"
	dsQuery "github.com/ipfs/go-datastore/query"
	sg "github.com/urchinfs/sugon-sdk/sugon"
	"io"
	"math/rand"
	"path"
	"strings"
)

const (
	HomeDir = "/work/home/denglf"
)

type Sugon struct {
	utils.DataStoreConfig
	clients []sg.Sgclient
	region  string
}

type SugonConfig struct {
	cfg utils.DataStoreConfig
}

type WithSugonOption struct{}

func (pc *WithSugonOption) WithRegion(region string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.Region = region
		return nil
	}
}

func (pc *WithSugonOption) WithEndpoint(endpoint string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.Endpoint = endpoint
		return nil
	}
}

func (pc *WithSugonOption) WithAccessKey(accessKey string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.AccessKey = accessKey
		return nil
	}
}

func (pc *WithSugonOption) WithSecretKey(secretKey string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.SecretKey = secretKey
		return nil
	}
}

func (pc *WithSugonOption) WithBucket(bucket string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.Bucket = bucket

		if options.Bucket == "" {
			options.Bucket = utils.DefaultDataBucket
		}
		return nil
	}
}

func (pc *WithSugonOption) WithRootDirectory(rootDirectory string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.RootDirectory = rootDirectory

		if options.RootDirectory == "" {
			options.RootDirectory = utils.DefaultRootDirectory
		}
		return nil
	}
}

func (pc *WithSugonOption) WithWorkers(workers int) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.Workers = workers

		if options.Workers <= 0 {
			options.Workers = utils.MaxBatchWorkers
		}
		return nil
	}
}

func (s *Sugon) dsPath(p string) string {
	return path.Join(s.RootDirectory, p)
}

func NewSugon(opts ...utils.WithOption) (DataStorage, error) {
	dsConfig := new(utils.DataStoreConfig)
	for _, o := range opts {
		if err := o(dsConfig); err != nil {
			return nil, err
		}
	}

	regionSplit := strings.Split(dsConfig.Region, ",")
	protocolPrefix := "https://"
	if len(regionSplit) != 3 {
		return nil, fmt.Errorf("new sugon client failed, invalid region %s", dsConfig.Region)
	}

	clusterId, _, found := strings.Cut(dsConfig.Endpoint, ".")
	if !found {
		return nil, fmt.Errorf("new sugon client failed, invalid endpoint %s", dsConfig.Endpoint)
	}

	var clients []sg.Sgclient
	for i := 0; i < MaxInstanceNum; i++ {
		client, err := sg.New(clusterId, dsConfig.AccessKey, dsConfig.SecretKey, regionSplit[0], protocolPrefix+regionSplit[1], protocolPrefix+regionSplit[2])
		if err != nil {
			return nil, fmt.Errorf("new sugon client failed: %s", err)
		}

		clients = append(clients, client)
	}

	dsConfig.Bucket = path.Join(HomeDir, dsConfig.Bucket)
	return &Sugon{
		DataStoreConfig: *dsConfig,
		clients:         clients,
		region:          dsConfig.Region,
	}, nil
}

func (s *Sugon) RootDir() string {
	return s.RootDirectory
}

func (s *Sugon) client() sg.Sgclient {
	return s.clients[rand.Intn(MaxInstanceNum)]
}

func (s *Sugon) Put(ctx context.Context, k ds.Key, value []byte) error {
	return s.client().Upload(path.Join(s.Bucket, s.dsPath(k.String())), bytes.NewReader(value), int64(len(value)))
}

func (s *Sugon) Sync(_ context.Context, prefix ds.Key) error {
	return nil
}

func (s *Sugon) Get(ctx context.Context, k ds.Key) ([]byte, error) {
	exist, err := s.IsObjectExist(ctx, s.dsPath(k.String()), false)
	if err != nil {
		return nil, err
	}

	if !exist {
		return nil, ds.ErrNotFound
	}

	resp, err := s.client().Download(path.Join(s.Bucket, s.dsPath(k.String())))
	if err != nil {
		if ErrNotFound(err) {
			return nil, ds.ErrNotFound
		}

		return nil, err
	}
	defer resp.Close()

	return io.ReadAll(resp)
}

func (s *Sugon) Has(ctx context.Context, k ds.Key) (bool, error) {
	_, err := s.GetSize(ctx, k)
	if err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Sugon) GetSize(ctx context.Context, k ds.Key) (int, error) {
	metadata, err := s.client().GetFileMeta(path.Join(s.Bucket, s.dsPath(k.String())))
	if err != nil {
		if ErrNotFound(err) {
			return -1, ds.ErrNotFound
		}

		return -1, err
	}

	return int(metadata.Size), nil
}

func (s *Sugon) Delete(ctx context.Context, k ds.Key) error {
	_, err := s.client().DeleteFile(path.Join(s.Bucket, s.dsPath(k.String())))
	if ErrNotFound(err) {
		err = nil
	}

	return err
}

func (s *Sugon) Query(ctx context.Context, q dsQuery.Query) (dsQuery.Results, error) {
	if q.Orders != nil || q.Filters != nil {
		return nil, fmt.Errorf("datastore minio: filters or orders are not supported")
	}

	q.Prefix = strings.TrimPrefix(q.Prefix, "/")
	limit := q.Offset + q.Limit
	if limit <= 0 {
		limit = utils.DefaultListMax
	}

	resp, err := s.client().GetFileListRecursive(path.Join(s.Bucket, q.Prefix), "", 0, int64(limit), MaxFolderDepth)
	if err != nil {
		return nil, err
	}

	index := q.Offset
	nextValue := func() (dsQuery.Result, bool) {
		for index >= len(resp) {
			return dsQuery.Result{}, false
		}

		dsKey, ok := utils.Decode(s.RootDirectory, resp[index].Name)
		if !ok {
			return dsQuery.Result{Error: utils.ErrQueryBadData}, false
		}

		entry := dsQuery.Entry{
			Key:  dsKey.String(),
			Size: int(resp[index].Size),
		}
		if !q.KeysOnly {
			resp, err := s.GetObject(ctx, resp[index].Name)
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

func (s *Sugon) Close() error {
	return nil
}

func (s *Sugon) Batch(_ context.Context) (ds.Batch, error) {
	return nil, fmt.Errorf("datastore sugon: batch is supported on upper class")
}

func (s *Sugon) GetObjectMetadata(ctx context.Context, objectKey string) (*ObjectMetadata, bool, error) {
	resp, err := s.client().GetFileMeta(path.Join(s.Bucket, objectKey))
	if err != nil {
		if ErrNotFound(err) {
			return nil, false, nil
		}

		return nil, false, err
	}

	return &ObjectMetadata{
		Key:           objectKey,
		ContentLength: resp.Size,
		ContentType:   resp.Type,
		ETag:          resp.LastModifiedTime,
	}, true, nil
}

func (s *Sugon) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	resp, err := s.client().Download(path.Join(s.Bucket, objectKey))
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *Sugon) PutObject(ctx context.Context, objectKey, digest string, totalLength int64, reader io.Reader) error {
	return s.client().Upload(path.Join(s.Bucket, objectKey), reader, totalLength)
}

func (s *Sugon) DeleteObject(ctx context.Context, objectKey string) error {
	_, err := s.client().DeleteFile(path.Join(s.Bucket, objectKey))
	return err
}

func (s *Sugon) DelUncompletedDirtyObject(ctx context.Context, objectKey string) error {
	return nil
}

func (s *Sugon) DeleteObjects(ctx context.Context, objects []*ObjectMetadata) error {
	for _, obj := range objects {
		err := s.DeleteObject(ctx, obj.Key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Sugon) ListFolderObjects(ctx context.Context, prefix string) ([]*ObjectMetadata, error) {
	var metadatas []*ObjectMetadata

	resp, err := s.client().GetFileListRecursive(path.Join(s.Bucket, prefix), "", 0, MaxFolderListPageSize, MaxFolderDepth)
	if err != nil {
		return nil, err
	}

	for _, object := range resp {
		//-fixme
		objectKey := strings.TrimPrefix(strings.TrimPrefix(object.Path, s.Bucket), "/")
		if object.IsDirectory {
			objectKey += "/"
		}
		//-
		metadatas = append(metadatas, &ObjectMetadata{
			Key:           object.Name,
			ETag:          object.LastModifiedTime,
			ContentLength: object.Size,
		})
	}
	return metadatas, nil
}

func (s *Sugon) IsObjectExist(ctx context.Context, objectKey string, isFolder bool) (bool, error) {
	if isFolder {
		_, isExist, err := s.GetFolderMetadata(ctx, objectKey)
		return isExist, err
	} else {
		_, isExist, err := s.GetObjectMetadata(ctx, objectKey)
		return isExist, err
	}
}

func (s *Sugon) CreateFolder(ctx context.Context, folderName string, isEmptyFolder bool) error {
	if !isEmptyFolder {
		return nil
	}

	_, err := s.client().CreateDir(path.Join(s.Bucket, folderName))
	return err
}

func (s *Sugon) GetFolderMetadata(ctx context.Context, folderKey string) (*ObjectMetadata, bool, error) {
	meta, err := s.client().GetFileMeta(path.Join(s.Bucket, folderKey))
	if err != nil {
		if ErrNotFound(err) {
			return nil, false, nil
		}

		return nil, false, err
	}

	if meta != nil {
		return &ObjectMetadata{
			Key:           folderKey,
			ContentLength: meta.Size,
			ETag:          meta.LastModifiedTime,
			ContentType:   meta.Type,
		}, true, nil
	}

	return nil, false, nil
}
