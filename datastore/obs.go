package datastore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	ds "github.com/ipfs/go-datastore"
	dsQuery "github.com/ipfs/go-datastore/query"
	"go-net-datastore/utils"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	huaweiobs "github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

type Obs struct {
	utils.DataStoreConfig
	client *huaweiobs.ObsClient
}

type ObsConfig struct {
	cfg utils.DataStoreConfig
}

type WithObsOption struct{}

func (o *WithObsOption) WithRegion(region string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.Region = region
		return nil
	}
}

func (o *WithObsOption) WithEndpoint(endpoint string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.Endpoint = endpoint
		return nil
	}
}

func (o *WithObsOption) WithAccessKey(accessKey string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.AccessKey = accessKey
		return nil
	}
}

func (o *WithObsOption) WithSecretKey(secretKey string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.SecretKey = secretKey
		return nil
	}
}

func (o *WithObsOption) WithBucket(bucket string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.Bucket = bucket

		if options.Bucket == "" {
			options.Bucket = utils.DefaultDataBucket
		}
		return nil
	}
}

func (o *WithObsOption) WithRootDirectory(rootDirectory string) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.RootDirectory = rootDirectory

		if options.RootDirectory == "" {
			options.RootDirectory = utils.DefaultRootDirectory
		}
		return nil
	}
}

func (o *WithObsOption) WithWorkers(workers int) utils.WithOption {
	return func(options *utils.DataStoreConfig) error {
		options.Workers = workers

		if options.Workers <= 0 {
			options.Workers = utils.MaxBatchWorkers
		}
		return nil
	}
}

func (o *Obs) dsPath(p string) string {
	return path.Join(o.RootDirectory, p)
}

func NewOBS(opts ...utils.WithOption) (DataStorage, error) {
	dsConfig := new(utils.DataStoreConfig)
	for _, o := range opts {
		if err := o(dsConfig); err != nil {
			return nil, err
		}
	}

	client, err := huaweiobs.New(dsConfig.AccessKey, dsConfig.SecretKey, dsConfig.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("new obs client failed: %s", err)
	}

	return &Obs{
		DataStoreConfig: *dsConfig,
		client:          client,
	}, nil
}

func (o *Obs) Put(_ context.Context, k ds.Key, value []byte) error {
	_, err := o.client.PutObject(&huaweiobs.PutObjectInput{
		PutObjectBasicInput: huaweiobs.PutObjectBasicInput{
			ObjectOperationInput: huaweiobs.ObjectOperationInput{
				Bucket:   o.Bucket,
				Key:      o.dsPath(k.String()),
				Metadata: nil,
			},
		},
		Body: bytes.NewReader(value),
	})

	return err
}

func (o *Obs) Sync(ctx context.Context, prefix ds.Key) error {
	return nil
}

func (o *Obs) Get(ctx context.Context, k ds.Key) ([]byte, error) {
	resp, err := o.client.GetObject(&huaweiobs.GetObjectInput{
		GetObjectMetadataInput: huaweiobs.GetObjectMetadataInput{
			Bucket: o.Bucket,
			Key:    o.dsPath(k.String()),
		},
	})

	if err != nil {
		if ErrNotFound(err) {
			return nil, ds.ErrNotFound
		}

		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func (o *Obs) Has(ctx context.Context, k ds.Key) (bool, error) {
	_, err := o.GetSize(ctx, k)
	if err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (o *Obs) GetSize(ctx context.Context, k ds.Key) (int, error) {
	metadata, err := o.client.GetObjectMetadata(&huaweiobs.GetObjectMetadataInput{
		Bucket: o.Bucket,
		Key:    o.dsPath(k.String()),
	})
	if err != nil {
		if ErrNotFound(err) {
			return -1, ds.ErrNotFound
		}

		return -1, err
	}

	return int(metadata.ContentLength), nil
}

func (o *Obs) Delete(ctx context.Context, k ds.Key) error {
	_, err := o.client.DeleteObject(&huaweiobs.DeleteObjectInput{
		Bucket: o.Bucket,
		Key:    o.dsPath(k.String()),
	})
	if ErrNotFound(err) {
		err = nil
	}

	return err
}

func (o *Obs) Query(ctx context.Context, q dsQuery.Query) (dsQuery.Results, error) {
	if q.Orders != nil || q.Filters != nil {
		return nil, fmt.Errorf("datastore obs: filters or orders are not supported")
	}

	q.Prefix = strings.TrimPrefix(q.Prefix, "/")
	limit := q.Limit + q.Offset
	if limit == 0 || limit > utils.DefaultListMax {
		limit = utils.DefaultListMax
	}

	resp, err := o.client.ListObjects(&huaweiobs.ListObjectsInput{
		ListObjsInput: huaweiobs.ListObjsInput{
			Prefix:  o.dsPath(q.Prefix),
			MaxKeys: limit,
		},
		Bucket: o.Bucket,
		Marker: "",
	})
	if err != nil {
		return nil, err
	}

	index := q.Offset
	nextValue := func() (dsQuery.Result, bool) {
		for index >= len(resp.Contents) {
			if !resp.IsTruncated {
				return dsQuery.Result{}, false
			}

			index -= len(resp.Contents)
			resp, err = o.client.ListObjects(&huaweiobs.ListObjectsInput{
				ListObjsInput: huaweiobs.ListObjsInput{
					Prefix:    o.dsPath(q.Prefix),
					MaxKeys:   utils.DefaultListMax,
					Delimiter: "/",
				},
				Bucket: o.Bucket,
				Marker: "",
			})
			if err != nil {
				return dsQuery.Result{Error: err}, false
			}
		}

		entry := dsQuery.Entry{
			Key:  ds.NewKey(resp.Contents[index].Key).String(),
			Size: int(resp.Contents[index].Size),
		}
		if !q.KeysOnly {
			value, err := o.Get(ctx, ds.NewKey(entry.Key))
			if err != nil {
				return dsQuery.Result{Error: err}, false
			}
			entry.Value = value
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

func (o *Obs) Close() error {
	return nil
}

type obsBatch struct {
	o          *Obs
	ops        map[string]batchOp
	numWorkers int
}

type batchOp struct {
	val    []byte
	delete bool
}

func (o *Obs) Batch(_ context.Context) (ds.Batch, error) {
	return &obsBatch{
		o:          o,
		ops:        make(map[string]batchOp),
		numWorkers: o.Workers,
	}, nil
}

func (b *obsBatch) Put(ctx context.Context, k ds.Key, val []byte) error {
	b.ops[k.String()] = batchOp{
		val:    val,
		delete: false,
	}
	return nil
}

func (b *obsBatch) Delete(ctx context.Context, k ds.Key) error {
	b.ops[k.String()] = batchOp{
		val:    nil,
		delete: true,
	}
	return nil
}

func (b *obsBatch) Commit(ctx context.Context) error {
	var (
		//deleteObjs []*s3.ObjectIdentifier
		deleteObjs []huaweiobs.ObjectToDelete
		putKeys    []ds.Key
	)
	for k, op := range b.ops {
		if op.delete {
			deleteObjs = append(deleteObjs, huaweiobs.ObjectToDelete{
				Key: k,
			})
		} else {
			putKeys = append(putKeys, ds.NewKey(k))
		}
	}

	numJobs := len(putKeys) + (len(deleteObjs) / utils.DefaultDeleteMax)
	jobs := make(chan func() error, numJobs)
	results := make(chan error, numJobs)

	numWorkers := b.numWorkers
	if numJobs < numWorkers {
		numWorkers = numJobs
	}

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	defer wg.Wait()

	for w := 0; w < numWorkers; w++ {
		go func() {
			defer wg.Done()
			worker(jobs, results)
		}()
	}

	for _, k := range putKeys {
		jobs <- b.newPutJob(ctx, k, b.ops[k.String()].val)
	}

	if len(deleteObjs) > 0 {
		for i := 0; i < len(deleteObjs); i += utils.DefaultDeleteMax {
			limit := utils.DefaultDeleteMax
			if len(deleteObjs[i:]) < limit {
				limit = len(deleteObjs[i:])
			}

			jobs <- b.newDeleteJob(ctx, deleteObjs[i:i+limit])
		}
	}
	close(jobs)

	var errs []string
	for i := 0; i < numJobs; i++ {
		err := <-results
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("s3ds: failed batch operation:\n%s", strings.Join(errs, "\n"))
	}

	return nil
}

func (b *obsBatch) newPutJob(ctx context.Context, k ds.Key, value []byte) func() error {
	return func() error {
		return b.o.Put(ctx, k, value)
	}
}

func (b *obsBatch) newDeleteJob(ctx context.Context, objs []huaweiobs.ObjectToDelete) func() error {
	return func() error {
		resp, err := b.o.client.DeleteObjects(&huaweiobs.DeleteObjectsInput{
			Bucket:  b.o.Bucket,
			Objects: objs[:],
		})
		if err != nil && !ErrNotFound(err) {
			return err
		}

		var errs []string
		for _, err := range resp.Errors {
			if err.Code != "" && ErrNotFound(errors.New(err.Code)) {
				continue
			}
			errs = append(errs, err.Code)
		}

		if len(errs) > 0 {
			return fmt.Errorf("failed to delete objects: %s", errs)
		}

		return nil
	}
}

func worker(jobs <-chan func() error, results chan<- error) {
	for j := range jobs {
		results <- j()
	}
}

func (o *Obs) GetBucketMetadata(ctx context.Context, bucketName string) (*BucketMetadata, error) {
	if _, err := o.client.GetBucketMetadata(&huaweiobs.GetBucketMetadataInput{Bucket: bucketName}); err != nil {
		return nil, err
	}

	return &BucketMetadata{
		Name: bucketName,
	}, nil
}

func (o *Obs) CreateBucket(ctx context.Context, bucketName string) error {
	_, err := o.client.CreateBucket(&huaweiobs.CreateBucketInput{Bucket: bucketName})
	return err
}

func (o *Obs) DeleteBucket(ctx context.Context, bucketName string) error {
	_, err := o.client.DeleteBucket(bucketName)
	return err
}

func (o *Obs) ListBucketMetadatas(ctx context.Context) ([]*BucketMetadata, error) {
	resp, err := o.client.ListBuckets(&huaweiobs.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	var metadatas []*BucketMetadata
	for _, bucket := range resp.Buckets {
		metadatas = append(metadatas, &BucketMetadata{
			Name:     bucket.Name,
			CreateAt: bucket.CreationDate,
		})
	}

	return metadatas, nil
}

func (o *Obs) GetObjectMetadata(ctx context.Context, bucketName, objectKey string) (*ObjectMetadata, bool, error) {
	metadata, err := o.client.GetObjectMetadata(&huaweiobs.GetObjectMetadataInput{Bucket: bucketName, Key: objectKey})
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return nil, false, nil
		}

		return nil, false, err
	}

	object, err := o.client.GetObject(&huaweiobs.GetObjectInput{
		GetObjectMetadataInput: huaweiobs.GetObjectMetadataInput{
			Bucket: bucketName,
			Key:    objectKey,
		},
	})
	if err != nil {
		return nil, false, err
	}

	return &ObjectMetadata{
		Key:                objectKey,
		ContentDisposition: object.ContentDisposition,
		ContentEncoding:    object.ContentEncoding,
		ContentLanguage:    object.ContentLanguage,
		ContentLength:      metadata.ContentLength,
		ContentType:        metadata.ContentType,
		ETag:               metadata.ETag,
		Digest:             metadata.Metadata[MetaDigest],
	}, true, nil
}

func (o *Obs) GetObject(ctx context.Context, bucketName, objectKey string) (io.ReadCloser, error) {
	resp, err := o.client.GetObject(&huaweiobs.GetObjectInput{
		GetObjectMetadataInput: huaweiobs.GetObjectMetadataInput{
			Bucket: bucketName,
			Key:    objectKey,
		},
	})
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (o *Obs) PutObject(ctx context.Context, bucketName, objectKey, digest string, reader io.Reader) error {
	_, err := o.client.PutObject(&huaweiobs.PutObjectInput{
		PutObjectBasicInput: huaweiobs.PutObjectBasicInput{
			ObjectOperationInput: huaweiobs.ObjectOperationInput{
				Bucket: bucketName,
				Key:    objectKey,
				Metadata: map[string]string{
					MetaDigest: digest,
				},
			},
		},
		Body: reader,
	})

	return err
}

//func (o *obs) Batch(_ context.Context) (datastore.Batch, error) {
//	return nil, nil
//}

func (o *Obs) PutObjectWithTotalLength(ctx context.Context, bucketName, objectKey, digest string, totalLength int64, reader io.Reader) error {
	return nil
}

func (o *Obs) DeleteObject(ctx context.Context, bucketName, objectKey string) error {
	_, err := o.client.DeleteObject(&huaweiobs.DeleteObjectInput{Bucket: bucketName, Key: objectKey})
	return err
}

func (o *Obs) DelUncompletedDirtyObject(ctx context.Context, bucketName, objectKey string) error {
	return nil
}

func (o *Obs) DeleteObjects(ctx context.Context, bucketName string, objects []*ObjectMetadata) error {
	input := &huaweiobs.DeleteObjectsInput{}
	input.Bucket = bucketName

	var objectsToDel []huaweiobs.ObjectToDelete
	for _, obj := range objects {
		objectsToDel = append(objectsToDel, huaweiobs.ObjectToDelete{Key: obj.Key})
	}
	input.Objects = objectsToDel[:]

	_, err := o.client.DeleteObjects(input)
	return err
}

func (o *Obs) ListObjectMetadatas(ctx context.Context, bucketName, prefix, marker string, limit int64) ([]*ObjectMetadata, error) {
	resp, err := o.client.ListObjects(&huaweiobs.ListObjectsInput{
		ListObjsInput: huaweiobs.ListObjsInput{
			Prefix:  prefix,
			MaxKeys: int(limit),
		},
		Bucket: bucketName,
		Marker: marker,
	})
	if err != nil {
		return nil, err
	}

	var metadatas []*ObjectMetadata
	for _, object := range resp.Contents {
		metadatas = append(metadatas, &ObjectMetadata{
			Key:           object.Key,
			ETag:          object.ETag,
			ContentLength: object.Size,
		})
	}

	return metadatas, nil
}

func (o *Obs) ListFolderObjects(ctx context.Context, bucketName, prefix string) ([]*ObjectMetadata, error) {
	var metadatas []*ObjectMetadata
	input := &huaweiobs.ListObjectsInput{}
	input.Bucket = bucketName
	input.ListObjsInput.Prefix = prefix
	input.Marker = ""
	input.ListObjsInput.MaxKeys = 100

	for {
		resp, err := o.client.ListObjects(input)
		if err != nil {
			return nil, err
		}

		for _, object := range resp.Contents {
			metadatas = append(metadatas, &ObjectMetadata{
				Key:           object.Key,
				ETag:          object.ETag,
				ContentLength: object.Size,
			})
		}

		if resp.NextMarker == "" {
			return metadatas, nil
		}
		input.Marker = resp.NextMarker
	}
}

func (o *Obs) IsObjectExist(ctx context.Context, bucketName, objectKey string) (bool, error) {
	_, isExist, err := o.GetObjectMetadata(ctx, bucketName, objectKey)
	return isExist, err
}

func (o *Obs) IsBucketExist(ctx context.Context, bucketName string) (bool, error) {
	if _, err := o.client.HeadBucket(bucketName); err != nil {
		return false, err
	}

	return true, nil
}

func (o *Obs) GetSignURL(ctx context.Context, bucketName, objectKey string, method Method, expire time.Duration) (string, error) {
	var obsHTTPMethod huaweiobs.HttpMethodType
	switch method {
	case MethodGet:
		obsHTTPMethod = huaweiobs.HttpMethodGet
	case MethodPut:
		obsHTTPMethod = huaweiobs.HttpMethodPut
	case MethodHead:
		obsHTTPMethod = huaweiobs.HTTP_HEAD
	case MethodPost:
		obsHTTPMethod = huaweiobs.HttpMethodPost
	case MethodDelete:
		obsHTTPMethod = huaweiobs.HTTP_DELETE
	default:
		return "", fmt.Errorf("not support method %s", method)
	}

	resp, err := o.client.CreateSignedUrl(&huaweiobs.CreateSignedUrlInput{
		Bucket:  bucketName,
		Key:     objectKey,
		Method:  obsHTTPMethod,
		Expires: int(expire.Seconds()),
	})
	if err != nil {
		return "", err
	}

	return resp.SignedUrl, nil
}

func (o *Obs) CreateFolder(ctx context.Context, bucketName, folderName string, isEmptyFolder bool) error {
	if !strings.HasSuffix(folderName, "/") {
		folderName += "/"
	}

	_, err := o.client.PutObject(&huaweiobs.PutObjectInput{
		PutObjectBasicInput: huaweiobs.PutObjectBasicInput{
			ObjectOperationInput: huaweiobs.ObjectOperationInput{
				Bucket: bucketName,
				Key:    folderName,
			},
		},
	})

	return err
}

func (o *Obs) GetFolderMetadata(ctx context.Context, bucketName, folderKey string) (*ObjectMetadata, bool, error) {
	if !strings.HasSuffix(folderKey, "/") {
		folderKey += "/"
	}

	metadata, err := o.client.GetObjectMetadata(&huaweiobs.GetObjectMetadataInput{Bucket: bucketName, Key: folderKey})
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			//objs, err := o.ListObjectMetadatas(ctx, bucketName, folderKey, "", 1)
			//if err != nil {
			//	return nil, false, err
			//} else if len(objs) >= 1 {
			//	return &ObjectMetadata{
			//		Key: folderKey,
			//	}, true, nil
			//} else {
			//	return nil, false, nil
			//}
			return nil, false, nil
		}

		return nil, false, err
	}

	object, err := o.client.GetObject(&huaweiobs.GetObjectInput{
		GetObjectMetadataInput: huaweiobs.GetObjectMetadataInput{
			Bucket: bucketName,
			Key:    folderKey,
		},
	})
	if err != nil {
		return nil, false, err
	}

	return &ObjectMetadata{
		Key:                folderKey,
		ContentDisposition: object.ContentDisposition,
		ContentEncoding:    object.ContentEncoding,
		ContentLanguage:    object.ContentLanguage,
		ContentLength:      metadata.ContentLength,
		ContentType:        metadata.ContentType,
		ETag:               metadata.ETag,
		Digest:             metadata.Metadata[MetaDigest],
	}, true, nil
}

func (o *Obs) GetCoroutineCount(ctx context.Context) (int32, error) {
	return -1, nil
}
