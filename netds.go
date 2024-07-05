package netds

import (
	"context"
	"errors"
	"fmt"
	"github.com/glin-gogogo/go-net-datastore/datastore"
	"github.com/glin-gogogo/go-net-datastore/utils"
	ds "github.com/ipfs/go-datastore"
	dsQuery "github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var log = logging.Logger("netds")

var (
	ErrDatastoreExists       = errors.New("datastore already exists")
	ErrDatastoreDoesNotExist = errors.New("datastore directory does not exist")
	ErrShardingFileMissing   = fmt.Errorf("%s file not found in datastore", ShardingFn)
	ErrClosed                = errors.New("datastore closed")
	ErrInvalidKey            = errors.New("key not supported by netds")
)

type ShardFunc func(string) string

type NetDataStore struct {
	shardStr   string
	getDir     ShardFunc
	ds         datastore.DataStorage
	numWorkers int
}

func CreateOrOpen(ctx context.Context, path string, fun *ShardIdV1, cfg utils.DataStoreConfig) (*NetDataStore, error) {
	backendDs, err := New(cfg)
	if err != nil {
		return nil, err
	}

	exist, err := backendDs.IsObjectExist(ctx, path, true)
	if err != nil {
		return nil, err
	}

	if !exist {
		err := backendDs.CreateFolder(ctx, path, false)
		if err != nil {
			return nil, err
		}

		dsFun, err := ReadShardFunc(backendDs, path)
		switch {
		case errors.Is(err, ErrShardingFileMissing):
			err = WriteShardFunc(backendDs, path, fun)
			if err != nil {
				return nil, err
			}
		case err == nil:
			if fun.String() != dsFun.String() {
				return nil, fmt.Errorf("specified shard func '%s' does not match repo shard func '%s'",
					fun.String(), dsFun.String())
			}
			return nil, ErrDatastoreExists
		default:
			return nil, err
		}
	}

	shardId, err := ReadShardFunc(backendDs, path)
	if err != nil {
		return nil, err
	}

	return &NetDataStore{
		shardStr:   shardId.String(),
		getDir:     shardId.Func(),
		ds:         backendDs,
		numWorkers: cfg.Workers,
	}, nil
}

func (nds *NetDataStore) encode(key ds.Key) (dir, file string) {
	noSlash := key.String()[1:]
	dir = nds.getDir(noSlash)
	file = filepath.Join(dir, noSlash+utils.Extension)
	return dir, file
}

func (nds *NetDataStore) decode(file string) (key ds.Key, ok bool) {
	if !strings.HasSuffix(file, utils.Extension) {
		return ds.Key{}, false
	}
	name := file[:len(file)-len(utils.Extension)]
	return ds.NewKey(name), true
}

func (nds *NetDataStore) Put(ctx context.Context, k ds.Key, value []byte) error {
	if !utils.KeyIsValid(k) {
		return fmt.Errorf("when putting '%q': %v", k, ErrInvalidKey)
	}

	dir, path := nds.encode(k)

	exist, _ := nds.IsObjectExist(ctx, dir, true)
	if !exist {
		if err := nds.CreateFolder(ctx, dir, false); err != nil {
			return err
		}
	}

	return nds.ds.Put(ctx, ds.NewKey(path), value)
}

func (nds *NetDataStore) Get(ctx context.Context, k ds.Key) ([]byte, error) {
	if !utils.KeyIsValid(k) {
		return nil, ds.ErrNotFound
	}

	_, path := nds.encode(k)
	return nds.ds.Get(ctx, ds.NewKey(path))
}

func (nds *NetDataStore) Has(ctx context.Context, k ds.Key) (bool, error) {
	if !utils.KeyIsValid(k) {
		return false, nil
	}

	_, path := nds.encode(k)
	return nds.ds.Has(ctx, ds.NewKey(path))
}
func (nds *NetDataStore) GetSize(ctx context.Context, k ds.Key) (int, error) {
	if !utils.KeyIsValid(k) {
		return -1, ds.ErrNotFound
	}

	_, path := nds.encode(k)
	return nds.ds.GetSize(ctx, ds.NewKey(path))
}
func (nds *NetDataStore) Delete(ctx context.Context, k ds.Key) error {
	if !utils.KeyIsValid(k) {
		return nil
	}

	_, path := nds.encode(k)
	return nds.ds.Delete(ctx, ds.NewKey(path))
}
func (nds *NetDataStore) Query(ctx context.Context, q dsQuery.Query) (dsQuery.Results, error) {
	return nds.ds.Query(ctx, q)
}

func (nds *NetDataStore) Sync(ctx context.Context, prefix ds.Key) error {
	return nds.ds.Sync(ctx, prefix)
}
func (nds *NetDataStore) Close() error {
	return nds.ds.Close()
}

type NetDataStoreBatch struct {
	nds        *NetDataStore
	ops        map[string]batchOp
	numWorkers int
}

type batchOp struct {
	val    []byte
	delete bool
}

func (nds *NetDataStore) Batch(_ context.Context) (ds.Batch, error) {
	return &NetDataStoreBatch{
		nds:        nds,
		ops:        make(map[string]batchOp),
		numWorkers: nds.numWorkers,
	}, nil
}

func (n *NetDataStoreBatch) Put(ctx context.Context, k ds.Key, val []byte) error {
	n.ops[k.String()] = batchOp{
		val:    val,
		delete: false,
	}
	return nil
}

func (n *NetDataStoreBatch) Delete(ctx context.Context, k ds.Key) error {
	n.ops[k.String()] = batchOp{
		val:    nil,
		delete: true,
	}
	return nil
}

func (n *NetDataStoreBatch) Commit(ctx context.Context) error {
	var (
		deleteObjs []ds.Key
		putKeys    []ds.Key
	)
	for k, op := range n.ops {
		if op.delete {
			deleteObjs = append(deleteObjs, ds.NewKey(k))
		} else {
			putKeys = append(putKeys, ds.NewKey(k))
		}
	}

	numJobs := len(putKeys) + (len(deleteObjs) / utils.DefaultDeleteMax)
	jobs := make(chan func() error, numJobs)
	results := make(chan error, numJobs)

	numWorkers := n.numWorkers
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
		jobs <- n.newPutJob(ctx, k, n.ops[k.String()].val)
	}

	if len(deleteObjs) > 0 {
		for i := 0; i < len(deleteObjs); i += utils.DefaultDeleteMax {
			limit := utils.DefaultDeleteMax
			if len(deleteObjs[i:]) < limit {
				limit = len(deleteObjs[i:])
			}

			jobs <- n.newDeleteJob(ctx, deleteObjs[i:i+limit])
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
		return fmt.Errorf("netds: failed batch operation:\n%s", strings.Join(errs, "\n"))
	}

	return nil
}

func (n *NetDataStoreBatch) newPutJob(ctx context.Context, k ds.Key, value []byte) func() error {
	return func() error {
		return n.nds.Put(ctx, k, value)
	}
}

func (n *NetDataStoreBatch) newDeleteJob(ctx context.Context, objs []ds.Key) func() error {
	var objects []*datastore.ObjectMetadata
	for _, obj := range objs {
		_, path := n.nds.encode(obj)
		objects = append(objects, &datastore.ObjectMetadata{
			Key: path,
		})
	}

	return func() error {
		return n.nds.DeleteObjects(ctx, objects)
	}
}

func worker(jobs <-chan func() error, results chan<- error) {
	for j := range jobs {
		results <- j()
	}
}

func (nds *NetDataStore) GetObjectMetadata(ctx context.Context, objectKey string) (*datastore.ObjectMetadata, bool, error) {
	return nds.ds.GetObjectMetadata(ctx, objectKey)
}

func (nds *NetDataStore) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	return nds.ds.GetObject(ctx, objectKey)
}

func (nds *NetDataStore) PutObject(ctx context.Context, objectKey, digest string, reader io.Reader) error {
	return nds.ds.PutObject(ctx, objectKey, digest, reader)
}

func (nds *NetDataStore) PutObjectWithTotalLength(ctx context.Context, objectKey, digest string, totalLength int64, reader io.Reader) error {
	return nds.ds.PutObjectWithTotalLength(ctx, objectKey, digest, totalLength, reader)
}

func (nds *NetDataStore) DeleteObject(ctx context.Context, objectKey string) error {
	return nds.ds.DeleteObject(ctx, objectKey)
}

func (nds *NetDataStore) DelUncompletedDirtyObject(ctx context.Context, objectKey string) error {
	return nds.ds.DelUncompletedDirtyObject(ctx, objectKey)
}

func (nds *NetDataStore) DeleteObjects(ctx context.Context, objects []*datastore.ObjectMetadata) error {
	return nds.ds.DeleteObjects(ctx, objects)
}

func (nds *NetDataStore) ListObjectMetadatas(ctx context.Context, prefix, marker string, limit int64) ([]*datastore.ObjectMetadata, error) {
	return nds.ds.ListObjectMetadatas(ctx, prefix, marker, limit)
}

func (nds *NetDataStore) ListFolderObjects(ctx context.Context, prefix string) ([]*datastore.ObjectMetadata, error) {
	return nds.ds.ListFolderObjects(ctx, prefix)
}

func (nds *NetDataStore) IsObjectExist(ctx context.Context, objectKey string, isFolder bool) (bool, error) {
	return nds.ds.IsObjectExist(ctx, objectKey, isFolder)
}

func (nds *NetDataStore) GetSignURL(ctx context.Context, objectKey string, method datastore.Method, expire time.Duration) (string, error) {
	return nds.ds.GetSignURL(ctx, objectKey, method, expire)
}

func (nds *NetDataStore) CreateFolder(ctx context.Context, folderName string, isEmptyFolder bool) error {
	return nds.ds.CreateFolder(ctx, folderName, isEmptyFolder)
}

func (nds *NetDataStore) GetFolderMetadata(ctx context.Context, folderKey string) (*datastore.ObjectMetadata, bool, error) {
	return nds.ds.GetFolderMetadata(ctx, folderKey)
}
