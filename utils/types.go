package utils

const (
	Extension = ".data"
)

var RedisClusterIP = []string{"192.168.242.28:6379"}
var RedisClusterPwd = "dragonfly"

type DataStoreConfig struct {
	Name          string
	Region        string
	Endpoint      string
	AccessKey     string
	SecretKey     string
	Bucket        string
	RootDirectory string
	Workers       int
}

type WithDataStoreOption interface {
	WithRegion(region string) WithOption
	WithEndpoint(endpoint string) WithOption
	WithAccessKey(accessKey string) WithOption
	WithSecretKey(secretKey string) WithOption
	WithBucket(bucket string) WithOption
	WithRootDirectory(rootDirectory string) WithOption
	WithWorkers(workers int) WithOption
}

type WithOption func(options *DataStoreConfig) error
