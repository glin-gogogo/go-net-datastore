package utils

import (
	"context"
	"math"
	"math/rand"
	"strings"
	"time"
)

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

func NeedRetry(err error) bool {
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "context deadline exceeded") ||
		strings.Contains(errMsg, "connection failed") ||
		strings.Contains(errMsg, "timeout") ||
		strings.Contains(errMsg, "server misbehaving") ||
		strings.Contains(errMsg, "no such host") ||
		strings.Contains(errMsg, "eof")
}

func RetryRun(ctx context.Context,
	initBackoff float64,
	maxBackoff float64,
	maxAttempts int,
	f func() (data any, cancel bool, err error)) (any, bool, error) {
	var (
		res    any
		cancel bool
		cause  error
	)
	for i := 0; i < maxAttempts; i++ {
		if i > 0 {
			m := math.Max(initBackoff, rand.Float64()*math.Min(math.Pow(2.0, float64(i))*initBackoff, maxBackoff))
			time.Sleep(time.Duration(m * float64(time.Second)))
		}

		res, cancel, cause = f()
		if cause == nil || cancel {
			break
		}
		select {
		case <-ctx.Done():
			return nil, cancel, ctx.Err()
		default:
		}
	}

	return res, cancel, cause
}
