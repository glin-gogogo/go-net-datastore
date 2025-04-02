package utils

const (
	ServiceNameS3        = "s3"
	ServiceNameOSS       = "oss"
	ServiceNameOBS       = "obs"
	ServiceNameMINIO     = "minio"
	ServiceNameSUGON     = "sugon"
	ServiceNameSTARLIGHT = "starlight"
	ServiceNamePARACLOUD = "paracloud"
)

const (
	DefaultDataBucket    = "urchin-data"
	DefaultCacheBucket   = "urchin-cache"
	DefaultRootDirectory = "cads/blocks"
	MaxBatchWorkers      = 32
)

const (
	DefaultListMax      = 1000
	DefaultDeleteMax    = 1000
	DefaultBatchWorkers = 100
	BlocksContentType   = "binary/octet-stream"
	BlocksContentLength = 262158
)
