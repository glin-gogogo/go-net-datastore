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
	DefaultRootDirectory = "datastore/"
	MaxBatchWorkers      = 100
)

const (
	DefaultListMax      = 1000
	DefaultDeleteMax    = 1000
	DefaultBatchWorkers = 100
)
