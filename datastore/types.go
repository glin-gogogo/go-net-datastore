package datastore

import "time"

type Method string

const (
	MetaDigest            = "digest"
	MetaDigestUpper       = "Digest"
	MaxFolderDepth        = 1024
	MaxFolderListPageSize = 65536

	MaxInstanceNum = 1
	MinInstanceNum = 1
)

const (
	MethodHead   Method = "HEAD"
	MethodGet    Method = "GET"
	MethodPut    Method = "PUT"
	MethodPost   Method = "POST"
	MethodDelete Method = "Delete"
	MethodList   Method = "List"
)

type ObjectMetadata struct {
	Key                string
	ContentDisposition string
	ContentEncoding    string
	ContentLanguage    string
	ContentLength      int64
	ContentType        string
	ETag               string
	Digest             string
}

type BucketMetadata struct {
	Name     string
	CreateAt time.Time
}
