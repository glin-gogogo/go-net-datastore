package netds

import (
	"fmt"
	"github.com/glin-gogogo/go-net-datastore/datastore"
	"github.com/glin-gogogo/go-net-datastore/utils"
)

func WithOption(cfg utils.DataStoreConfig, dsOption utils.WithDataStoreOption) []utils.WithOption {
	withOpts := []utils.WithOption{dsOption.WithRegion(cfg.Region)}
	withOpts = append(withOpts, dsOption.WithEndpoint(cfg.Endpoint))
	withOpts = append(withOpts, dsOption.WithAccessKey(cfg.AccessKey))
	withOpts = append(withOpts, dsOption.WithSecretKey(cfg.SecretKey))
	withOpts = append(withOpts, dsOption.WithBucket(cfg.Bucket))
	withOpts = append(withOpts, dsOption.WithRootDirectory(cfg.RootDirectory))
	withOpts = append(withOpts, dsOption.WithWorkers(cfg.Workers))

	return withOpts
}

func New(cfg utils.DataStoreConfig) (datastore.DataStorage, error) {
	switch cfg.Name {
	//case ServiceNameS3:
	//	return newS3(region, endpoint, accessKey, secretKey)
	//case ServiceNameOSS:
	//	return newOSS(region, endpoint, accessKey, secretKey)
	case utils.ServiceNameOBS:
		opts := WithOption(cfg, &datastore.WithObsOption{})
		return datastore.NewOBS(opts...)
	case utils.ServiceNameMINIO:
		opts := WithOption(cfg, &datastore.WithMinioOption{})
		return datastore.NewMinio(opts...)
	case utils.ServiceNameSUGON:
		opts := WithOption(cfg, &datastore.WithMinioOption{})
		return datastore.NewSugon(opts...)
	//case utils.ServiceNameSTARLIGHT:
	//	return newStarlight(region, endpoint, accessKey, secretKey)
	case utils.ServiceNamePARACLOUD:
		opts := WithOption(cfg, &datastore.WithMinioOption{})
		return datastore.NewParaCloud(opts...)
	}

	return nil, fmt.Errorf("unknow service name %s", cfg.Name)
}
