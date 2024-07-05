package utils

import (
	"github.com/ipfs/go-datastore"
	"strings"
)

func KeyIsValid(key datastore.Key) bool {
	ks := key.String()
	if len(ks) < 2 || ks[0] != '/' {
		return false
	}
	for _, b := range ks[1:] {
		if '0' <= b && b <= '9' {
			continue
		}
		if 'A' <= b && b <= 'Z' {
			continue
		}
		switch b {
		case '+', '-', '_', '=':
			continue
		}
		return false
	}
	return true
}

func Decode(file string) (key datastore.Key, ok bool) {
	if !strings.HasSuffix(file, Extension) {
		return datastore.Key{}, false
	}
	name := file[:len(file)-len(Extension)]
	return datastore.NewKey(name), true
}
