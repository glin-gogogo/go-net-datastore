package utils

import (
	"errors"
)

var (
	ErrQueryBadData = errors.New("bad data")
	ErrInvalidKey   = errors.New("key not supported by netds")
)
