package storage

import (
	"github.com/karthiklsarma/cedar-schema/gen"
)

type IStorageSink interface {
	Connect() error
	InsertLocation(location *gen.Location) (bool, error)
}
