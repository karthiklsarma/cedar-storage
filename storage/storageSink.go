package storage

import (
	"github.com/karthiklsarma/cedar-schema/gen"
)

type IStorageSink interface {
	Connect() error
	Authenticate(username, password string) (bool, error)
	TestConnect(contact_point, cassandra_port, cassandra_user, cassandra_password string) error
	InsertLocation(location *gen.Location) (bool, error)
	InsertUser(user *gen.User) (bool, error)
}
