package storage

import (
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/karthiklsarma/cedar-logging/logging"
	"github.com/karthiklsarma/cedar-schema/gen"
)

const (
	COSMOSDB_CONTACT_POINT = "COSMOSDB_CONTACT_POINT"
	COSMOSDB_PORT          = "COSMOSDB_PORT"
	COSMOSDB_USER          = "COSMOSDB_USER"
	COSMOSDB_PASSWORD      = "COSMOSDB_PASSWORD"
)

const INSERT_QUERY = "INSERT INTO cedarcosmoskeyspace.cedarlocation (id, lat, lng, timestamp, device) VALUES (?, ?, ?, ?, ?)"

type CosmosSink struct {
	contact_point      string
	cassandra_port     string
	cassandra_user     string
	cassandra_password string
	cosmos_session     *gocql.Session
}

func (sink *CosmosSink) Connect() error {
	sink.contact_point = os.Getenv(COSMOSDB_CONTACT_POINT)
	sink.cassandra_port = os.Getenv(COSMOSDB_PORT)
	sink.cassandra_user = os.Getenv(COSMOSDB_USER)
	sink.cassandra_password = os.Getenv(COSMOSDB_PASSWORD)

	logging.Debug(
		fmt.Sprintf(
			"Connecting to Cosmos DB with contact point: [%v], port [%v], user: [%v]", sink.contact_point, sink.cassandra_port, sink.cassandra_user))

	if sink.cosmos_session == nil || sink.cosmos_session.Closed() {
		var err error
		sink.cosmos_session, err = getSession(sink.contact_point, sink.cassandra_port, sink.cassandra_user, sink.cassandra_password)
		if err != nil {
			logging.Fatal(fmt.Sprintf("error fetching cosmos session: %v", err))
			return err
		}
	}

	return nil
}

func (sink *CosmosSink) InsertLocation(location *gen.Location) (bool, error) {
	if sink.cosmos_session == nil {
		logging.Fatal("Please connect before inserting location.")
		return false, fmt.Errorf("Please connect before inserting location.")
	}
	err := sink.cosmos_session.Query(INSERT_QUERY).Bind(location.Id, location.Lat, location.Lng, location.Timestamp, location.Device).Exec()
	if err != nil {
		logging.Fatal(fmt.Sprintf("Failed to insert location into location table: %v", err))
		return false, err
	}

	logging.Info("successfully inserted location into location table.")
	return true, nil
}

func getSession(contactPoint, cassandraPort, user, password string) (*gocql.Session, error) {
	clusterConfig := gocql.NewCluster(contactPoint)
	port, err := strconv.Atoi(cassandraPort)
	if err != nil {
		logging.Fatal(fmt.Sprintf("Couldn't convert cosmos cassandra port to int, err: %v", err))
	}
	clusterConfig.Port = port
	clusterConfig.ProtoVersion = 4
	clusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: user, Password: password}
	clusterConfig.SslOpts = &gocql.SslOptions{Config: &tls.Config{InsecureSkipVerify: true}}
	clusterConfig.ConnectTimeout = 10 * time.Second
	clusterConfig.Timeout = 10 * time.Second
	clusterConfig.DisableInitialHostLookup = true

	session, err := clusterConfig.CreateSession()
	if err != nil {
		logging.Fatal(fmt.Sprintf("Failed to connect to Azure Cosmos DB, err : %v", err))
		return nil, err
	}

	return session, nil
}
