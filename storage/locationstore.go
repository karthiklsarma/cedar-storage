package storage

import (
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/karthiklsarma/cedar-logging/logging"
	"github.com/karthiklsarma/cedar-schema/gen"
)

const (
	COSMOSDB_CONTACT_POINT = "COSMOSDB_CONTACT_POINT"
	COSMOSDB_PORT          = "COSMOSDB_PORT"
	COSMOSDB_USER          = "COSMOSDB_USER"
	COSMOSDB_PASSWORD      = "COSMOSDB_PASSWORD"
)

const INSERT_LOCATION_QUERY = "INSERT INTO cedarcosmoskeyspace.cedarlocation (id, lat, lng, timestamp, device) VALUES (?, ?, ?, ?, ?)"
const INSERT_USER_QUERY = "INSERT INTO cedarcosmoskeyspace.cedarusers (id, creationtime, username, firstname, lastname, password, email, phone) VALUES (?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS"
const AUTHENTICATION_QUERY = "SELECT password FROM cedarcosmoskeyspace.cedarusers WHERE username = ?"

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

func (sink *CosmosSink) TestConnect(contact_point, cassandra_port, cassandra_user, cassandra_password string) error {
	sink.contact_point = contact_point
	sink.cassandra_port = cassandra_port
	sink.cassandra_user = cassandra_user
	sink.cassandra_password = cassandra_password

	logging.Debug(
		fmt.Sprintf(
			"Test Connecting to Cosmos DB with contact point: [%v], port [%v], user: [%v]", sink.contact_point, sink.cassandra_port, sink.cassandra_user))

	if sink.cosmos_session == nil || sink.cosmos_session.Closed() {
		var err error
		sink.cosmos_session, err = getSession(sink.contact_point, sink.cassandra_port, sink.cassandra_user, sink.cassandra_password)
		if err != nil {
			logging.Fatal(fmt.Sprintf("error fetching cosmos test session: %v", err))
			return err
		}
	}

	return nil
}

func (sink *CosmosSink) Authenticate(username, password string) (bool, error) {
	if sink.cosmos_session == nil {
		logging.Error("Please connect before inserting location.")
		return false, fmt.Errorf("Please connect before inserting location.")
	}

	var dbpassword string
	err := sink.cosmos_session.Query(AUTHENTICATION_QUERY).Bind(username).Consistency(gocql.One).Scan(&dbpassword)
	if err != nil {
		logging.Error(fmt.Sprintf("Authentication failed: %v", err))
		return false, err
	}

	return password == dbpassword, nil
}

func (sink *CosmosSink) InsertLocation(location *gen.Location) (bool, error) {
	if sink.cosmos_session == nil {
		logging.Error("Please connect before inserting location.")
		return false, fmt.Errorf("Please connect before inserting location.")
	}
	err := sink.cosmos_session.Query(INSERT_LOCATION_QUERY).Bind(location.Id, location.Lat, location.Lng, location.Timestamp, location.Device).Exec()
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to insert location into location table: %v", err))
		return false, err
	}

	logging.Info("successfully inserted location into location table.")
	return true, nil
}

func (sink *CosmosSink) InsertUser(user *gen.User) (bool, error) {
	if sink.cosmos_session == nil {
		logging.Error("Please connect before inserting user.")
		return false, fmt.Errorf("Please connect before inserting user.")
	}
	err := sink.cosmos_session.Query(INSERT_USER_QUERY).Bind(
		uuid.New().String(), time.Now().UTC().Unix(), user.Username, user.Firstname, user.Lastname, user.Password, user.Email, user.Phone).Exec()
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to insert user %v into user table: %v", user.Username, err))
		return false, err
	}

	logging.Info(fmt.Sprintf("Successfully inserted user %v into user table.", user.Username))
	return true, nil
}

func getSession(contactPoint, cassandraPort, user, password string) (*gocql.Session, error) {
	clusterConfig := gocql.NewCluster(contactPoint)
	port, err := strconv.Atoi(cassandraPort)
	if err != nil {
		logging.Error(fmt.Sprintf("Couldn't convert cosmos cassandra port to int, err: %v", err))
		return nil, err
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
