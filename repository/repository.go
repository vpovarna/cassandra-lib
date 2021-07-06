package repository

import (
	"github.com/gocql/gocql"
	"github.com/vpovarna/logging-lib/logger"
	"os"
	"strconv"
	"time"
)

func GetDbClient() *gocql.Session {
	cluster := gocql.NewCluster(os.Getenv("CASSANDRA_HOSTS"))
	cluster.Port = port(os.Getenv("CASSANDRA_PORT"))
	cluster.Keyspace = os.Getenv("CASSANDRA_KEYSPACE_NAME")
	cluster.Consistency = consistency(os.Getenv("CASSANDRA_KEYSPACE_CONSISTENCY"))
	cluster.ProtoVersion = 3
	cluster.ConnectTimeout = time.Second * 100

	s, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	logger.Info("Successfully connecting to Cassandra cluster!")
	return s
}

func ClearSession(s *gocql.Session) {
	logger.Info("Closing Cassandra session")
	s.Close()
}

func consistency(c string) gocql.Consistency {
	return gocql.ParseConsistency(c)
}

func port(port string) int {
	p, err := strconv.Atoi(port)
	if err != nil {
		return 9092
	}

	return p
}
