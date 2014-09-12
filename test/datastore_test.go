package test

import (
	"fmt"
	"sync"

	"testing"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
)

var initdb sync.Once

// getDB ensures a walker database is set up and empty, returning a db session
func getDB(t *testing.T) (*gocql.Session, *gocql.ClusterConfig) {
	initdb.Do(func() {
		//TODO: load the schema
	})

	if walker.Config.Cassandra.Keyspace != "walker_test" {
		t.Fatal("Running tests requires using the walker_test keyspace")
		return nil, nil
	}
	config := gocql.NewCluster(walker.Config.Cassandra.Hosts[0])
	config.Keyspace = walker.Config.Cassandra.Keyspace
	db, err := config.CreateSession()
	if err != nil {
		t.Fatalf("Could not connect to local cassandra db: %v", err)
		return nil, nil
	}

	tables := []string{"links", "segments", "domain_info", "domains_to_crawl"}
	for _, table := range tables {
		err := db.Query(fmt.Sprintf(`TRUNCATE %v`, table)).Exec()
		if err != nil {
			t.Fatalf("Failed to truncate table %v: %v", table, err)
		}
	}

	return db, config
}

func TestClaimsNewHost(t *testing.T) {
	db, config := getDB(t)

	expectedHost := "iparadigms.com"
	err := db.Query(
		`INSERT INTO domains_to_crawl (domain, crawler_token, priority)
			VALUES (?, ?, ?)`,
		expectedHost,
		gocql.UUID{},
		0,
	).Exec()
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	ds, err := walker.NewCassandraDatastore(config)
	if err != nil {
		t.Fatalf("Failed to create CassandraDatastore: %v", err)
	}
	host := ds.ClaimNewHost()
	if host != expectedHost {
		t.Errorf("Expected %v but got %v", expectedHost, host)
	}
}

func TestUnclaimsHost(t *testing.T) {
	db, config := getDB(t)

	expectedHost := "iparadigms.com"
	err := db.Query(
		`INSERT INTO domains_to_crawl (domain, crawler_token, priority)
			VALUES (?, ?, ?)`,
		expectedHost,
		gocql.UUID{},
		0,
	).Exec()
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	q := `INSERT INTO segments (domain, subdomain, path, protocol)
			VALUES (?, ?, ?, ?)`
	db.Query(q, expectedHost, "", "page1.html", "http").Exec()
	db.Query(q, expectedHost, "", "page2.html", "http").Exec()

	ds, err := walker.NewCassandraDatastore(config)
	if err != nil {
		t.Fatalf("Failed to create CassandraDatastore: %v", err)
	}
	ds.UnclaimHost(expectedHost)

	var count int
	db.Query(`SELECT COUNT(*) FROM segments WHERE domain = ?`, expectedHost).Scan(&count)
	if count != 0 {
		t.Error("Expected links from unclaimed domain to be deleted, found %v", count)
	}

	db.Query(`SELECT COUNT(*) FROM domains_to_crawl WHERE priority = ? AND domain = ?`,
		0, expectedHost).Scan(&count)
	if count != 0 {
		t.Error("Expected unclaimed domain to be deleted from domains_to_crawl")
	}
}
