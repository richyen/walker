package test

import (
	"fmt"
	"sync"

	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
	"github.com/iParadigms/walker/console"
)

var initdb sync.Once

func getDs(t *testing.T) *console.CqlDataStore {
	//XXX: More elegant way to do this?
	walker.Config.Cassandra.Keyspace = "walker_test"
	walker.Config.Cassandra.Hosts = []string{"localhost"}

	// initdb.Do(func() {
	// 	err := walker.CreateCassandraSchema()
	// 	if err != nil {
	// 		t.Fatalf(err.Error())
	// 	}
	// })

	ds, err := console.NewCqlDataStore()
	if err != nil {
		panic(err)
	}

	//
	ds.Db.SetConsistency(gocql.One)

	return ds
}

func populate(t *testing.T, ds *console.CqlDataStore) {
	db := ds.Db

	//
	// Clear out the tables first
	//
	tables := []string{"links", "segments", "domain_info", "domains_to_crawl"}
	for _, table := range tables {
		err := db.Query(fmt.Sprintf(`TRUNCATE %v`, table)).Exec()
		if err != nil {
			t.Fatalf("Failed to truncate table %v: %v", table, err)
		}
	}

	//
	// Insert some data
	//
	insertDomainToCrawl := `INSERT INTO domains_to_crawl (domain, crawler_token, priority)
                                VALUES (?, ?, ?)`
	insertSegment := `INSERT INTO segments (domain, subdomain, path, protocol)
                        VALUES (?, ?, ?, ?)`
	insertLink := `INSERT INTO links (domain, subdomain, path, protocol, crawl_time)
                        VALUES (?, ?, ?, ?, ?)`

	queries := []*gocql.Query{
		db.Query(insertDomainToCrawl, "test.com", gocql.UUID{}, 0),
		db.Query(insertSegment, "test.com", "", "page1.html", "http"),
		db.Query(insertSegment, "test.com", "", "page2.html", "http"),

		db.Query(insertLink, "test.com", "", "page1.html", "http", walker.NotYetCrawled),
		db.Query(insertLink, "test.com", "", "page2.html", "http", walker.NotYetCrawled),

		db.Query(insertLink, "foo.com", "", "page1.html", "http", time.Now().AddDate(0, 0, -1)),
		db.Query(insertLink, "foo.com", "", "page2.html", "http", time.Now().AddDate(0, 0, -1)),
	}
	for _, q := range queries {
		err := q.Exec()
		if err != nil {
			t.Fatalf("Failed to insert test data: %v\nQuery: %v", err, q)
		}
	}
}

/*
type DomainInfo struct {
    Domain            string
    ExcludeReason     string
    TimeQueued        time.Time
    UuidOfQueued      string
    NumberLinksTotal  int
    NumberLinksQueued int
}
*/

func TestSomething(t *testing.T) {
	store := getDs(t)
	populate(t, store)
	dinfos, err := store.ListDomains("f", 50)
	if err != nil {
		t.Errorf("ListDomains error: %v", err)
	}
	for _, d := range dinfos {
		fmt.Printf(">> %v\n", d)
	}
	t.Fail()
}
