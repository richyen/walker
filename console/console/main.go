package main

import (
	"fmt"
	"net/http"
	"time"

	"code.google.com/p/log4go"
	"github.com/codegangsta/negroni"
	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
	"github.com/iParadigms/walker"
	"github.com/iParadigms/walker/console"
)

func spoofDs() (ds *console.CqlDataStore) {
	walker.Config.Cassandra.Keyspace = "walker_test"
	walker.Config.Cassandra.Hosts = []string{"localhost"}
	walker.Config.Cassandra.ReplicationFactor = 1

	err := walker.CreateCassandraSchema()
	if err != nil {
		panic(err)
	}

	ds, err = console.NewCqlDataStore()
	if err != nil {
		panic(fmt.Errorf("Failed to start data source: %v", err))
	}
	db := ds.Db

	//
	// Clear out the tables first
	//
	tables := []string{"links", "segments", "domain_info", "domains_to_crawl"}
	for _, table := range tables {
		err := db.Query(fmt.Sprintf(`TRUNCATE %v`, table)).Exec()
		if err != nil {
			panic(fmt.Errorf("Failed to truncate table %v: %v", table, err))
		}
	}

	insertDomainInfo := `INSERT INTO domain_info (domain, excluded, exclude_reason, mirror_for) VALUES (?, ?, ?, ?)`
	insertDomainToCrawl := `INSERT INTO domains_to_crawl (domain, crawler_token, priority, claim_time) VALUES (?, ?, ?, ?)`
	insertSegment := `INSERT INTO segments (domain, subdomain, path, protocol) VALUES (?, ?, ?, ?)`
	insertLink := `INSERT INTO links (domain, subdomain, path, protocol, crawl_time, status, error, robots_excluded) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

	queries := []*gocql.Query{
		db.Query(insertDomainInfo, "test.com", false, "", ""),
		db.Query(insertLink, "test.com", "", "/page1.html", "http", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "test.com", "", "/page2.html", "http", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "test.com", "", "/page3.html", "http", walker.NotYetCrawled, 404, "", false),
		db.Query(insertLink, "test.com", "", "/page4.html", "http", walker.NotYetCrawled, 200, "An Error", false),
		db.Query(insertLink, "test.com", "", "/page5.html", "http", walker.NotYetCrawled, 200, "", true),

		db.Query(insertLink, "test.com", "sub", "/page6.html", "http", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "test.com", "sub", "/page7.html", "https", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "test.com", "sub", "/page8.html", "https", walker.NotYetCrawled, 200, "", false),

		db.Query(insertDomainToCrawl, "test.com", gocql.UUID{}, 0, time.Now()),
		db.Query(insertSegment, "test.com", "", "/page1.html", "http"),
		db.Query(insertSegment, "test.com", "", "/page2.html", "http"),

		db.Query(insertDomainInfo, "foo.com", false, "", ""),
		db.Query(insertLink, "foo.com", "sub", "/page1.html", "http", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "foo.com", "sub", "/page2.html", "http", walker.NotYetCrawled, 200, "", false),

		db.Query(insertDomainInfo, "bar.com", true, "Didn't like it", ""),

		db.Query(insertDomainInfo, "baz.com", false, "", ""),

		db.Query(insertDomainToCrawl, "baz.com", gocql.UUID{}, 0, time.Now()),
		db.Query(insertSegment, "baz.com", "sub", "page1.html", "http"),
	}

	for _, q := range queries {
		err := q.Exec()
		if err != nil {
			panic(fmt.Errorf("Failed to insert test data: %v\nQuery: %v", err, q))
		}
	}

	for i := 0; i < 100; i++ {
		domain := fmt.Sprintf("x%d.com", i)
		err := db.Query(insertDomainInfo, domain, false, "", "").Exec()
		if err != nil {
			panic(err)
		}
	}

	return
}

func main() {
	var ds *console.CqlDataStore
	if true {
		ds = spoofDs()
	} else {
		var err error
		ds, err = console.NewCqlDataStore()
		if err != nil {
			panic(fmt.Errorf("Failed to start data source: %v", err))
		}
	}

	console.DS = ds
	defer ds.Close()

	router := mux.NewRouter()
	routes := console.Routes()
	for _, route := range routes {
		log4go.Info("Registering path %s", route.Path)
		router.HandleFunc(route.Path, route.Handler)
	}

	n := negroni.New(negroni.NewRecovery(), negroni.NewLogger(), negroni.NewStatic(http.Dir("public")))
	n.UseHandler(router)
	n.Run(":3000")
}
