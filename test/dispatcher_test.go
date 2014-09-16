// +build cassandra

package test

import (
	"reflect"

	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
)

func TestDispatcherBasic(t *testing.T) {
	db := getDB(t)

	insertDomainInfo := `INSERT INTO domain_info (domain) VALUES (?)`
	insertLinkStatus := `INSERT INTO links (domain, subdomain, path, protocol, crawl_time, status)
							VALUES (?, ?, ?, ?, ?, ?)`
	insertLink := `INSERT INTO links (domain, subdomain, path, protocol, crawl_time)
						VALUES (?, ?, ?, ?, ?)`

	queries := []*gocql.Query{
		db.Query(insertDomainInfo, "test.com"),
		db.Query(insertLink, "test.com", "", "page1.html", "http", time.Unix(0, 0)),
		db.Query(insertLink, "test.com", "", "page2.html", "http", time.Unix(0, 0)),
		db.Query(insertLink, "test.com", "", "page404.html", "http", time.Unix(0, 0)),
		db.Query(insertLink, "test.com", "", "page500.html", "http", time.Unix(0, 0)),
		db.Query(insertLink, "test.com", "", "notcrawled1.html", "http", time.Unix(0, 0)),
		db.Query(insertLink, "test.com", "", "notcrawled2.html", "http", time.Unix(0, 0)),
		db.Query(insertLinkStatus, "test.com", "", "page1.html", "http", time.Now(), 200),
		db.Query(insertLinkStatus, "test.com", "", "page2.html", "http", time.Now(), 200),
		db.Query(insertLinkStatus, "test.com", "", "page404.html", "http", time.Now(), 404),
		db.Query(insertLinkStatus, "test.com", "", "page500.html", "http", time.Now(), 500),
	}
	for _, q := range queries {
		err := q.Exec()
		if err != nil {
			t.Fatalf("Failed to insert test data: %v\nQuery: %v", err, q)
		}
	}

	d := &walker.Dispatcher{}
	done := make(chan bool)
	go func() {
		d.Start()
		done <- true
	}()
	time.Sleep(time.Second)
	d.Stop()
	<-done

	expectedResults := map[walker.CassandraLink]bool{
		walker.CassandraLink{
			Domain:    "test.com",
			Subdomain: "",
			Path:      "notcrawled1.html",
			Protocol:  "http",
		}: true,
		walker.CassandraLink{
			Domain:    "test.com",
			Subdomain: "",
			Path:      "notcrawled2.html",
			Protocol:  "http",
		}: true,
	}
	results := map[walker.CassandraLink]bool{}
	iter := db.Query(`SELECT domain, subdomain, path, protocol
						FROM segments WHERE domain = 'test.com'`).Iter()
	var linkdomain, subdomain, path, protocol string
	for iter.Scan(&linkdomain, &subdomain, &path, &protocol) {
		results[walker.CassandraLink{
			Domain:    linkdomain,
			Subdomain: subdomain,
			Path:      path,
			Protocol:  protocol,
		}] = true
	}
	if !reflect.DeepEqual(results, expectedResults) {
		t.Errorf("Expected results in segments: %v\nBut got: %v",
			expectedResults, results)
	}
}
