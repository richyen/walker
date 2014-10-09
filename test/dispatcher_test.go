// +build cassandra

package test

import (
	"net/url"
	"reflect"

	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
)

func TestDispatcherBasic(t *testing.T) {
	db := getDB(t)

	insertDomainInfo := `INSERT INTO domain_info (dom, claim_tok, priority, dispatched)
							VALUES (?, ?, ?, ?)`
	insertLinkStatus := `INSERT INTO links (dom, subdom, path, proto, time, stat)
							VALUES (?, ?, ?, ?, ?, ?)`
	insertLink := `INSERT INTO links (dom, subdom, path, proto, time)
						VALUES (?, ?, ?, ?, ?)`

	queries := []*gocql.Query{
		db.Query(insertDomainInfo, "test.com", gocql.UUID{}, 0, true),
		db.Query(insertLink, "test.com", "", "page1.html", "http", walker.NotYetCrawled),
		db.Query(insertLink, "test.com", "", "page2.html", "http", walker.NotYetCrawled),
		db.Query(insertLink, "test.com", "", "page404.html", "http", walker.NotYetCrawled),
		db.Query(insertLink, "test.com", "", "page500.html", "http", walker.NotYetCrawled),
		db.Query(insertLink, "test.com", "", "notcrawled1.html", "http", walker.NotYetCrawled),
		db.Query(insertLink, "test.com", "", "notcrawled2.html", "http", walker.NotYetCrawled),
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

	d := &walker.CassandraDispatcher{}
	go d.StartDispatcher()
	time.Sleep(time.Second)
	d.StopDispatcher()

	url1 := parse("http://test.com/notcrawled1.html")
	url2 := parse("http://test.com/notcrawled2.html")
	expectedResults := map[url.URL]bool{
		*url1.URL: true,
		*url2.URL: true,
	}
	results := map[url.URL]bool{}
	iter := db.Query(`SELECT dom, subdom, path, proto
						FROM segments WHERE dom = 'test.com'`).Iter()
	var linkdomain, subdomain, path, protocol string
	for iter.Scan(&linkdomain, &subdomain, &path, &protocol) {
		u, _ := walker.CreateURL(linkdomain, subdomain, path, protocol, walker.NotYetCrawled)
		results[*u.URL] = true
	}
	if !reflect.DeepEqual(results, expectedResults) {
		t.Errorf("Expected results in segments: %v\nBut got: %v",
			expectedResults, results)
	}
}
