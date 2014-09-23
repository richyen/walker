// +build cassandra

package test

import (
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"sync"

	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
)

var initdb sync.Once

// getDB ensures a walker database is set up and empty, returning a db session
func getDB(t *testing.T) *gocql.Session {
	initdb.Do(func() {
		err := walker.CreateCassandraSchema()
		if err != nil {
			t.Fatalf(err.Error())
		}
	})

	if walker.Config.Cassandra.Keyspace != "walker_test" {
		t.Fatal("Running tests requires using the walker_test keyspace")
		return nil
	}
	config := walker.GetCassandraConfig()
	db, err := config.CreateSession()
	if err != nil {
		t.Fatalf("Could not connect to local cassandra db: %v", err)
		return nil
	}

	tables := []string{"links", "segments", "domain_info", "domains_to_crawl"}
	for _, table := range tables {
		err := db.Query(fmt.Sprintf(`TRUNCATE %v`, table)).Exec()
		if err != nil {
			t.Fatalf("Failed to truncate table %v: %v", table, err)
		}
	}

	return db
}

var page1URL *walker.URL
var page1Fetch *walker.FetchResults
var page2URL *walker.URL
var page2Fetch *walker.FetchResults

func init() {
	page1URL = parse("http://test.com/page1.html")
	page1Fetch = &walker.FetchResults{
		URL:       page1URL,
		FetchTime: time.Now(),
		Response: &http.Response{
			Status:        "200 OK",
			StatusCode:    200,
			Proto:         "HTTP/1.1",
			ProtoMajor:    1,
			ProtoMinor:    1,
			ContentLength: 18,
			Request: &http.Request{
				Method:        "GET",
				URL:           page1URL.URL,
				Proto:         "HTTP/1.1",
				ProtoMajor:    1,
				ProtoMinor:    1,
				ContentLength: 18,
				Host:          "test.com",
			},
		},
	}
	page2URL = parse("http://test.com/page2.html")
	page2Fetch = &walker.FetchResults{
		URL:       page2URL,
		FetchTime: time.Now(),
		Response: &http.Response{
			Status:        "200 OK",
			StatusCode:    200,
			Proto:         "HTTP/1.1",
			ProtoMajor:    1,
			ProtoMinor:    1,
			ContentLength: 18,
			Request: &http.Request{
				Method:        "GET",
				URL:           page2URL.URL,
				Proto:         "HTTP/1.1",
				ProtoMajor:    1,
				ProtoMinor:    1,
				ContentLength: 18,
				Host:          "test.com",
			},
		},
	}

}

func TestDatastoreBasic(t *testing.T) {
	db := getDB(t)

	ds, err := walker.NewCassandraDatastore()
	if err != nil {
		t.Fatalf("Failed to create CassandraDatastore: %v", err)
	}

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
	}
	for _, q := range queries {
		err := q.Exec()
		if err != nil {
			t.Fatalf("Failed to insert test data: %v\nQuery: %v", err, q)
		}
	}

	host := ds.ClaimNewHost()
	if host != "test.com" {
		t.Errorf("Expected test.com but got %v", host)
	}

	links := map[url.URL]bool{}
	expectedLinks := map[url.URL]bool{
		*page1URL.URL: true,
		*page2URL.URL: true,
	}
	for u := range ds.LinksForHost("test.com") {
		links[*u.URL] = true
	}
	if !reflect.DeepEqual(links, expectedLinks) {
		t.Errorf("Expected links from LinksForHost: %v\nBut got: %v", expectedLinks, links)
	}

	ds.StoreURLFetchResults(page1Fetch)
	ds.StoreURLFetchResults(page2Fetch)

	expectedResults := map[url.URL]int{
		*page1URL.URL: 200,
		*page2URL.URL: 200,
	}
	iter := db.Query(`SELECT domain, subdomain, path, protocol, crawl_time, status
						FROM links WHERE domain = 'test.com'`).Iter()
	var linkdomain, subdomain, path, protocol string
	var status int
	var crawl_time time.Time
	results := map[url.URL]int{}
	for iter.Scan(&linkdomain, &subdomain, &path, &protocol, &crawl_time, &status) {
		if !crawl_time.Equal(walker.NotYetCrawled) {
			u, _ := walker.CreateURL(linkdomain, subdomain, path, protocol, crawl_time)
			results[*u.URL] = status
		}
	}
	if !reflect.DeepEqual(results, expectedResults) {
		t.Errorf("Expected results from StoreURLFetchResults: %v\nBut got: %v",
			expectedResults, results)
	}

	ds.StoreParsedURL(parse("http://test2.com/page1-1.html"), page1Fetch)
	ds.StoreParsedURL(parse("http://test2.com/page2-1.html"), page2Fetch)

	var count int
	db.Query(`SELECT COUNT(*) FROM links WHERE domain = 'test2.com'`).Scan(&count)
	if count != 2 {
		t.Errorf("Expected 2 parsed links to be inserted for test2.com, found %v", count)
	}

	ds.UnclaimHost("test.com")

	db.Query(`SELECT COUNT(*) FROM segments WHERE domain = 'test.com'`).Scan(&count)
	if count != 0 {
		t.Errorf("Expected links from unclaimed domain to be deleted, found %v", count)
	}

	db.Query(`SELECT COUNT(*) FROM domains_to_crawl
				WHERE priority = ? AND domain = 'test.com'`, 0).Scan(&count)
	if count != 0 {
		t.Errorf("Expected unclaimed domain to be deleted from domains_to_crawl")
	}
}

func TestNewDomainAdditions(t *testing.T) {
	db := getDB(t)

	ds, err := walker.NewCassandraDatastore()
	if err != nil {
		t.Fatalf("Failed to create CassandraDatastore: %v", err)
	}

	origAddNewDomains := walker.Config.AddNewDomains
	defer func() { walker.Config.AddNewDomains = origAddNewDomains }()

	walker.Config.AddNewDomains = false
	ds.StoreParsedURL(parse("http://test.com/page1-1.html"), page1Fetch)

	var count int
	db.Query(`SELECT COUNT(*) FROM domain_info WHERE domain = 'test.com'`).Scan(&count)
	if count != 0 {
		t.Error("Expected test.com not to be added to domain_info")
	}

	walker.Config.AddNewDomains = true
	ds.StoreParsedURL(parse("http://test.com/page1-1.html"), page1Fetch)

	db.Query(`SELECT COUNT(*) FROM domain_info WHERE domain = 'test.com'`).Scan(&count)
	if count != 1 {
		t.Error("Expected test.com to be added to domain_info")
	}

	db.Query(`DELETE FROM domain_info WHERE domain = 'test.com'`).Exec()
	ds.StoreParsedURL(parse("http://test.com/page1-1.html"), page1Fetch)
	db.Query(`SELECT COUNT(*) FROM domain_info WHERE domain = 'test.com'`).Scan(&count)
	if count != 0 {
		t.Error("Expected test.com not to be added to domain_info due to cache")
	}
}

func TestWalkerURL(t *testing.T) {
	url1, err := url.Parse("http://sub1.test.com/thepath?query=blah")
	if err != nil {
		t.Fatal(err)
	}
	wurl1, err := walker.ParseURL("http://sub1.test.com/thepath?query=blah")
	if err != nil {
		t.Fatal(err)
	}

	if url1.String() != wurl1.String() {
		t.Errorf("URLs should be the same: %v\nAnd: %v")
	}
	tld := wurl1.ToplevelDomainPlusOne()
	expectedtld := "test.com"
	if tld != expectedtld {
		t.Errorf("Expected ToplevelDomainPlusOne to be %v\nBut got: %v", expectedtld, tld)
	}
	sub := wurl1.Subdomain()
	expectedsub := "sub1"
	if sub != expectedsub {
		t.Errorf("Expected ToplevelDomainPlusOne to be %v\nBut got: %v", expectedsub, sub)
	}

	created, err := walker.CreateURL("test.com", "sub1", "thepath?query=blah", "http",
		walker.NotYetCrawled)
	if err != nil {
		t.Fatal(err)
	}
	if created.String() != wurl1.String() {
		t.Errorf("Expected CreateURL to return %v\nBut got: %v", wurl1, created)
	}
}
