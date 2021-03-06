// +build cassandra

package test

import (
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
)

// getDS is a convenience function for getting a CassandraDatastore and failing
// if we couldn't
func getDS(t *testing.T) *walker.CassandraDatastore {
	ds, err := walker.NewCassandraDatastore()
	if err != nil {
		t.Fatalf("Failed to create CassandraDatastore: %v", err)
	}
	return ds
}

//TODO: test with query params

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
	ds := getDS(t)

	insertDomainInfo := `INSERT INTO domain_info (dom, claim_tok, priority, dispatched)
								VALUES (?, ?, ?, ?)`
	insertSegment := `INSERT INTO segments (dom, subdom, path, proto)
						VALUES (?, ?, ?, ?)`
	insertLink := `INSERT INTO links (dom, subdom, path, proto, time)
						VALUES (?, ?, ?, ?, ?)`

	queries := []*gocql.Query{
		db.Query(insertDomainInfo, "test.com", gocql.UUID{}, 0, true),
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
	iter := db.Query(`SELECT dom, subdom, path, proto, time, stat
						FROM links WHERE dom = 'test.com'`).Iter()
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
	db.Query(`SELECT COUNT(*) FROM links WHERE dom = 'test2.com'`).Scan(&count)
	if count != 2 {
		t.Errorf("Expected 2 parsed links to be inserted for test2.com, found %v", count)
	}

	ds.UnclaimHost("test.com")

	db.Query(`SELECT COUNT(*) FROM segments WHERE dom = 'test.com'`).Scan(&count)
	if count != 0 {
		t.Errorf("Expected links from unclaimed domain to be deleted, found %v", count)
	}

	err := db.Query(`SELECT COUNT(*) FROM domain_info
						WHERE dom = 'test.com'
						AND claim_tok = 00000000-0000-0000-0000-000000000000
						AND dispatched = false ALLOW FILTERING`).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query for test.com in domain_info: %v", err)
	}
	if count != 1 {
		t.Fatalf("test.com has incorrect values in domain_info after unclaim")
	}
}

func TestNewDomainAdditions(t *testing.T) {
	db := getDB(t)
	ds := getDS(t)

	origAddNewDomains := walker.Config.AddNewDomains
	defer func() { walker.Config.AddNewDomains = origAddNewDomains }()

	walker.Config.AddNewDomains = false
	ds.StoreParsedURL(parse("http://test.com/page1-1.html"), page1Fetch)

	var count int
	db.Query(`SELECT COUNT(*) FROM domain_info WHERE dom = 'test.com'`).Scan(&count)
	if count != 0 {
		t.Error("Expected test.com not to be added to domain_info")
	}

	walker.Config.AddNewDomains = true
	ds.StoreParsedURL(parse("http://test.com/page1-1.html"), page1Fetch)

	err := db.Query(`SELECT COUNT(*) FROM domain_info
						WHERE dom = 'test.com'
						AND claim_tok = 00000000-0000-0000-0000-000000000000
						AND dispatched = false
						AND priority = 0 ALLOW FILTERING`).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query for test.com in domain_info: %v", err)
	}
	if count != 1 {
		t.Fatalf("test.com not added to domain_info (possibly incorrect field values)")
	}

	db.Query(`DELETE FROM domain_info WHERE dom = 'test.com'`).Exec()
	ds.StoreParsedURL(parse("http://test.com/page1-1.html"), page1Fetch)
	db.Query(`SELECT COUNT(*) FROM domain_info WHERE dom = 'test.com'`).Scan(&count)
	if count != 0 {
		t.Error("Expected test.com not to be added to domain_info due to cache")
	}
}

type StoreURLExpectation struct {
	Input    *walker.FetchResults
	Expected *LinksExpectation
}

// The results we expect in the database for various fields. Non-primary
// keys are pointers so we can expect NULL for any of them
type LinksExpectation struct {
	Domain           string
	Subdomain        string
	Path             string
	Protocol         string
	CrawlTime        time.Time
	FetchError       string
	ExcludedByRobots bool
	Status           int
	MimeType         string
}

var StoreURLExpectations []StoreURLExpectation

func init() {
	StoreURLExpectations = []StoreURLExpectation{
		StoreURLExpectation{
			Input: &walker.FetchResults{
				URL:       parse("http://test.com/page1.html"),
				FetchTime: time.Unix(0, 0),
				Response: &http.Response{
					StatusCode: 200,
					Request: &http.Request{
						Host: "test.com",
					},
				},
				MimeType: "text/html; charset=ISO-8859-4",
			},
			Expected: &LinksExpectation{
				Domain:    "test.com",
				Path:      "/page1.html",
				Protocol:  "http",
				CrawlTime: time.Unix(0, 0),
				Status:    200,
				MimeType:  "text/html; charset=ISO-8859-4",
			},
		},
		StoreURLExpectation{
			Input: &walker.FetchResults{
				URL:       parse("http://test.com/page2.html?var1=abc&var2=def"),
				FetchTime: time.Unix(0, 0),
				Response: &http.Response{
					StatusCode: 200,
				},
				MimeType: "foo/bar",
			},
			Expected: &LinksExpectation{
				Domain:    "test.com",
				Path:      "/page2.html?var1=abc&var2=def",
				Protocol:  "http",
				CrawlTime: time.Unix(0, 0),
				Status:    200,
				MimeType:  "foo/bar",
			},
		},
		StoreURLExpectation{
			Input: &walker.FetchResults{
				URL:              parse("http://test.com/page3.html"),
				ExcludedByRobots: true,
			},
			Expected: &LinksExpectation{
				Domain:           "test.com",
				Path:             "/page3.html",
				Protocol:         "http",
				CrawlTime:        time.Unix(0, 0),
				ExcludedByRobots: true,
			},
		},
		StoreURLExpectation{
			Input: &walker.FetchResults{
				URL:       parse("http://test.com/page4.html"),
				FetchTime: time.Unix(1234, 5678),
				Response: &http.Response{
					StatusCode: 200,
				},
			},
			Expected: &LinksExpectation{
				Domain:    "test.com",
				Path:      "/page4.html",
				Protocol:  "http",
				CrawlTime: time.Unix(1234, 5678),
				Status:    200,
			},
		},
		StoreURLExpectation{
			Input: &walker.FetchResults{
				URL:       parse("https://test.com/page5.html"),
				FetchTime: time.Unix(0, 0),
				Response: &http.Response{
					StatusCode: 200,
				},
			},
			Expected: &LinksExpectation{
				Domain:    "test.com",
				Path:      "/page5.html",
				Protocol:  "https",
				CrawlTime: time.Unix(0, 0),
				Status:    200,
			},
		},
		StoreURLExpectation{
			Input: &walker.FetchResults{
				URL:       parse("https://sub.dom1.test.com/page5.html"),
				FetchTime: time.Unix(0, 0),
				Response: &http.Response{
					StatusCode: 200,
				},
			},
			Expected: &LinksExpectation{
				Domain:    "test.com",
				Subdomain: "sub.dom1",
				Path:      "/page5.html",
				Protocol:  "https",
				CrawlTime: time.Unix(0, 0),
				Status:    200,
			},
		},
	}
}

func TestStoreURLFetchResults(t *testing.T) {
	db := getDB(t)
	ds := getDS(t)

	for _, tcase := range StoreURLExpectations {
		ds.StoreURLFetchResults(tcase.Input)
		exp := tcase.Expected

		actual := &LinksExpectation{}
		err := db.Query(
			`SELECT err, robot_ex, stat, mime FROM links
			WHERE dom = ? AND subdom = ? AND path = ? AND proto = ?`, // AND time = ?`,
			exp.Domain,
			exp.Subdomain,
			exp.Path,
			exp.Protocol,
			//exp.CrawlTime,
		).Scan(&actual.FetchError, &actual.ExcludedByRobots, &actual.Status, &actual.MimeType)
		if err != nil {
			t.Errorf("Did not find row in links: %+v\nInput: %+v\nError: %v", exp, tcase.Input, err)
		}
		if exp.FetchError != actual.FetchError {
			t.Errorf("Expected err: %v\nBut got: %v\nFor input: %+v",
				exp.FetchError, actual.FetchError, tcase.Input)
		}
		if exp.ExcludedByRobots != actual.ExcludedByRobots {
			t.Errorf("Expected robot_ex: %v\nBut got: %v\nFor input: %+v",
				exp.ExcludedByRobots, actual.ExcludedByRobots, tcase.Input)
		}
		if exp.Status != actual.Status {
			t.Errorf("Expected stat: %v\nBut got: %v\nFor input: %+v",
				exp.Status, actual.Status, tcase.Input)
		}
		if exp.MimeType != actual.MimeType {
			t.Errorf("Expected mime: %v\nBut got: %v\nFor input: %+v",
				exp.MimeType, actual.MimeType, tcase.Input)
		}
	}
}

func TestURLCreation(t *testing.T) {
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

	created, err := walker.CreateURL("test.com", "sub1", "thepath?query=blah", "http",
		walker.NotYetCrawled)
	if err != nil {
		t.Fatal(err)
	}
	if created.String() != wurl1.String() {
		t.Errorf("Expected CreateURL to return %v\nBut got: %v", wurl1, created)
	}
}

var tldtests = []struct {
	URL                string
	ExpectedTLDPlusOne string
	ExpectedSubdomain  string
	ErrorExpected      bool
}{
	{"http://sub1.test.com/thepath?query=blah", "test.com", "sub1", false},
	{"http://foo", "", "", true},
}

func TestURLTLD(t *testing.T) {
	for _, dt := range tldtests {
		u, err := walker.ParseURL(dt.URL)
		if err != nil {
			if !dt.ErrorExpected {
				t.Errorf("Did not expect error parsing %v: %v", dt.URL, err)
			}
			continue
		}

		dom, err := u.ToplevelDomainPlusOne()
		if err != nil && !dt.ErrorExpected {
			t.Errorf("Did not expect error getting TLD+1: %v", err)
		}
		if dom != dt.ExpectedTLDPlusOne {
			t.Errorf("Expected ToplevelDomainPlusOne to be %v\nBut got: %v",
				dt.ExpectedTLDPlusOne, dom)
		}
		subdom, err := u.Subdomain()
		if err != nil && !dt.ErrorExpected {
			t.Errorf("Did not expect error getting subdomain: %v", err)
		}
		if subdom != dt.ExpectedSubdomain {
			t.Errorf("Expected Subdomain to be %v\nBut got: %v",
				dt.ExpectedSubdomain, subdom)
		}

		dom2, subdom2, err := u.TLDPlusOneAndSubdomain()
		if err != nil && !dt.ErrorExpected {
			t.Errorf("Did not expect error getting TLD+1 and subdomain: %v", err)
		}
		if dom2 != dt.ExpectedTLDPlusOne {
			t.Errorf("Expected TLDPlusOneAndSubdomain to give domain %v\nBut got: %v",
				dt.ExpectedTLDPlusOne, dom2)
		}
		if subdom2 != dt.ExpectedSubdomain {
			t.Errorf("Expected TLDPlusOneAndSubdomain to give subdomain %v\nBut got: %v",
				dt.ExpectedSubdomain, subdom2)
		}
	}
}

func TestAddingRedirects(t *testing.T) {
	db := getDB(t)
	ds := getDS(t)

	link := func(index int) string {
		return fmt.Sprintf("http://subdom.dom.com/page%d.html", index)
	}

	fr := walker.FetchResults{
		URL:            parse(link(1)),
		RedirectedFrom: []*walker.URL{parse(link(2)), parse(link(3))},
		FetchTime:      time.Unix(0, 0),
	}

	ds.StoreURLFetchResults(&fr)

	expected := []struct {
		link  string
		redto string
	}{
		{link: link(1), redto: link(2)},
		{link: link(2), redto: link(3)},
		{link: link(3), redto: ""},
	}

	for _, exp := range expected {
		url := parse(exp.link)

		dom, subdom, _ := url.TLDPlusOneAndSubdomain()
		itr := db.Query("SELECT redto_url FROM links WHERE dom = ? AND subdom = ? AND path = ? AND proto = ?",
			dom,
			subdom,
			url.RequestURI(),
			url.Scheme).Iter()
		var redto string
		if !itr.Scan(&redto) {
			t.Errorf("Failed to find link %q", exp.link)
			continue
		}

		err := itr.Close()
		if err != nil {
			t.Errorf("Iterator returned error %v", err)
			continue
		}

		if redto != exp.redto {
			t.Errorf("Redirect mismatch: got %q, expected %q", redto, exp.redto)
		}
	}
}
