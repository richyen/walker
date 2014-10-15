// +build cassandra

package test

import (
	"net/http"
	"net/url"
	"reflect"

	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
)

type DispatcherTest struct {
	ExistingDomainInfos  []ExistingDomainInfo
	ExistingLinks        []ExistingLink
	ExpectedSegmentLinks []walker.URL
}

type ExistingDomainInfo struct {
	Dom        string
	ClaimTok   gocql.UUID
	Priority   int
	Dispatched bool
}

type ExistingLink struct {
	URL    walker.URL
	Status int // -1 indicates this is a parsed link, not yet fetched
}

var DispatcherTests = []DispatcherTest{
	{ // A basic run test
		[]ExistingDomainInfo{
			{"test.com", gocql.UUID{}, 0, false},
		},
		[]ExistingLink{
			{URL: walker.URL{URL: urlParse("http://test.com/page1.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: urlParse("http://test.com/page2.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: urlParse("http://test.com/page404.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: urlParse("http://test.com/page500.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: urlParse("http://test.com/notcrawled1.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: urlParse("http://test.com/notcrawled2.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: urlParse("http://test.com/page1.html"),
				LastCrawled: time.Now()}, Status: http.StatusOK},
			{URL: walker.URL{URL: urlParse("http://test.com/page2.html"),
				LastCrawled: time.Now()}, Status: http.StatusOK},
			{URL: walker.URL{URL: urlParse("http://test.com/page404.html"),
				LastCrawled: time.Now()}, Status: http.StatusNotFound},
			{URL: walker.URL{URL: urlParse("http://test.com/page500.html"),
				LastCrawled: time.Now()}, Status: http.StatusInternalServerError},
		},
		[]walker.URL{
			{URL: urlParse("http://test.com/notcrawled1.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: urlParse("http://test.com/notcrawled2.html"),
				LastCrawled: walker.NotYetCrawled},
		},
	},
	{ // Verifies that we work with query parameters properly
		[]ExistingDomainInfo{
			{"test.com", gocql.UUID{}, 0, false},
		},
		[]ExistingLink{
			{URL: walker.URL{URL: urlParse("http://test.com/page1.html?p=v"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
		},
		[]walker.URL{
			{URL: urlParse("http://test.com/page1.html?p=v"),
				LastCrawled: walker.NotYetCrawled},
		},
	},
	{ // Verifies that we don't generate an already-dispatched domain
		[]ExistingDomainInfo{
			{"test.com", gocql.UUID{}, 0, true},
		},
		[]ExistingLink{
			{URL: walker.URL{URL: urlParse("http://test.com/page1.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
		},
		[]walker.URL{},
	},
}

func TestDispatcherBasic(t *testing.T) {
	var q *gocql.Query

	for _, dt := range DispatcherTests {
		db := getDB(t) // runs between tests to reset the db

		for _, edi := range dt.ExistingDomainInfos {
			q = db.Query(`INSERT INTO domain_info (dom, claim_tok, priority, dispatched)
							VALUES (?, ?, ?, ?)`,
				edi.Dom, edi.ClaimTok, edi.Priority, edi.Dispatched)
			if err := q.Exec(); err != nil {
				t.Fatalf("Failed to insert test domain info: %v\nQuery: %v", err, q)
			}
		}

		for _, el := range dt.ExistingLinks {
			if el.Status == -1 {
				q = db.Query(`INSERT INTO links (dom, subdom, path, proto, time)
								VALUES (?, ?, ?, ?, ?)`,
					el.URL.ToplevelDomainPlusOne(),
					el.URL.Subdomain(),
					el.URL.RequestURI(),
					el.URL.Scheme,
					el.URL.LastCrawled)
			} else {
				q = db.Query(`INSERT INTO links (dom, subdom, path, proto, time, stat)
								VALUES (?, ?, ?, ?, ?, ?)`,
					el.URL.ToplevelDomainPlusOne(),
					el.URL.Subdomain(),
					el.URL.RequestURI(),
					el.URL.Scheme,
					el.URL.LastCrawled,
					el.Status)
			}
			if err := q.Exec(); err != nil {
				t.Fatalf("Failed to insert test links: %v\nQuery: %v", err, q)
			}
		}

		d := &walker.CassandraDispatcher{}
		go d.StartDispatcher()
		time.Sleep(time.Second)
		d.StopDispatcher()

		expectedResults := map[url.URL]bool{}
		for _, esl := range dt.ExpectedSegmentLinks {
			expectedResults[*esl.URL] = true
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

		for _, edi := range dt.ExistingDomainInfos {
			q = db.Query(`SELECT dispatched FROM domain_info WHERE dom = ?`, edi.Dom)
			var dispatched bool
			if err := q.Scan(&dispatched); err != nil {
				t.Fatalf("Failed to insert find domain info: %v\nQuery: %v", err, q)
			}
			if !dispatched {
				t.Errorf("`dispatched` flag not set on domain: %v", edi.Dom)
			}
		}
	}
}
