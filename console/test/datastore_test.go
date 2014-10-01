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

	initdb.Do(func() {
		err := walker.CreateCassandraSchema()
		if err != nil {
			t.Fatalf(err.Error())
		}
	})

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
	insertDomainInfo := `INSERT INTO domain_info (domain, excluded, exclude_reason, mirror_for) VALUES (?, ?, ?, ?)`
	insertDomainToCrawl := `INSERT INTO domains_to_crawl (domain, crawler_token, priority) VALUES (?, ?, ?)`
	insertSegment := `INSERT INTO segments (domain, subdomain, path, protocol) VALUES (?, ?, ?, ?)`
	insertLink := `INSERT INTO links (domain, subdomain, path, protocol, crawl_time) VALUES (?, ?, ?, ?, ?)`

	queries := []*gocql.Query{
		db.Query(insertDomainInfo, "test.com", false, "", ""),
		db.Query(insertLink, "test.com", "", "page1.html", "http", walker.NotYetCrawled),
		db.Query(insertLink, "test.com", "", "page2.html", "http", walker.NotYetCrawled),
		db.Query(insertDomainToCrawl, "test.com", gocql.UUID{}, 0),
		db.Query(insertSegment, "test.com", "", "page1.html", "http"),
		db.Query(insertSegment, "test.com", "", "page2.html", "http"),

		db.Query(insertDomainInfo, "foo.com", false, "", ""),
		db.Query(insertLink, "foo.com", "", "page1.html", "http", time.Now().AddDate(0, 0, -1)),
		db.Query(insertLink, "foo.com", "", "page2.html", "http", time.Now().AddDate(0, 0, -1)),

		db.Query(insertDomainInfo, "bar.com", true, "Didn't like it", ""),
	}
	for _, q := range queries {
		err := q.Exec()
		if err != nil {
			t.Fatalf("Failed to insert test data: %v\nQuery: %v", err, q)
		}
	}
}

type domainTest struct {
	tag      string
	seed     string
	expected []console.DomainInfo
}

const LIM = 50

/*******
type DomainInfo struct {
    //TLD+1
    Domain string

    //Why did this domain get excluded, or empty if not excluded
    ExcludeReason string

    //When did this domain last get queued to be crawled. Or TimeQueed.IsZero() if not crawled
    TimeQueued time.Time

    //What was the UUID of the crawler that last crawled the domain
    UuidOfQueued string

    //Number of links found in this domain
    NumberLinksTotal int

    //Number of links queued to be processed for this domain
    NumberLinksQueued int
}*/
func TestListDomains1(t *testing.T) {
	store := getDs(t)
	populate(t, store)

	tests := []domainTest{
		domainTest{
			tag:  "BasicPull",
			seed: console.DontSeedDomain,
			expected: []console.DomainInfo{
				console.DomainInfo{
					Domain:            "test.com",
					NumberLinksTotal:  2,
					NumberLinksQueued: 2,
				},
				console.DomainInfo{
					Domain:            "foo.com",
					NumberLinksTotal:  2,
					NumberLinksQueued: 0,
				},
				console.DomainInfo{
					Domain:            "bar.com",
					NumberLinksTotal:  0,
					NumberLinksQueued: 0,
				},
			},
		},
	}

	for _, test := range tests {
		dinfos, err := store.ListDomains(test.seed, LIM)
		if err != nil {
			t.Errorf("ListDomains direct error %v", err)
			continue
		}
		if len(dinfos) != len(test.expected) {
			t.Errorf("ListDomains length mismatch")
			continue
		}
		for i := range dinfos {
			got := dinfos[i]
			exp := test.expected[i]
			if got.Domain != exp.Domain {
				t.Errorf("ListDomains Domain mismatch %s vs %s", got.Domain, exp.Domain)
			}
			if got.NumberLinksTotal != exp.NumberLinksTotal {
				t.Errorf("ListDomains NumberLinksTotal mismatch %d vs %d", got.NumberLinksTotal, exp.NumberLinksTotal)
			}
			if got.NumberLinksQueued != exp.NumberLinksQueued {
				t.Errorf("ListDomains NumberLinksQueued mismatch %d vs %d", got.NumberLinksQueued, exp.NumberLinksQueued)
			}
			if !got.TimeQueued.Equal(exp.TimeQueued) {
				t.Errorf("ListDomains TimeQueued mismatch %v vs %v", got.TimeQueued, exp.TimeQueued)
			}
			if got.UuidOfQueued != exp.UuidOfQueued {
				t.Errorf("ListDomains UuidOfQueued mismatch %v vs %v", got.UuidOfQueued, exp.UuidOfQueued)
			}
			if got.ExcludeReason != exp.ExcludeReason {
				t.Errorf("ListDomains ExcludeReason mismatch %v vs %v", got.ExcludeReason, exp.ExcludeReason)
			}
		}
	}
}
