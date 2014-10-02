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
	//XXX: More elegant way to do this? Right now I want to make sure
	// it's set
	walker.Config.Cassandra.Keyspace = "walker_test"
	walker.Config.Cassandra.Hosts = []string{"localhost"}
	walker.Config.Cassandra.ReplicationFactor = 1

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

var fooTime = time.Now().AddDate(0, 0, -1)
var testTime = time.Now().AddDate(0, 0, -2)
var bazUuid, _ = gocql.RandomUUID()

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
	insertDomainToCrawl := `INSERT INTO domains_to_crawl (domain, crawler_token, priority, claim_time) VALUES (?, ?, ?, ?)`
	insertSegment := `INSERT INTO segments (domain, subdomain, path, protocol) VALUES (?, ?, ?, ?)`
	insertLink := `INSERT INTO links (domain, subdomain, path, protocol, crawl_time, status, error, robots_excluded) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

	queries := []*gocql.Query{
		db.Query(insertDomainInfo, "test.com", false, "", ""),
		db.Query(insertLink, "test.com", "", "page1.html", "http", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "test.com", "", "page2.html", "http", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "test.com", "", "page3.html", "http", walker.NotYetCrawled, 404, "", false),
		db.Query(insertLink, "test.com", "", "page4.html", "http", walker.NotYetCrawled, 200, "An Error", false),
		db.Query(insertLink, "test.com", "", "page5.html", "http", walker.NotYetCrawled, 200, "", true),

		db.Query(insertLink, "test.com", "sub", "page6.html", "http", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "test.com", "sub", "page7.html", "https", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "test.com", "sub", "page8.html", "https", walker.NotYetCrawled, 200, "", false),

		db.Query(insertDomainToCrawl, "test.com", gocql.UUID{}, 0, testTime),
		db.Query(insertSegment, "test.com", "", "page1.html", "http"),
		db.Query(insertSegment, "test.com", "", "page2.html", "http"),

		db.Query(insertDomainInfo, "foo.com", false, "", ""),
		db.Query(insertLink, "foo.com", "sub", "page1.html", "http", fooTime, 200, "", false),
		db.Query(insertLink, "foo.com", "sub", "page2.html", "http", fooTime, 200, "", false),

		db.Query(insertDomainInfo, "bar.com", true, "Didn't like it", ""),

		db.Query(insertDomainInfo, "baz.com", false, "", ""),
		db.Query(insertLink, "baz.com", "sub", "page1.html", "http", walker.NotYetCrawled, 200, "", false),
		db.Query(insertDomainToCrawl, "baz.com", bazUuid, 0, testTime),
		db.Query(insertSegment, "baz.com", "sub", "page1.html", "http"),
	}
	for _, q := range queries {
		err := q.Exec()
		if err != nil {
			t.Fatalf("Failed to insert test data: %v\nQuery: %v", err, q)
		}
	}

	// //TEST
	// itr := db.Query("SELECT domain FROM domain_info").Iter()
	// var domain string
	// for itr.Scan(&domain) {
	// 	fmt.Printf("DOMAIN: %v\n", domain)
	// }
	// err := itr.Close()
	// if err != nil {
	// 	panic(err)
	// }
	// return
	// itr = db.Query("SELECT domain FROM domain_info").Iter()
	// for itr.Scan(&domain) {
	// 	fmt.Printf("DOMAIN(2): %v\n", domain)
	// }
	// err = itr.Close()
	// if err != nil {
	// 	panic(err)
	// }

}

type domainTest struct {
	omittest bool
	tag      string
	seed     string
	limit    int
	expected []console.DomainInfo
}

type linkTest struct {
	omittest bool
	tag      string
	domain   string
	seed     string
	limit    int
	expected []console.LinkInfo
}

const LIM = 50

func dlist2dhash(target []console.DomainInfo) map[string]console.DomainInfo {
	h := map[string]console.DomainInfo{}
	for _, d := range target {
		h[d.Domain] = d
	}
	return h
}

const EPSILON_SECONDS = 1

func timeClose(l time.Time, r time.Time) bool {
	delta := l.Unix() - r.Unix()
	if delta < 0 {
		delta = -delta
	}
	return delta <= EPSILON_SECONDS
}

//Shared Domain Information
var bazDomain = console.DomainInfo{
	Domain:            "baz.com",
	NumberLinksTotal:  1,
	NumberLinksQueued: 1,
	TimeQueued:        testTime,
	UuidOfQueued:      bazUuid.String(),
}

var fooDomain = console.DomainInfo{
	Domain:            "foo.com",
	NumberLinksTotal:  2,
	NumberLinksQueued: 0,
}

var barDomain = console.DomainInfo{
	Domain:            "bar.com",
	NumberLinksTotal:  0,
	NumberLinksQueued: 0,
	ExcludeReason:     "Didn't like it",
}

var testDomain = console.DomainInfo{
	Domain:            "test.com",
	NumberLinksTotal:  8,
	NumberLinksQueued: 2,
	TimeQueued:        testTime,
	UuidOfQueued:      gocql.UUID{}.String(),
}

func TestListDomains(t *testing.T) {
	store := getDs(t)
	populate(t, store)

	tests := []domainTest{
		domainTest{
			tag:   "Basic Pull",
			seed:  console.DontSeedDomain,
			limit: LIM,
			expected: []console.DomainInfo{
				bazDomain,
				fooDomain,
				barDomain,
				testDomain,
			},
		},

		domainTest{
			tag:   "Limit Pull",
			seed:  console.DontSeedDomain,
			limit: 1,
			expected: []console.DomainInfo{
				bazDomain,
			},
		},

		domainTest{
			tag:   "Seeded Pull",
			seed:  "foo.com",
			limit: LIM,
			expected: []console.DomainInfo{
				barDomain,
				testDomain,
			},
		},

		domainTest{
			tag:   "Seeded & Limited Pull",
			seed:  "foo.com",
			limit: 1,
			expected: []console.DomainInfo{
				barDomain,
			},
		},
	}

	for _, test := range tests {
		if test.omittest {
			continue
		}
		dinfos, err := store.ListDomains(test.seed, test.limit)
		if err != nil {
			t.Errorf("ListDomains direct error %v", err)
			continue
		}

		// if !(len(dinfos) == test.limit || len(dinfos) == len(test.expected)) {
		// 	t.Errorf("ListDomains length mismatch")
		// 	continue
		// }

		if len(dinfos) != len(test.expected) {
			t.Errorf("ListDomains length mismatch %v: got %d, expected %d", test.tag, len(dinfos), len(test.expected))
			continue
		}

		//NOTE: we ARE NOT assuming any order from cassandra. The order I observed was neither insert order, nor
		//lexical order. Oh goodness!! The order I observed was "foo.com", "bar.com", "test.com"
		//expHash := dlist2dhash(test.expected)

		for i := range dinfos {
			got := dinfos[i]
			// exp, gotExp := expHash[got.Domain]
			// if !gotExp {
			// 	t.Errorf("ListDomains for tag '%s' Domain mismatch got %v, expected %v", test.tag, got.Domain, exp.Domain)
			// }
			exp := test.expected[i]
			if got.NumberLinksTotal != exp.NumberLinksTotal {
				t.Errorf("ListDomains with domain '%s' for tag '%s' NumberLinksTotal mismatch got %v, expected %v", got.Domain, test.tag, got.NumberLinksTotal, exp.NumberLinksTotal)
			}
			if got.NumberLinksQueued != exp.NumberLinksQueued {
				t.Errorf("ListDomains with domain '%s' for tag '%s' NumberLinksQueued mismatch got %v, expected %v", got.Domain, test.tag, got.NumberLinksQueued, exp.NumberLinksQueued)
			}
			if !timeClose(got.TimeQueued, exp.TimeQueued) {
				t.Errorf("ListDomains with domain '%s' for tag '%s' TimeQueued mismatch got %v, expected %v", got.Domain, test.tag, got.TimeQueued, exp.TimeQueued)
			}
			if got.UuidOfQueued != exp.UuidOfQueued {
				t.Errorf("ListDomains with domain '%s' for tag '%s' UuidOfQueued mismatch got %v, expected %v", got.Domain, test.tag, got.UuidOfQueued, exp.UuidOfQueued)
			}
			if got.ExcludeReason != exp.ExcludeReason {
				t.Errorf("ListDomains with domain '%s' for tag '%s' ExcludeReason mismatch got %v, expected %v", got.Domain, test.tag, got.ExcludeReason, exp.ExcludeReason)
			}
		}
	}
	store.Close()
}

func TestListWorkingDomains(t *testing.T) {
	store := getDs(t)
	populate(t, store)

	tests := []domainTest{
		domainTest{
			tag:   "Basic Pull",
			seed:  console.DontSeedDomain,
			limit: LIM,
			expected: []console.DomainInfo{
				bazDomain,
				testDomain,
			},
		},

		domainTest{
			tag:   "Limit Pull",
			seed:  console.DontSeedDomain,
			limit: 1,
			expected: []console.DomainInfo{
				bazDomain,
			},
		},

		domainTest{
			tag:   "Seeded Pull",
			seed:  "baz.com",
			limit: LIM,
			expected: []console.DomainInfo{
				testDomain,
			},
		},
	}

	for _, test := range tests {
		dinfos, err := store.ListWorkingDomains(test.seed, test.limit)
		if err != nil {
			t.Errorf("ListWorkingDomains direct error %v", err)
			continue
		}
		if len(dinfos) != len(test.expected) {
			t.Errorf("ListWorkingDomains length mismatch: got %d, expected %d", len(dinfos), len(test.expected))
			continue
		}
		for i := range dinfos {
			got := dinfos[i]
			exp := test.expected[i]
			if got.Domain != exp.Domain {
				t.Errorf("ListWorkingDomains %s Domain mismatch got %v, expected %v", test.tag, got.Domain, exp.Domain)
			}
			if got.NumberLinksTotal != exp.NumberLinksTotal {
				t.Errorf("ListWorkingDomains %s NumberLinksTotal mismatch got %v, expected %v", test.tag, got.NumberLinksTotal, exp.NumberLinksTotal)
			}
			if got.NumberLinksQueued != exp.NumberLinksQueued {
				t.Errorf("ListWorkingDomains %s NumberLinksQueued mismatch got %v, expected %v", test.tag, got.NumberLinksQueued, exp.NumberLinksQueued)
			}
			if !timeClose(got.TimeQueued, exp.TimeQueued) {
				t.Errorf("ListWorkingDomains %s TimeQueued mismatch got %v, expected %v", test.tag, got.TimeQueued, exp.TimeQueued)
			}
			if got.UuidOfQueued != exp.UuidOfQueued {
				t.Errorf("ListWorkingDomains %s UuidOfQueued mismatch got %v, expected %v", test.tag, got.UuidOfQueued, exp.UuidOfQueued)
			}
			if got.ExcludeReason != exp.ExcludeReason {
				t.Errorf("ListWorkingDomains %s ExcludeReason mismatch got %v, expected %v", test.tag, got.ExcludeReason, exp.ExcludeReason)
			}
		}
	}
	store.Close()
}

func TestListLinks(t *testing.T) {
	return
	store := getDs(t)
	populate(t, store)
	tests := []linkTest{
		linkTest{
			tag:    "Basic Pull",
			domain: "test.com",
			seed:   console.DontSeedUrl,
			limit:  5,
			expected: []console.LinkInfo{
				console.LinkInfo{
					Url:            "http://test.com/page1.html",
					Status:         200,
					Error:          "",
					RobotsExcluded: false,
					CrawlTime:      walker.NotYetCrawled,
				},

				console.LinkInfo{
					Url:            "http://test.com/page2.html",
					Status:         200,
					Error:          "",
					RobotsExcluded: false,
					CrawlTime:      walker.NotYetCrawled,
				},

				console.LinkInfo{
					Url:            "http://test.com/page3.html",
					Status:         404,
					Error:          "",
					RobotsExcluded: false,
					CrawlTime:      walker.NotYetCrawled,
				},

				console.LinkInfo{
					Url:            "http://test.com/page4.html",
					Status:         200,
					Error:          "An Error",
					RobotsExcluded: false,
					CrawlTime:      walker.NotYetCrawled,
				},

				console.LinkInfo{
					Url:            "http://test.com/page5.html",
					Status:         200,
					Error:          "",
					RobotsExcluded: true,
					CrawlTime:      walker.NotYetCrawled,
				},
			},
		},

		linkTest{
			tag:    "foo pull",
			domain: "foo.com",
			seed:   console.DontSeedUrl,
			limit:  LIM,
			expected: []console.LinkInfo{
				console.LinkInfo{
					Url:            "http://sub.foo.com/page1.html",
					Status:         200,
					Error:          "",
					RobotsExcluded: false,
					CrawlTime:      fooTime,
				},

				console.LinkInfo{
					Url:            "http://sub.foo.com/page2.html",
					Status:         200,
					Error:          "",
					RobotsExcluded: false,
					CrawlTime:      fooTime,
				},
			},
		},

		linkTest{
			tag:      "bar pull",
			domain:   "bar.com",
			seed:     console.DontSeedUrl,
			limit:    LIM,
			expected: []console.LinkInfo{},
		},

		linkTest{
			tag:    "seeded pull",
			domain: "test.com",
			seed:   "http://sub.test.com/page6.html",
			limit:  2,
			expected: []console.LinkInfo{
				console.LinkInfo{
					Url:            "http://sub.test.com/page7.html",
					Status:         200,
					Error:          "",
					RobotsExcluded: false,
					CrawlTime:      walker.NotYetCrawled,
				},

				console.LinkInfo{
					Url:            "http://sub.test.com/page8.html",
					Status:         200,
					Error:          "",
					RobotsExcluded: false,
					CrawlTime:      walker.NotYetCrawled,
				},
			},
		},
	}

	// run the tests
	for _, test := range tests {
		linfos, err := store.ListLinks(test.domain, test.seed, test.limit)
		if err != nil {
			t.Errorf("ListLinks direct error %v", err)
			continue
		}
		if len(linfos) != len(test.expected) {
			t.Errorf("ListLinks length mismatch")
			continue
		}
		for i := range linfos {
			got := linfos[i]
			exp := test.expected[i]
			if got.Url != exp.Url {
				t.Errorf("ListLinks %s Url mismatch %s vs %s", test.tag, got.Url, exp.Url)
			}
			if got.Status != exp.Status {
				t.Errorf("ListLinks %s Status mismatch %d vs %d", test.tag, got.Status, exp.Status)
			}
			if got.Error != exp.Error {
				t.Errorf("ListLinks %s Error mismatch %d vs %d", test.tag, got.Error, exp.Error)
			}
			if got.RobotsExcluded != exp.RobotsExcluded {
				t.Errorf("ListLinks %s RobotsExcluded mismatch %v vs %v", test.tag, got.RobotsExcluded, exp.RobotsExcluded)
			}
			if !got.CrawlTime.Equal(exp.CrawlTime) {
				t.Errorf("ListLinks %s CrawlTime mismatch %v vs %v", test.tag, got.CrawlTime, exp.CrawlTime)
			}
		}
	}

}

func TestInsertLinks(t *testing.T) {

}
