package walker

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"code.google.com/p/go.net/publicsuffix"
	"code.google.com/p/log4go"

	"github.com/gocql/gocql"
)

// Datastore defines the interface for an object to be used as walker's datastore.
//
// Note that this is for link and metadata storage required to make walker
// function properly. It has nothing to do with storing fetched content (see
// `Handler` for that).
type Datastore interface {
	// ClaimNewHost returns a hostname that is now claimed for this crawler to
	// crawl. A segment of links for this host is assumed to be available.
	// Returns the domain of the segment it claimed, or "" if there are none
	// available.
	ClaimNewHost() string

	// UnclaimHost indicates that all links from `LinksForHost` have been
	// processed, so other work may be done with this host. For example the
	// dispatcher will be free analyze the links and generate a new segment.
	UnclaimHost(host string)

	// LinksForHost returns a channel that will feed URLs for a given host.
	LinksForHost(host string) <-chan *url.URL

	// StoreURLFetchResults takes the return data/metadata from a fetch and
	// stores the visit. Fetchers will call this once for each link in the
	// segment being crawled.
	StoreURLFetchResults(fr *FetchResults)

	// StoreParsedURL stores a URL parsed out of a page (i.e. a URL we may not
	// have crawled yet). `u` is the URL to store. `res` is the FetchResults
	// object for the fetch from which we got the URL, for any context the
	// datastore may want.
	//
	// This layer should handle efficiently deduplicating
	// links (i.e. a fetcher should be safe feeding the same URL many times.
	StoreParsedURL(u *url.URL, fr *FetchResults)
}

type CassandraDatastore struct {
	cf            *gocql.ClusterConfig
	db            *gocql.Session
	cachedDomains []string
}

func NewCassandraDatastore(cf *gocql.ClusterConfig) (*CassandraDatastore, error) {
	ds := new(CassandraDatastore)
	ds.cf = cf
	var err error
	ds.db, err = ds.cf.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("Failed to create cassandra datastore: %v", err)
	}
	return ds, nil
}

func (ds *CassandraDatastore) Close() {
	ds.db.Close()
}

func (ds *CassandraDatastore) ClaimNewHost() string {

	// Get our range of priority values and sort high to low
	// Currently simplified to one level top optimize fake crawler
	priorities := []int{0}

	//priorities := []int{}
	//var p int
	//priority_iter := ds.db.Query(`SELECT DISTINCT priority FROM domains_to_crawl`).Iter()
	//defer priority_iter.Close()
	//for priority_iter.Scan(&p) {
	//	priorities = append(priorities, p)
	//}
	//sort.Sort(sort.Reverse(sort.IntSlice(priorities)))

	if len(ds.cachedDomains) == 0 {
		// Start with the highest priority selecting until we find an unclaimed domain segment,
		// then claim it
		start := time.Now()
		var domain string
		for _, p := range priorities {
			domain_iter := ds.db.Query(`SELECT domain FROM domains_to_crawl
										WHERE priority = ?
										AND crawler_token = 00000000-0000-0000-0000-000000000000
										LIMIT 50`, p).Iter()
			defer domain_iter.Close()
			for domain_iter.Scan(&domain) {
				//TODO: use lightweight transaction to allow more crawlers
				//TODO: use a per-crawler uuid
				log4go.Info("ClaimNextDomain selected new domain in %v", time.Since(start))
				start = time.Now()
				crawluuid, _ := gocql.RandomUUID()
				err := ds.db.Query(`UPDATE domains_to_crawl SET crawler_token = ?, claim_time = ?
									WHERE priority = ? AND domain = ?`,
					crawluuid, time.Now(), p, domain).Exec()
				if err != nil {
					log4go.Error("Failed to claim segment %v: %v", domain, err)
				} else {
					log4go.Info("Claimed segment %v with token %v in %v", domain, crawluuid, time.Since(start))
					ds.cachedDomains = append(ds.cachedDomains, domain)
				}
			}
		}
	}

	if len(ds.cachedDomains) > 0 {
		// Pop the last element and return it
		lastIndex := len(ds.cachedDomains) - 1
		domain := ds.cachedDomains[lastIndex]
		ds.cachedDomains = ds.cachedDomains[:lastIndex]
		return domain
	} else {
		return ""
	}
}

func (ds *CassandraDatastore) UnclaimHost(host string) {
}

func (ds *CassandraDatastore) LinksForHost(domain string) <-chan *url.URL {
	links, err := ds.getSegmentLinks(domain)
	if err != nil {
		log4go.Error("Failed to grab segment for %v: %v", domain, err)
		return nil
	}
	log4go.Info("Returning %v links to crawl domain %v", len(links), domain)

	err = ds.deleteClaimedSegment(domain)
	if err != nil {
		log4go.Error("Failed to delete claimed segment for %v: %v", domain, err)
	}

	linkchan := make(chan *url.URL, len(links))
	for _, l := range links {
		linkchan <- l
	}
	return linkchan
}

func (ds *CassandraDatastore) StoreURLFetchResults(fr *FetchResults) {
	u := fr.Url
	domain, err := publicsuffix.EffectiveTLDPlusOne(u.Host)
	subdomain := strings.TrimSuffix(u.Host, domain)

	if fr.FetchError != nil {
		//TODO
	}

	if fr.ExcludedByRobots {
		//TODO: populate robots_excluded
	}

	redirectURL, _ := fr.Res.Location()

	err = ds.db.Query(
		`INSERT INTO links (domain, subdomain, path, protocol, crawl_time, status,
			error, fp, redirect_url, ip, mime)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		domain,
		subdomain,
		u.Path,
		u.Scheme,
		fr.FetchTime,
		fr.Res.StatusCode,
		nil, //TODO: get fp
		redirectURL,
		nil, //TODO can we get RemoteAddr? fr.Res.Request.RemoteAddr may not be filled in
		fr.Res.Header.Get("Content-Type"),
	).Exec()
	if err != nil {
		log4go.Error("Failed storing fetch results: %v", err)
	}
}

func (ds *CassandraDatastore) StoreParsedURL(u *url.URL, fr *FetchResults) {
	if u.Host == "" {
		log4go.Warn("Not handling link because there is no host: %v", *u)
		return
	}
	ds.addDomainIfNew(u.Host)
	err := ds.db.Query(`INSERT INTO links (domain, subdomain, path, protocol, crawl_time)
						VALUES (?, ?, ?, ?, ?)`, u.Host, "", u.Path, u.Scheme, time.Unix(0, 0)).Exec()
	if err != nil {
		log4go.Error("failed inserting parsed url (%v) to cassandra, %v", u, err)
	}
}

func (ds *CassandraDatastore) addDomainIfNew(domain string) {
	var count int
	err := ds.db.Query(`SELECT COUNT(*) FROM domain_info WHERE domain = ?`, domain).Scan(&count)
	if err != nil {
		log4go.Error("Failed to check if %v is in domain_info: %v", domain, err)
		return // with error, assume we already have it and move on
	}
	if count == 0 {
		err := ds.db.Query(`INSERT INTO domain_info (domain) VALUES (?)`, domain).Exec()
		if err != nil {
			log4go.Error("Failed to add new domain %v: %v", domain, err)
		}
	}
}

func (ds *CassandraDatastore) getSegmentLinks(domain string) (links []*url.URL, err error) {
	q := ds.db.Query(`SELECT domain, subdomain, path, protocol, crawl_time
						FROM segments WHERE domain = ?`, domain)
	iter := q.Iter()
	defer func() { err = iter.Close() }()

	var dbdomain, subdomain, path, protocol string
	var crawl_time time.Time
	for iter.Scan(&dbdomain, &subdomain, &path, &protocol, &crawl_time) {
		if subdomain != "" {
			subdomain = subdomain + "."
		}
		link := fmt.Sprintf("%s://%s%s%s", protocol, subdomain, dbdomain, path)
		u, e := url.Parse(link)
		if e != nil {
			log4go.Error("Error adding link (%v) to crawl: %v", link, e)
		} else {
			log4go.Debug("Adding link: %v", u)
			links = append(links, u)
		}
	}
	return
}

func (ds *CassandraDatastore) deleteClaimedSegment(domain string) error {
	err := ds.db.Query(`DELETE FROM segments WHERE domain = ?`, domain).Exec()
	if err != nil {
		return fmt.Errorf("Failed deleting segment links for %v: %v", domain, err)
	}

	// Since (priority, domain) is the primary key we need to select the priority
	// first in order to delete. https://issues.apache.org/jira/browse/CASSANDRA-5527
	var priority int
	err = ds.db.Query(`SELECT priority FROM domains_to_crawl WHERE domain = ?`, domain).Scan(&priority)
	if err != nil {
		return fmt.Errorf("Failed getting priority for %v: %v", domain, err)
	}
	err = ds.db.Query(`DELETE FROM domains_to_crawl WHERE priority = ? AND domain = ?`,
		priority, domain).Exec()
	if err != nil {
		return fmt.Errorf("Failed deleting %v from domains_to_crawl: %v", domain, err)
	}
	return nil
}
