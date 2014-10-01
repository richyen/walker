package console

import (
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
)

//NOTE NOTE NOTE: I'm going to start to define a set of structs and interfaces UNDOCUMENTED.
// will document in follow up

type DomainInfo struct {
	Domain            string
	ExcludeReason     string
	TimeQueued        time.Time
	UuidOfQueued      string
	NumberLinksTotal  int
	NumberLinksQueued int
}

type LinkInfo struct {
	Url            string
	Status         int
	Error          string
	RobotsExcluded bool
	CrawlTime      time.Time
}

//DataStore represents all the interaction the application has with the datastore.
//
type DataStore interface {
	Close()
	InsertLinks(links []string) []error
	ListDomains(seed string, limit int) ([]DomainInfo, error)
	ListWorkingDomains(seed string, limit int) ([]DomainInfo, error)
	ListLinks(domain string, seed string, limit int) ([]LinkInfo, error)
}

var DS DataStore

//
// Cassandra DataSTore
//
type CqlDataStore struct {
	Cluster *gocql.ClusterConfig
	Db      *gocql.Session
}

func NewCqlDataStore() (*CqlDataStore, error) {
	ds := new(CqlDataStore)
	ds.Cluster = gocql.NewCluster(walker.Config.Cassandra.Hosts...)

	ds.Cluster.Keyspace = walker.Config.Cassandra.Keyspace
	var err error
	ds.Db, err = ds.Cluster.CreateSession()
	return ds, err
}

func (ds *CqlDataStore) Close() {
	ds.Db.Close()
}

//XXX: part of this is cribbed from walker.datastore.go. Code share?
func (ds *CqlDataStore) addDomainIfNew(domain string) error {
	var count int
	err := ds.Db.Query(`SELECT COUNT(*) FROM domain_info WHERE domain = ?`, domain).Scan(&count)
	if err != nil {
		return err
	}

	if count == 0 {
		err := ds.Db.Query(`INSERT INTO domain_info (domain) VALUES (?)`, domain).Exec()
		if err != nil {
			return err
		}
	}

	return nil
}

//NOTE: InsertLinks should try to insert as much information as possible
//return errors for things it can't handle
func (ds *CqlDataStore) InsertLinks(links []string) []error {
	//
	// Collect domains
	//
	var errList []error
	var domains []string
	var urls []*walker.URL
	for _, link := range links {
		url, err := walker.ParseURL(link)
		if err != nil {
			errList = append(errList, fmt.Errorf("Link %v: %v", link, err))
			continue
		}
		domain := url.ToplevelDomainPlusOne()
		urls = append(urls, url)
		domains = append(domains, domain)
	}

	//
	// Push domain information to table. The only trick to this, is I don't add links unless
	// the domain can be added
	//
	db := ds.Db
	var seen = map[string]bool{}
	for i := range domains {
		d := domains[i]
		u := urls[i]

		if !seen[d] {
			err := ds.addDomainIfNew(d)
			if err != nil {
				errList = append(errList, fmt.Errorf("Link %v unable to push domain: %v", u.String(), err))
				continue
			}
		}
		seen[d] = true
		err := db.Query(`INSERT INTO links (domain, subdomain, path, protocol, crawl_time)
                                     VALUES (?, ?, ?, ?, ?)`, d, u.Subdomain(),
			u.RequestURI(), u.Scheme, walker.NotYetCrawled).Exec()
		if err != nil {
			errList = append(errList, fmt.Errorf("Link %v unable to push to links: %v", u.String(), err))
			continue
		}

		err = db.Query(`INSERT INTO segments (domain, subdomain, path, protocol, crawl_time)
                                     VALUES (?, ?, ?, ?)`, d, u.Subdomain(), u.RequestURI(),
			u.Scheme).Exec()
		if err != nil {
			errList = append(errList, fmt.Errorf("Link %v unable to push to segments: %v", u.String(), err))
			continue
		}
	}

	return errList
}

func (ds *CqlDataStore) annotateDomainInfo(dinfos []DomainInfo) error {
	var itr *gocql.Iter
	db := ds.Db

	//NOTE: ClaimNewHost in walker.datastore.go uses priority 0, so I will as well.
	priority := 0
	for _, d := range dinfos {
		var uuid gocql.UUID
		var t time.Time
		itr = db.Query("SELECT crawler_token, claim_time FROM domains_to_crawl WHERE priority = ? AND domain = ?", priority, d).Iter()
		got := itr.Scan(&uuid, &t)
		err := itr.Close()
		if err != nil {
			return err
		}
		if got {
			d.TimeQueued = t
			d.UuidOfQueued = uuid.String()
		}
	}

	//
	// Count Links
	//
	for _, d := range dinfos {
		var linkCount, segmentCount int
		itr = db.Query("SELECT count(*) FROM links WHERE domain = ?", d.Domain).Iter()
		itr.Scan(&linkCount)
		err := itr.Close()
		if err != nil {
			return err
		}
		d.NumberLinksTotal = linkCount
		d.NumberLinksQueued = 0
		if d.UuidOfQueued != "" {
			itr = db.Query("SELECT count(*) FROM segments WHERE domain = ?", d.Domain).Iter()
			itr.Scan(&segmentCount)
			err := itr.Close()
			if err != nil {
				return err
			}
			d.NumberLinksQueued = segmentCount
		}
	}

	return nil
}

func (ds *CqlDataStore) ListDomains(seed string, limit int) ([]DomainInfo, error) {
	db := ds.Db

	var itr *gocql.Iter
	if seed == "" {
		itr = db.Query("SELECT domain, excluded, exclude_reason FROM domain_info LIMIT ?", limit).Iter()
	} else {
		itr = db.Query("SELECT domain, excluded, exclude_reason FROM domain_info WHERE TOKEN(domain) > TOKEN(?) LIMIT ?", seed, limit).Iter()
	}

	var dinfos []DomainInfo
	var domain string
	var excluded bool
	var excludeReason string
	for itr.Scan(&domain, &excluded, &excludeReason) {
		if excluded && excludeReason == "" {
			excludeReason = "Excluded"
		}
		dinfos = append(dinfos, DomainInfo{Domain: domain, ExcludeReason: excludeReason})
		excludeReason = ""
	}
	err := itr.Close()
	if err != nil {
		return dinfos, err
	}

	err = ds.annotateDomainInfo(dinfos)

	return dinfos, err
}

func (ds *CqlDataStore) ListWorkingDomains(seed string, limit int) ([]DomainInfo, error) {
	db := ds.Db

	var itr *gocql.Iter
	zeroUuid := gocql.UUID{}
	if seed == "" {
		itr = db.Query("SELECT domain FROM domains_to_crawl WHERE crawler_token > ? OR crawler_token < ? LIMIT ?", zeroUuid, zeroUuid, limit).Iter()
	} else {
		itr = db.Query("SELECT domain FROM domains_to_crawl WHERE (crawler_token > ? OR crawler_token < ?) AND TOKEN(domain) > TOKEN(?) LIMIT ?",
			zeroUuid, zeroUuid, seed, limit).Iter()
	}

	var domain string
	var domains []string
	for itr.Scan(&domain) {
		domains = append(domains, domain)
	}
	err := itr.Close()
	if err != nil {
		return nil, err
	}

	queryString := "SELECT domain, excluded, exclude_reason FROM domain_info WHERE domain IN (" +
		strings.Join(domains, ",") +
		")"

	itr = db.Query(queryString).Iter()
	var dinfos []DomainInfo
	var excluded bool
	var excludeReason string
	for itr.Scan(&domain, &excluded, &excludeReason) {
		if excluded && excludeReason == "" {
			excludeReason = "Excluded"
		}
		dinfos = append(dinfos, DomainInfo{Domain: domain, ExcludeReason: excludeReason})
		excludeReason = ""
	}
	err = itr.Close()
	if err != nil {
		return dinfos, err
	}

	err = ds.annotateDomainInfo(dinfos)

	return dinfos, err
}

// Pagination note:
// To paginate a single column you can do
//
//   SELECT a FROM table WHERE TOKEN(a) > TOKEN(startingA)
//
// If you have two columns though, it requires two queries
//
//   SELECT a,b from table WHERE TOKEN(a) == TOKEN(startingA) AND TOKEN(b) > TOKEN(startingB)
//   SELECT a,b from table WHERE TOKEN(a) > TOKEN(startingA)
//
// With 3 columns it looks like this
//
//   SELECT a,b,c FROM table WHERE TOKEN(a) == TOKEN(startingA) AND TOKEN(b) == TOKEN(startingB) AND TOKEN(c) > TOKEN(startingC)
//   SELECT a,b,c FROM table WHERE TOKEN(a) == TOKEN(startingA) AND TOKEN(b) > TOKEN(startingB)
//   SELECT a,b,c FROM table WHERE TOKEN(a) > TOKEN(startingA)
//
// Particularly for our links table, with primary key (domain, subdomain, path, protocol, crawl_time)
// (For right now, ignore the crawl time) we write
//
// SELECT * FROM links WHERE TOKEN(domain) == TOKEN(startDomain) AND TOKEN(subdomain) == TOKEN(startSubDomain) AND TOKEN(path) == TOKEN(startPath)
//                           AND TOKEN(protocol) > TOKEN(startProtocol)
// SELECT * FROM links WHERE TOKEN(domain) == TOKEN(startDomain) AND TOKEN(subdomain) == TOKEN(startSubDomain) AND TOKEN(path) > TOKEN(startPath)
// SELECT * FROM links WHERE TOKEN(domain) == TOKEN(startDomain) AND TOKEN(subdomain) > TOKEN(startSubDomain)
// SELECT * FROM links WHERE TOKEN(domain) > TOKEN(startDomain)
//
// Now the only piece left, is that crawl_time is part of the primary key. Generally we're only going to take the latest crawl time. But see
// Historical query
//

//XXX: seed is currently ignored
func (ds *CqlDataStore) ListLinks(domain string, seed string, limit int) ([]LinkInfo, error) {
	db := ds.Db
	var itr *gocql.Iter
	if seed != "" {
		_, err := walker.ParseURL(seed)
		if err != nil {
			return nil, err
		}
		//SEED IS IGNORED RIGHT NOW. Note above on pagination. Going to come back to this
	}
	if limit > 0 {
		itr = db.Query(`SELECT subdomain, path, protocol, crawl_time, status, error, robots_excluded 
                                   FROM links WHERE domain = ? LIMIT ?`, domain, limit).Iter()
	} else {
		itr = db.Query(`SELECT subdomain, path, protocol, crawl_time, status, error, robots_excluded 
                                   FROM links WHERE domain = ? `, domain).Iter()
	}

	var subdomain, path, protocol, anerror string
	var crawlTime time.Time
	var robotsExcluded bool
	var status int
	var linfos []LinkInfo

	// for now, I'm not depending on order
	times := map[string]struct {
		ctm time.Time
		ind int
	}{}

	for itr.Scan(&subdomain, &path, &protocol, &crawlTime, &status, &anerror) {

		u, err := walker.CreateURL(domain, subdomain, path, protocol, crawlTime)
		if err != nil {
			return nil, err
		}
		urlString := u.String()

		qq, yes := times[urlString]

		if yes && qq.ctm.After(crawlTime) {
			continue
		}

		linfo := LinkInfo{
			Url:            urlString,
			Status:         status,
			Error:          anerror,
			RobotsExcluded: robotsExcluded,
			CrawlTime:      crawlTime,
		}

		if yes {
			qq.ctm = crawlTime
			linfos[qq.ind] = linfo
		} else {
			linfos = append(linfos, linfo)
			times[urlString] = struct {
				ctm time.Time
				ind int
			}{crawlTime, len(linfos) - 1}
		}

	}

	return linfos, nil
}

/*
Add Link:
    * Can insert any number of newline separated links. That list will be parsed, the union of all the domains in the list of links will be entered into the correct tables. And the links will be entered and queued up to be searched.

Search on Domain (see Rendered for each Domain):
    * Can list all domains stored in cassandra
    * Can list all currently being crawled domains

Rendered for each Domain
    * domain string [example.com]
    * excluded reason: [robots.txt excluded], possibly NULL
    * last time queued: time when this domain was last picked up by a crawler, NULL if never queued
    * UUID of queued: the UUID of the crawler currently working on this domain, or NULL if not currently queued
    * Number of Links (how many links in 'links' table for this domain)
    * Number of Links queued to process (how many links in the 'segments' table for this domain)
    * Can click to list links (see Rendered for each Link)
    * Can do subdomain search on links  (see Rendered for each Link)


Rendered for each Link:
    * url: http://foo.bar.com/niffler.txt
    * status: the HTTP status code of the last GET
    * error: the error that occurred during the last GET operation, or NULL if no error.
    * robots excluded: boolean indicates if the link was excluded by robots.txt
    * A link to the history of this link. A list of each attempt to GET this link.

A note on what it means to 'list':
    Below any place we say "list" we mean limited list. We'll always only render N elements to page. So when we "list domains" we'll only list, say, 50 domains on a page. We'll paginate as needed for longer lists.

*/

/*
func (ds *CqlDataStore) LinkStats(domain string, windowStart int, windowLen int) (int, int, error) {

	var countLinks int
	countLinksIter := ds.db.Query(
		`SELECT count(*) FROM links WHERE domain = ?`,
		domain,
	).Iter()
	if !countLinksIter.Scan(&countLinks) {
		return 0, 0, fmt.Errorf("Failed to count links: %v", err)
	}
	countLinksIter.Close()

	var countSegments int
	countSegmentsIter := ds.db.Query(
		`SELECT count(*) FROM segments WHERE domain = ?`,
		domain,
	).Iter()
	if !countLinksIter.Scan(&countSegments) {
		return 0, 0, fmt.Errorf("Failed to count links: %v", err)
	}
	countSegmentsIter.Close()

	return countLinks, countSegments, nil
}
*/

/*
From DAN:

By crawl status I just meant any general aggregate stats we already have for the given domain (or searched links).

For example a crawl history, (*) meaning the list of links we've crawled and when we crawled them, (*) what their signature was, etc.

basically I should be able to type in a domain and see a summary of (*) how many links we've crawled, (*) how many we haven't yet crawled.

I should be able to search for a specific link and see (*) how many times we crawled it and (*) what the result was each time, including when we initially parsed it.

Hopefully that makes some sense; these are the things we'd want to show but how to do it and what we are able to show now is going to require a bit of creativity. Some things, for example signature (meaning 'fp' in the database, fingerprint) is not something we are calculating yet, so not yet useful in the console.


*/
