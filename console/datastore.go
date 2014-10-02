package console

import (
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
)

type DomainInfo struct {
	//TLD+1
	Domain string

	//Why did this domain get excluded, or empty if not excluded
	ExcludeReason string

	//When did this domain last get queued to be crawled. Or TimeQueed.IsZero() if not crawled
	TimeQueued time.Time

	//What was the UUID of the crawler that last crawled the domain
	UuidOfQueued string

	//Number of (unique) links found in this domain
	NumberLinksTotal int

	//Number of (unique) links queued to be processed for this domain
	NumberLinksQueued int
}

type LinkInfo struct {
	//URL of the link
	Url string

	//Status of the GET
	Status int

	//Any error reported during the get
	Error string

	//Was this excluded by robots
	RobotsExcluded bool

	//When did this link get crawled
	CrawlTime time.Time
}

//
//DataStore represents all the interaction the application has with the datastore.
//
const DontSeedDomain = ""
const DontSeedIndex = 0

var DontSeedCrawlTime = time.Time{}

type DataStore interface {
	//INTERFACE NOTE: any place you see a seed variable that is a string/timestamp
	// that represents the last value of the previous call. limit is the max number
	// of results returned. seed and limit are used to implement pagination.

	// Close the data store after you're done with it
	Close()

	// InsertLinks queues a set of URLS to be crawled
	InsertLinks(links []string) []error

	// List domains
	ListDomains(seedDomain string, limit int) ([]DomainInfo, error)

	// Same as ListDomains, but only lists the domains that are currently queued
	ListWorkingDomains(seedDomain string, limit int) ([]DomainInfo, error)

	// List links from the given domain
	ListLinks(domain string, seedIndex int, limit int) ([]LinkInfo, error)

	// For a given linkUrl, return the entire crawl history
	ListLinkHistorical(linkUrl string, seedCrawlTime time.Time, limit int) ([]LinkInfo, error)
}

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

//NOTE: part of this is cribbed from walker.datastore.go. Code share?
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

func (ds *CqlDataStore) countUniqueLinks(domain string, table string) (int, error) {
	db := ds.Db
	q := fmt.Sprintf("SELECT subdomain, path, protocol, crawl_time FROM %s WHERE domain = ?", table)
	itr := db.Query(q, domain).Iter()

	var subdomain, path, protocol string
	var crawlTime time.Time
	found := map[string]time.Time{}
	for itr.Scan(&subdomain, &path, &protocol, &crawlTime) {
		key := fmt.Sprintf("%s : %s : %s", subdomain, path, protocol)
		t, foundT := found[key]
		if !foundT || t.Before(crawlTime) {
			found[key] = crawlTime
		}
	}
	err := itr.Close()
	return len(found), err
}

func (ds *CqlDataStore) annotateDomainInfo(dinfos []DomainInfo) error {
	var itr *gocql.Iter
	db := ds.Db

	//NOTE: ClaimNewHost in walker.datastore.go uses priority 0, so I will as well.
	priority := 0
	for i := range dinfos {
		d := &dinfos[i]
		var uuid gocql.UUID
		var t time.Time
		itr = db.Query("SELECT crawler_token, claim_time FROM domains_to_crawl WHERE priority = ? AND domain = ?", priority, d.Domain).Iter()
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
	for i := range dinfos {
		d := &dinfos[i]

		linkCount, err := ds.countUniqueLinks(d.Domain, "links")
		if err != nil {
			return err
		}
		d.NumberLinksTotal = linkCount

		d.NumberLinksQueued = 0
		if d.UuidOfQueued != "" {
			segmentCount, err := ds.countUniqueLinks(d.Domain, "segments")
			if err != nil {
				return err
			}
			d.NumberLinksQueued = segmentCount
		}
	}

	return nil
}

func (ds *CqlDataStore) ListDomains(seed string, limit int) ([]DomainInfo, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("Bad value for limit parameter %d", limit)
	}

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

func (ds *CqlDataStore) ListWorkingDomains(seedDomain string, limit int) ([]DomainInfo, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("Bad value for limit parameter %d", limit)
	}

	db := ds.Db

	//NOTE TO READERS: CQL has no OR syntax. Which means queries that might look like this
	//    itr = db.Query("SELECT domain FROM domains_to_crawl WHERE crawler_token > ? OR crawler_token < ? LIMIT ?", zeroUuid, zeroUuid, limit).Iter()
	//won't compile. I hope that domains to crawl isn't that big.
	itr := db.Query("SELECT domain FROM domains_to_crawl").Iter()
	var domain string
	var domains []string
	for itr.Scan(&domain) {
		if seedDomain != "" && domain <= seedDomain {
			continue
		}
		domains = append(domains, domain)
		if len(domains) >= limit {
			break
		}
	}
	err := itr.Close()
	if err != nil {
		return nil, err
	}

	if len(domains) == 0 {
		return []DomainInfo{}, nil
	}

	//NOTE: this query is not going to be efficient for large domains_to_crawl
	quotedDomains := []string{}
	for _, d := range domains {
		quotedDomains = append(quotedDomains, "'"+d+"'")
	}
	queryString := "SELECT domain, excluded, exclude_reason FROM domain_info WHERE domain IN (" +
		strings.Join(quotedDomains, ",") +
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

type rememberTimes struct {
	ctm time.Time
	ind int
}

// //NOTE: it's the callers responsibility to close itr and check the DS error
// func (ds *CqlDataStore) collectLinkInfos(linfos []LinkInfo, rtimes map[string]rememberTimes, itr *gocql.Iter, limit int) ([]LinkInfo, error) {
// 	var domain, subdomain, path, protocol, anerror string
// 	var crawlTime time.Time
// 	var robotsExcluded bool
// 	var status int
// 	for itr.Scan(&domain, &subdomain, &path, &protocol, &crawlTime, &status, &anerror, &robotsExcluded) {
// 		u, err := walker.CreateURL(domain, subdomain, path, protocol, crawlTime)
// 		if err != nil {
// 			return linfos, err
// 		}
// 		urlString := u.String()

// 		qq, yes := rtimes[urlString]

// 		if yes && qq.ctm.After(crawlTime) {
// 			continue
// 		}

// 		linfo := LinkInfo{
// 			Url:            urlString,
// 			Status:         status,
// 			Error:          anerror,
// 			RobotsExcluded: robotsExcluded,
// 			CrawlTime:      crawlTime,
// 		}

// 		nindex := -1
// 		if yes {
// 			nindex = qq.ind
// 			linfos[qq.ind] = linfo
// 		} else {
// 			linfos = append(linfos, linfo)
// 			nindex = len(linfos) - 1
// 		}
// 		rtimes[urlString] = rememberTimes{ctm: crawlTime, ind: nindex}

// 		if len(linfos) >= limit {
// 			break
// 		}
// 	}
// 	return linfos, nil
// }

type queryEntry struct {
	query string
	args  []interface{}
}

// func (ds *CqlDataStore) seededListLinks(seedUrl string, limit int) ([]LinkInfo, error) {
// 	db := ds.Db

// 	var linfos []LinkInfo
// 	rtimes := map[string]rememberTimes{}

// 	u, err := walker.ParseURL(seedUrl)
// 	if err != nil {
// 		return linfos, err
// 	}
// 	dom := u.ToplevelDomainPlusOne()
// 	sub := u.Subdomain()
// 	pat := u.RequestURI()
// 	pro := u.Scheme

// 	table := []queryEntry{
// 		queryEntry{
// 			query: `SELECT domain, subdomain, path, protocol, crawl_time, status, error, robots_excluded
//                       FROM links
//                       WHERE TOKEN(domain) = TOKEN(?) AND
//                             TOKEN(subdomain) = TOKEN(?) AND
//                             TOKEN(path) = TOKEN(?) AND
//                             TOKEN(protocol) > TOKEN(?)`,
// 			args: []interface{}{dom, sub, pat, pro},
// 		},
// 		queryEntry{
// 			query: `SELECT domain, subdomain, path, protocol, crawl_time, status, error, robots_excluded
//                       FROM links
//                       WHERE TOKEN(domain) = TOKEN(?) AND
//                             TOKEN(subdomain) = TOKEN(?) AND
//                             TOKEN(path) > TOKEN(?)`,
// 			args: []interface{}{dom, sub, pat},
// 		},
// 		queryEntry{
// 			query: `SELECT domain, subdomain, path, protocol, crawl_time, status, error, robots_excluded
//                       FROM links
//                       WHERE TOKEN(domain) = TOKEN(?) AND
//                             TOKEN(subdomain) > TOKEN(?)`,
// 			args: []interface{}{dom, sub},
// 		},
// 	}

// 	for _, qt := range table {
// 		itr := db.Query(qt.query, qt.args...).Iter()
// 		linfos, err = ds.collectLinkInfos(linfos, rtimes, itr, limit)
// 		if err != nil {
// 			return linfos, err
// 		}

// 		err = itr.Close()
// 		if err != nil {
// 			return linfos, err
// 		} else if len(linfos) >= limit {
// 			return linfos, nil
// 		}
// 	}

// 	return linfos, nil
// }

//NOTE: have to implement paging in software. domain is the partition key, and can only use TOKEN on partition key
func (ds *CqlDataStore) ListLinks(domain string, seedIndex int, limit int) ([]LinkInfo, int, error) {
	if limit <= 0 {
		return nil, seedIndex, fmt.Errorf("Bad value for limit parameter %d", limit)
	}
	db := ds.Db

	q := `SELECT subdomain, path, protocol, crawl_time, status, error, robots_excluded 
                          FROM links 
                          WHERE domain = ?`

	var subdomain, path, protocol, anerror string
	var crawlTime time.Time
	var robotsExcluded bool
	var status int
	linfos := []LinkInfo{}
	rtimes := map[string]rememberTimes{}
	count := 0
	itr := db.Query(q, domain).Iter()
	for itr.Scan(&subdomain, &path, &protocol, &crawlTime, &status, &anerror, &robotsExcluded) {
		// SKip first seedIndex elements
		if count < seedIndex {
			count++
			continue
		}

		//ok we got an unseeded value
		u, err := walker.CreateURL(domain, subdomain, path, protocol, crawlTime)
		if err != nil {
			itr.Close()
			return linfos, seedIndex + len(linfos), err
		}
		urlString := u.String()

		qq, yes := rtimes[urlString]

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

		nindex := -1
		if yes {
			linfos[qq.ind] = linfo
			nindex = qq.ind
		} else {
			linfos = append(linfos, linfo)
			nindex = len(linfos) - 1
		}
		rtimes[urlString] = rememberTimes{ctm: crawlTime, ind: nindex}

		if len(linfos) >= limit {
			break
		}
	}
	err := itr.Close()
	return linfos, seedIndex + len(linfos), err

}

func (ds *CqlDataStore) ListLinkHistorical(linkUrl string, seedCrawlTime time.Time, limit int) ([]LinkInfo, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("Bad value for limit parameter %d", limit)
	}
	db := ds.Db
	u, err := walker.ParseURL(linkUrl)
	if err != nil {
		return nil, err
	}

	query := `SELECT domain, subdomain, path, protocol, crawl_time, status, error, robots_excluded
              FROM links
              WHERE domain = ? AND subdomain = ? AND path = ? AND protocol = ?`
	args := []interface{}{u.ToplevelDomainPlusOne(), u.Subdomain(), u.RequestURI(), u.Scheme}
	if !seedCrawlTime.IsZero() {
		query = query + " AND crawl_time > ?"
		args = append(args, seedCrawlTime)
	}
	query = query + " LIMIT ?"
	args = append(args, limit)
	itr := db.Query(query, args...).Iter()

	var linfos []LinkInfo
	var dom, sub, path, prot, getError string
	var crawlTime time.Time
	var status int
	var robotsExcluded bool
	for itr.Scan(&dom, &sub, &path, &prot, &crawlTime, &status, &getError, &robotsExcluded) {
		url, _ := walker.CreateURL(dom, sub, path, prot, crawlTime)
		linfo := LinkInfo{
			Url:            url.String(),
			Status:         status,
			Error:          getError,
			RobotsExcluded: robotsExcluded,
			CrawlTime:      crawlTime,
		}
		linfos = append(linfos, linfo)
	}
	err = itr.Close()

	return linfos, err
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
