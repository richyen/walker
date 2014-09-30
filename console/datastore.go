package console

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
)

//NOTE NOTE NOTE: I'm going to start to define a set of structs and interfaces UNDOCUMENTED.
// will document in follow up

type UrlInfo struct {
	// url string
	Link string

	// when the url was last crawled (could be zero for uncrawled url)
	CrawledOn time.Time
}

type DomainInfo struct {
	Domain            string
	ExcludeReason     string
	LastTimeQueued    time.Time
	UuidOfQueued      string
	NumberLinksTotal  int
	NumberLinksQueued int
}

type LinkInfo struct {
	Url            string
	LastStatus     int
	LastError      string
	RobotsExcluded bool
}

//DataStore represents all the interaction the application has with the datastore.
//
type DataStore interface {
	Close()

	//InsertLinks adds a set of Links to the system. The TLD+1 domain of each
	//link is also added to the system.
	InsertLinks(links []string) []error

	ListDomains(limit int) ([]DomainInfo, error)
	ListWorkingDomains(limit int) ([]DomainInfo, error)

	ListLinks(domain string, limit int) ([]LinkInfo, error)
	ListWorkingLinks(domain string, limit int) ([]LinkInfo, error)

	//LEGACY BELOW

	//List all known domains.
	ListLinkDomains() ([]string, error)

	// LinksForDomain returns a list of urls for a given domain. If there are N,
	// Links to return: this is represented by <0 ... N-1>. If windowStart >= 0 then
	// the method returns <windowStart ... N-1>. If windowLen >=0 and windowStart >= 0
	// the method returns <windowStart ... windowStart+windowLen>
	LinksForDomain(domain string, windowStart int, windowLen int) ([]UrlInfo, error)

	// WorkingDomains returns all the domains currently owned by a crawler instance
	// (i.e. being worked on)
	WorkingDomains(windowStart int, windowLen int) ([]string, error)
}

var DS DataStore

func UNIMP() {
	panic("UNIMPLEMENTED")
}

//
// Spoof data source
//

//TBD

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

func (ds *CqlDataStore) InsertLinks(links []string) []error {
	var errList []error
	var domains []string
	var urls []walker.URL
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
}
func (ds *CqlDataStore) ListDomains(limit int) ([]DomainInfo, error)        { UNIMP(); return nil, nil }
func (ds *CqlDataStore) ListWorkingDomains(limit int) ([]DomainInfo, error) { UNIMP(); return nil, nil }
func (ds *CqlDataStore) SearchDomains(prefix string, limit int) ([]DomainInfo, error) {
	UNIMP()
	return nil, nil
}
func (ds *CqlDataStore) SearchWorkingDomains(prefix string, limit int) ([]DomainInfo, error) {
	UNIMP()
	return nil, nil
}
func (ds *CqlDataStore) ListLinks(domain string, limit int) ([]LinkInfo, error) {
	UNIMP()
	return nil, nil
}
func (ds *CqlDataStore) ListWorkingLinks(domain string, limit int) ([]LinkInfo, error) {
	UNIMP()
	return nil, nil
}
func (ds *CqlDataStore) SearchLinks(domain string, subdomain string) ([]LinkInfo, error) {
	UNIMP()
	return nil, nil
}
func (ds *CqlDataStore) SearchWorkingLinks(domain string, subdomain string) ([]LinkInfo, error) {
	UNIMP()
	return nil, nil
}

func (ds *CqlDataStore) ListLinkDomains() ([]string, error) {
	var domains []string
	var domain string
	i := ds.Db.Query(`SELECT distinct domain FROM links`).Iter()
	for i.Scan(&domain) {
		domains = append(domains, domain)
	}
	err := i.Close()
	return domains, err
}

func (ds *CqlDataStore) LinksForDomain(domain string, windowStart int, windowLen int) ([]UrlInfo, error) {
	i := ds.Db.Query(
		`SELECT domain, subdomain, path, protocol, crawl_time
            FROM links WHERE domain = ?
            ORDER BY subdomain, path, protocol, crawl_time`,
		domain,
	).Iter()

	var urls []UrlInfo
	var linkdomain, subdomain, path, protocol string
	var crawl_time time.Time
	for i.Scan(&linkdomain, &subdomain, &path, &protocol, &crawl_time) {
		if subdomain != "" {
			subdomain = subdomain + "."
		}
		url := UrlInfo{
			Link:      fmt.Sprintf("%s://%s%s/%s", protocol, subdomain, linkdomain, path),
			CrawledOn: crawl_time,
		}
		urls = append(urls, url)
	}
	err := i.Close()

	return urls, err
}

func (ds *CqlDataStore) WorkingDomains(windowStart int, windowLen int) ([]string, error) {
	panic("UNIMPLEMENTED")
	return []string{}, nil
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
