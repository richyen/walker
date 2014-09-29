package console

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

type UrlInfo struct {
	Link      string
	CrawledOn time.Time
}

type DataStore interface {
	Close()
	ListLinkDomains() ([]string, error)
	LinksForDomain(domain string) ([]UrlInfo, error)
}

var DS DataStore

//
// Spoof data source
//
type SpoofDataSource struct {
}

func NewSpoofDataSource() (*SpoofDataSource, error) {
	return &SpoofDataSource{}, nil
}

func (ds *SpoofDataSource) Close() {}

func (ds *SpoofDataSource) ListLinkDomains() ([]string, error) {
	return []string{"test1.org", "test2.com"}, nil
}

func (ds *SpoofDataSource) LinksForDomain(domain string) ([]UrlInfo, error) {
	return []UrlInfo{
		UrlInfo{"http://foo.bar.com", time.Now().AddDate(0, 0, -30)},
		UrlInfo{"http://foo.baz.org", time.Now().AddDate(0, 0, -15)},
		UrlInfo{"http://www.niffler.com", time.Now()},
	}, nil
}

//
// DataStore methods
//
type CqlDataStore struct {
	cluster *gocql.ClusterConfig
	db      *gocql.Session
}

func NewCqlDataStore(host string) (*CqlDataStore, error) {
	ds := new(CqlDataStore)
	ds.cluster = gocql.NewCluster(host)
	ds.cluster.Keyspace = "walker"
	var err error
	ds.db, err = ds.cluster.CreateSession()
	return ds, err
}

func (ds *CqlDataStore) Close() {
	ds.db.Close()
}

func (ds *CqlDataStore) ListLinkDomains() ([]string, error) {
	var domains []string
	var domain string
	i := ds.db.Query(`SELECT distinct domain FROM links`).Iter()
	for i.Scan(&domain) {
		domains = append(domains, domain)
	}
	err := i.Close()
	return domains, err
}

func (ds *CqlDataStore) LinksForDomain(domain string) ([]UrlInfo, error) {
	i := ds.db.Query(
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
