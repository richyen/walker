package walker

import (
	"fmt"
	"sync"
	"time"

	"code.google.com/p/log4go"

	"github.com/gocql/gocql"
)

// Dispatcher defines the calls a dispatcher should respond to. A dispatcher
// would typically be paired with a particular Datastore, and not all Datastore
// implementations may need a Dispatcher.
//
// A basic crawl will likely run the dispatcher in the same process as the
// fetchers, but higher-scale crawl setups may run dispatchers separately.
type Dispatcher interface {
	// StartDispatcher should be a blocking call that starts the dispatcher. It
	// should return an error if it could not start or stop properly and nil
	// when it has safely shut down and stopped all internal processing.
	StartDispatcher() error

	// Stop signals the dispatcher to stop. It should block until all internal
	// goroutines have stopped.
	StopDispatcher() error
}

// CassandraDispatcher analyzes what we've crawled so far (generally on a per-domain
// basis) and updates the database. At minimum this means generating new
// segments to crawl in the `segments` table, but it can also mean updating
// domain_info if we find out new things about a domain.
//
// This dispatcher has been designed to run simultaneously with the
// fetchmanager. Fetchers and dispatchers claim domains in Cassandra, so the
// dispatcher can operate on the domains not currently being crawled (and vice
// versa).
type CassandraDispatcher struct {
	cf *gocql.ClusterConfig
	db *gocql.Session

	domains chan string    // For passing domains to generate to worker goroutines
	quit    chan struct{}  // Channel to close to stop the dispatcher (used by `Stop()`)
	wg      sync.WaitGroup // WaitGroup for the generator goroutines
}

func (d *CassandraDispatcher) StartDispatcher() error {
	log4go.Info("Starting CassandraDispatcher")
	d.cf = GetCassandraConfig()
	var err error
	d.db, err = d.cf.CreateSession()
	if err != nil {
		return fmt.Errorf("Failed to create cassandra session: %v", err)
	}

	d.quit = make(chan struct{})
	d.domains = make(chan string)

	//TODO: add Concurrency to config
	concurrency := 1
	for i := 0; i < concurrency; i++ {
		d.wg.Add(1)
		go func() {
			d.generateRoutine()
			d.wg.Done()
		}()
	}

	d.domainIterator()
	return nil
}

func (d *CassandraDispatcher) StopDispatcher() error {
	log4go.Info("Stopping CassandraDispatcher")
	close(d.quit)
	d.wg.Wait()
	d.db.Close()
	return nil
}

func (d *CassandraDispatcher) domainIterator() {
	for {
		log4go.Debug("Starting new domain iteration")
		domainiter := d.db.Query(`SELECT dom FROM domain_info
									WHERE claim_tok = 00000000-0000-0000-0000-000000000000`).Iter()

		domain := ""
		for domainiter.Scan(&domain) {
			select {
			case <-d.quit:
				log4go.Debug("Domain iterator signaled to stop")
				close(d.domains)
				return
			default:
			}

			d.domains <- domain
		}

		// Check for exit here as well in case domain_info is empty
		select {
		case <-d.quit:
			log4go.Debug("Domain iterator signaled to stop")
			close(d.domains)
			return
		default:
		}

		if err := domainiter.Close(); err != nil {
			log4go.Error("Error iterating domains from domain_info: %v", err)
		}

		//TODO: configure this sleep time
		time.Sleep(time.Second)
	}
}

func (d *CassandraDispatcher) generateRoutine() {
	for domain := range d.domains {
		if err := d.generateSegment(domain); err != nil {
			log4go.Error("error generating segment for %v: %v", domain, err)
		}
	}
	log4go.Debug("Finishing generateRoutine")
}

// generateSegment reads links in for this domain, generates a segment for it,
// and inserts the domain into domains_to_crawl (assuming a segment is ready to
// go)
//
// This implementation is dumb, we're just scheduling the first 500 links we
// haven't crawled yet. We never recrawl.
func (d *CassandraDispatcher) generateSegment(domain string) error {
	log4go.Info("Generating a crawl segment for %v", domain)
	iter := d.db.Query(`SELECT dom, subdom, path, proto, time
						FROM links WHERE dom = ?
						ORDER BY subdom, path, proto, time`, domain).Iter()
	var linkdomain, subdomain, path, protocol string
	var crawl_time time.Time
	links := make(map[string]*URL)
	for iter.Scan(&linkdomain, &subdomain, &path, &protocol, &crawl_time) {
		u, err := CreateURL(linkdomain, subdomain, path, protocol, crawl_time)
		if err != nil {
			log4go.Error(err.Error())
			continue
		}

		if crawl_time.Equal(NotYetCrawled) {
			if len(links) >= 500 {
				// Stop here because we've moved on to a new link
				log4go.Fine("Hit 500 links, not adding any more to the segment")
				break
			}

			log4go.Fine("Adding link to segment list: %#v", u)
			links[u.String()] = u
		} else {
			// This means we've already crawled the link, so leave it out
			// Because we order by crawl_time we won't hit the link again
			// later with crawl_time == NotYetCrawled

			log4go.Fine("Link already crawled, removing from segment list: %#v", u)
			delete(links, u.String())
		}
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("error selecting links for %v: %v", domain, err)
	}

	for _, u := range links {
		log4go.Debug("Inserting link in segment: %v", u)
		err := d.db.Query(`INSERT INTO segments
			(dom, subdom, path, proto, time)
			VALUES (?, ?, ?, ?, ?)`,
			u.ToplevelDomainPlusOne(), u.Subdomain(), u.Path, u.Scheme, u.LastCrawled).Exec()

		if err != nil {
			log4go.Error("Failed to insert link (%v), error: %v", u, err)
		}
	}
	err := d.db.Query(`UPDATE domain_info SET dispatched = true WHERE dom = ?`, domain).Exec()
	if err != nil {
		return fmt.Errorf("error inserting %v to domains_to_crawl: %v", domain, err)
	}
	log4go.Info("Generated segment for %v (%v links)", domain, len(links))

	// Batch insert -- may be faster but hard to figured out what happened on
	// errors
	//
	//batch := d.db.NewBatch(gocql.UnloggedBatch)
	//batch.Query(`INSERT INTO domains_to_crawl (domain, priority, crawler_token)
	//				VALUES (?, ?, 00000000-0000-0000-0000-000000000000)`, domain, 0)
	//for u, _ := range links {
	//	log4go.Debug("Adding link to segment batch insert: %v", u)
	//	batch.Query(`INSERT INTO segments (domain, subdom, path, proto, time)
	//						VALUES (?, ?, ?, ?, ?)`,
	//		u.Host, "", u.Path, u.Scheme, NotYetCrawled)
	//}
	//log4go.Info("Inserting %v links in segment for %v", batch.Size()-1, domain)
	//if err := d.db.ExecuteBatch(batch); err != nil {
	//	return fmt.Errorf("error inserting links for segment %v: %v", domain, err)
	//}
	return nil
}
