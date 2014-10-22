package walker

import (
	"container/heap"
	"fmt"
	"math"
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

	domains chan string   // For passing domains to generate to worker goroutines
	quit    chan struct{} // Channel to close to stop the dispatcher (used by `Stop()`)

	// synchronizes when all generator routines have exited, so
	// `StopDispatcher()` can wait until all processing is done
	finishWG sync.WaitGroup

	// synchronizes generators that are currently working, so we can wait for
	// them to finish before we start a new domain iteration
	generatingWG sync.WaitGroup
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

	for i := 0; i < Config.Dispatcher.NumConcurrentDomains; i++ {
		d.finishWG.Add(1)
		go func() {
			d.generateRoutine()
			d.finishWG.Done()
		}()
	}

	d.domainIterator()
	return nil
}

func (d *CassandraDispatcher) StopDispatcher() error {
	log4go.Info("Stopping CassandraDispatcher")
	close(d.quit)
	d.finishWG.Wait()
	d.db.Close()
	return nil
}

func (d *CassandraDispatcher) domainIterator() {
	for {
		log4go.Debug("Starting new domain iteration")
		domainiter := d.db.Query(`SELECT dom, dispatched FROM domain_info
									WHERE claim_tok = 00000000-0000-0000-0000-000000000000
									AND dispatched = false ALLOW FILTERING`).Iter()

		var domain string
		var dispatched bool
		for domainiter.Scan(&domain, &dispatched) {
			select {
			case <-d.quit:
				log4go.Debug("Domain iterator signaled to stop")
				close(d.domains)
				return
			default:
			}

			if !dispatched {
				d.domains <- domain
			}
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
		d.generatingWG.Wait()
	}
}

func (d *CassandraDispatcher) generateRoutine() {
	for domain := range d.domains {
		d.generatingWG.Add(1)
		if err := d.generateSegment(domain); err != nil {
			log4go.Error("error generating segment for %v: %v", domain, err)
		}
		d.generatingWG.Done()
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
						ORDER BY subdom, path, proto, time, getnow`, domain).Iter()

	//
	// Three lists to hold the 3 link types: (a) getNow links (b) uncrawled links (c) crawled links
	//
	var getNowLinks []*URL
	var uncrawledLinks []*URL
	var crawledLinks PriorityUrl
	heap.Init(&crawledLinks)

	//
	// Cell captures all the information for a link in one place
	//
	type Cell struct {
		dom, subdom, path, proto string
		crawl_time               time.Time
		getnow                   bool
	}

	cell_equal := func(l *Cell, r *Cell) bool {
		return l.dom == r.dom &&
			l.subdom == r.subdom &&
			l.path == r.path &&
			l.proto == r.proto
	}

	//
	// Some integer handling functions
	//
	// imax := func(l int, r int) int {
	// 	if l < r {
	// 		return r
	// 	} else {
	// 		return l
	// 	}
	// }
	imin := func(l int, r int) int {
		if l > r {
			return r
		} else {
			return l
		}
	}
	round := func(f float64) float64 {
		abs := math.Abs(f)
		sign := f / abs
		floor := math.Floor(abs)
		if abs-floor >= 0.5 {
			return sign * (floor + 1)
		} else {
			return sign * floor
		}

	}

	//
	// Do the scan
	//
	var limit = Config.Dispatcher.MaxLinksPerSegment
	var start = true
	var current Cell
	var previous Cell
	for iter.Scan(&current.dom, &current.subdom, &current.path, &current.proto, &current.crawl_time, &current.getnow) {
		if start {
			previous = current
			start = false
		}

		// IMPL NOTE: So the trick here is that, within a given domain, the entries
		// come out so that the crawl_time increases as you iterate. So in order to
		// get the most recent link, simply take the last link in a series that shares
		// dom, subdom, path, and protocol
		if !cell_equal(&current, &previous) {

			u, err := CreateURL(previous.dom, previous.subdom, previous.path, previous.proto, previous.crawl_time)
			if err != nil {
				log4go.Error(err.Error())
				continue
			}

			if previous.getnow {
				getNowLinks = append(getNowLinks, u)
			} else if previous.crawl_time.Equal(NotYetCrawled) {
				if len(uncrawledLinks) < limit {
					uncrawledLinks = append(uncrawledLinks, u)
				}
			} else {
				if crawledLinks.Len() < limit {
					heap.Push(&crawledLinks, u)
				}
			}

			if len(getNowLinks) >= limit {
				break
			}
		}

		current = previous
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("error selecting links for %v: %v", domain, err)
	}

	//
	// Merge the 3 link types
	//
	var links []*URL
	links = append(links, getNowLinks...)

	numRemain := limit - len(links)
	if numRemain > 0 {
		refreshDecimal := Config.Dispatcher.RefreshPercentage / 100.0
		numCrawledF := round(refreshDecimal * float64(numRemain))
		numCrawled := imin(int(numCrawledF), crawledLinks.Len())
		numUncrawled := numRemain - numCrawled
		for numCrawled+numUncrawled > 0 {

			if numUncrawled > 0 {
				u := uncrawledLinks[numUncrawled-1]
				links = append(links, u)
				numUncrawled--
			}

			if numCrawled > 0 {
				u := heap.Pop(&crawledLinks).(*URL)
				links = append(links, u)
				numCrawled--
			}
		}

	}

	//
	// Got any links
	//
	if len(links) == 0 {
		log4go.Info("No links to dispatch for %v", domain)
		return nil
	}

	//
	// Insert into segments
	//
	for _, u := range links {
		log4go.Debug("Inserting link in segment: %v", u.String())
		dom, subdom, err := u.TLDPlusOneAndSubdomain()
		if err != nil {
			log4go.Error("generateSegment not inserting %v: %v", u, err)
			return err
		}
		err = d.db.Query(`INSERT INTO segments
			(dom, subdom, path, proto, time)
			VALUES (?, ?, ?, ?, ?)`,
			dom, subdom, u.RequestURI(), u.Scheme, u.LastCrawled).Exec()
		if err != nil {
			log4go.Error("Failed to insert link (%v), error: %v", u, err)
		}
	}

	//
	// Go back and clean out the getnow flag.
	//
	for _, u := range getNowLinks {
		dom, subdom, err := u.TLDPlusOneAndSubdomain()
		if err != nil {
			log4go.Error("generateSegment not updateing %v: %v", u, err)
			continue
		}
		err = d.db.Query(`UPDATE links SET getnow = false WHERE dom = ? AND subdom = ? AND path = ? AND proto = ? AND crawl_time = ?`,
			dom, subdom, u.RequestURI(), u.Scheme, u.LastCrawled).Exec()
		if err != nil {
			log4go.Error("generateSegment failed update to %v: %v", u, err)
		}
	}

	//
	// Update dispatched flag
	//
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

//
// PriorityUrl is a heap of URLs, where the next element Pop'ed off
// the list points to the oldest (as measured by LastCrawled) element
// in the list
//
type PriorityUrl []*URL

func (pq PriorityUrl) Len() int {
	return len(pq)
}

func (pq PriorityUrl) Less(i, j int) bool {
	return pq[i].LastCrawled.Before(pq[j].LastCrawled)
}

func (pq PriorityUrl) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityUrl) Push(x interface{}) {
	*pq = append(*pq, x.(*URL))
}

func (pq *PriorityUrl) Pop() interface{} {
	old := *pq
	n := len(old)
	x := old[n-1]
	*pq = old[0 : n-1]
	return x
}
