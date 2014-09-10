package walker

import (
	"net/http"

	"code.google.com/p/log4go"
)

// CrawlManager configures and starts the crawl.
//
// This implementation is dumb and only starts one fetcher
type CrawlManager struct {
	// Transport can be set to override the default network transport the
	// CrawlManager is going to use. Good for faking remote servers for
	// testing.
	Transport http.RoundTripper
	f         *fetcher
	handlers  []Handler
	ds        Datastore
	started   bool
}

// NewCrawlManager creates but does not start a CrawlManager. The caller must
// set a Datastore and handlers, then call `Start()`
func NewCrawlManager() *CrawlManager {
	return new(CrawlManager)
}

// Start begins processing assuming that the datastore and any handlers have
// been set. This is a blocking call (run in a goroutine if you want to do
// other things)
//
// You cannot change the datastore or handlers after starting.
func (cm *CrawlManager) Start() {
	log4go.Info("Starting CrawlManager")
	if cm.ds == nil {
		panic("Cannot start a CrawlManager without a datastore")
	}
	if cm.started {
		panic("Cannot start a CrawlManager multiple times")
	}
	cm.started = true
	cm.f = newFetcher(cm)
	cm.f.start()
}

func (cm *CrawlManager) Stop() {
	log4go.Info("Stopping CrawlManager")
	if !cm.started {
		panic("Cannot stop a CrawlManager that has not been started")
	}
	cm.f.stop()
	//TODO: wait till all fetchers have stopped
}

// AddHandler will cause the given handler to be called once for each fetch
// response that comes back. Handlers cannot be added after the crawl has
// started.
//
// Every added handler will be called with every response (it is not a
// multiplexer/pool to choose from).
func (cm *CrawlManager) AddHandler(h Handler) {
	log4go.Info("Adding handler: %v", h)
	if cm.started {
		panic("You cannot add handlers to a CrawlManager after starting it")
	}
	cm.handlers = append(cm.handlers, h)
}

// SetDatastore assigns the datastore this CrawlManager should use. There must
// be exactly one of these and after starting the crawler it cannot be changed.
func (cm *CrawlManager) SetDatastore(ds Datastore) {
	log4go.Info("Setting Datastore: %v", ds)
	if cm.started {
		panic("You cannot set the CrawlManager datastore after starting it")
	}
	cm.ds = ds
}