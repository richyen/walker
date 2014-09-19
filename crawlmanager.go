package walker

import (
	"net/http"
	"sync"

	"code.google.com/p/log4go"
)

// FetchManager configures and runs the crawl.
//
// The calling code must create a FetchManager, set a Datastore and handlers,
// then call `Start()`
type FetchManager struct {
	// Transport can be set to override the default network transport the
	// FetchManager is going to use. Good for faking remote servers for
	// testing.
	Transport http.RoundTripper
	fetchers  []*fetcher
	fetchWait sync.WaitGroup
	handlers  []Handler
	ds        Datastore
	started   bool
}

// Start begins processing assuming that the datastore and any handlers have
// been set. This is a blocking call (run in a goroutine if you want to do
// other things)
//
// You cannot change the datastore or handlers after starting.
func (fm *FetchManager) Start() {
	log4go.Info("Starting FetchManager")
	if fm.ds == nil {
		panic("Cannot start a FetchManager without a datastore")
	}
	if fm.started {
		panic("Cannot start a FetchManager multiple times")
	}
	fm.started = true
	numFetchers := Config.NumSimultaneousFetchers
	fm.fetchers = make([]*fetcher, numFetchers)
	for i := 0; i < numFetchers; i++ {
		f := newFetcher(fm)
		fm.fetchers[i] = f
		fm.fetchWait.Add(1)
		go func() {
			f.start()
			fm.fetchWait.Done()
		}()
	}
	fm.fetchWait.Wait()
}

// Stop notifies the fetchers to finish their current requests. It blocks until
// all fetchers have finished.
func (fm *FetchManager) Stop() {
	log4go.Info("Stopping FetchManager")
	if !fm.started {
		panic("Cannot stop a FetchManager that has not been started")
	}
	for _, f := range fm.fetchers {
		go f.stop()
	}
	fm.fetchWait.Wait()
}

// AddHandler will cause the given handler to be called once for each fetch
// response that comes back. Handlers cannot be added after the crawl has
// started.
//
// Every added handler will be called with every response (it is not a
// multiplexer/pool to choose from).
func (fm *FetchManager) AddHandler(h Handler) {
	log4go.Info("Adding handler: %v", h)
	if fm.started {
		panic("You cannot add handlers to a FetchManager after starting it")
	}
	fm.handlers = append(fm.handlers, h)
}

// SetDatastore assigns the datastore this FetchManager should use. There must
// be exactly one of these and after starting the crawler it cannot be changed.
func (fm *FetchManager) SetDatastore(ds Datastore) {
	log4go.Info("Setting Datastore: %v", ds)
	if fm.started {
		panic("You cannot set the FetchManager datastore after starting it")
	}
	fm.ds = ds
}
