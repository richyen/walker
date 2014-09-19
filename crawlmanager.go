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
	// Handler must be set to handle fetch responses.
	Handler Handler

	// Datastore must be set to drive the fetching.
	Datastore Datastore

	// Transport can be set to override the default network transport the
	// FetchManager is going to use. Good for faking remote servers for
	// testing.
	Transport http.RoundTripper

	fetchers  []*fetcher
	fetchWait sync.WaitGroup
	started   bool
}

// Start begins processing assuming that the datastore and any handlers have
// been set. This is a blocking call (run in a goroutine if you want to do
// other things)
//
// You cannot change the datastore or handlers after starting.
func (fm *FetchManager) Start() {
	log4go.Info("Starting FetchManager")
	if fm.Datastore == nil {
		panic("Cannot start a FetchManager without a datastore")
	}
	if fm.Handler == nil {
		panic("Cannot start a FetchManager without a handler")
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
