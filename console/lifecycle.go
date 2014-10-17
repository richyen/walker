package console

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"code.google.com/p/log4go"
	"github.com/codegangsta/negroni"
	"github.com/gorilla/mux"
	"github.com/iParadigms/walker"
)

//
// Pulled the stopable listener from the link at:
// http://www.hydrogen18.com/blog/stop-listening-http-server-go.html
//
type stoppableListener struct {
	*net.TCPListener          //Wrapped listener
	stopchan         chan int //Channel used only to indicate listener should shutdown
}

func newStopableListner(l net.Listener) (*stoppableListener, error) {
	tcpL, ok := l.(*net.TCPListener)

	if !ok {
		return nil, fmt.Errorf("Cannot wrap listener")
	}

	retval := &stoppableListener{}
	retval.TCPListener = tcpL
	retval.stopchan = make(chan int)

	return retval, nil
}

var stoppedErrorMark = fmt.Errorf("Listener stopped")
var stopingPollTime time.Duration = 1 * time.Second

func (sl *stoppableListener) Accept() (net.Conn, error) {

	for {
		//Wait up to one second for a new connection
		sl.TCPListener.SetDeadline(time.Now().Add(stopingPollTime))
		newConn, err := sl.TCPListener.AcceptTCP()

		//Check for the channel being closed
		select {
		case <-sl.stopchan:
			return nil, stoppedErrorMark
		default:
			//If the channel is still open, continue as normal
		}

		if err != nil {
			netErr, ok := err.(net.Error)

			//If this is a timeout, then continue to wait for
			//new connections
			if ok && netErr.Timeout() && netErr.Temporary() {
				continue
			}
		}

		// Emulate ListenAndServer (This code is taken from http://golang.org/src/pkg/net/http/server.go)
		newConn.SetKeepAlive(true)
		newConn.SetKeepAlivePeriod(3 * time.Minute)

		return newConn, err
	}
}

func (sl *stoppableListener) stop() {
	close(sl.stopchan)
}

//
// We need to be able to keep track of outstanding handler go-routines
//
var shutdownWaitGroup sync.WaitGroup
var shutdownChannel chan struct{}

func buildControllerCounter(toWrap func(w http.ResponseWriter, req *http.Request)) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		shutdownWaitGroup.Add(1)
		defer shutdownWaitGroup.Done()
		toWrap(w, req)
	}
}

// Start the console. NOTE: we only support a single instance of console
// at a time. You must match all your Start() calls with Stop() calls or else
// bad things happen.
func Start() {
	shutdownChannel = make(chan struct{})
	shutdownWaitGroup = sync.WaitGroup{}

	// this Add is being offset in the Listen and Serve part of the next go-routine
	shutdownWaitGroup.Add(1)
	go func() {

		//
		// Set up data store
		//
		ds, err := NewCqlModel()
		if err != nil {
			panic(fmt.Errorf("Failed to start data source: %v", err))
		}
		DS = ds
		defer ds.Close()

		//
		// Set up template renderer
		//
		BuildRender(true)

		//
		// Set up router to point to controllers
		//
		router := mux.NewRouter()
		routes := Routes()
		for _, route := range routes {
			log4go.Info("Registering path %s", route.Path)
			router.HandleFunc(route.Path, buildControllerCounter(route.Controller))
		}

		//
		// Set up middleware
		//
		neg := negroni.New(negroni.NewRecovery(), negroni.NewLogger(), negroni.NewStatic(http.Dir("public")))
		neg.UseHandler(router)

		//
		// Set up stopable listener apparatus
		//
		port := walker.Config.Console.Port

		// Build a stock tcp listener
		originalListener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			log4go.Error("Startup error: %v", err)
			panic(err)
		}

		// Now wrap that listener to include stop functionality
		stopper, err := newStopableListner(originalListener)
		if err != nil {
			log4go.Error("Startup error: %v", err)
			panic(err)
		}

		//
		// Shutdown server on signal or if shutdownChannel is closed
		//
		stop := make(chan os.Signal)
		signal.Notify(stop, syscall.SIGINT)
		go func() {
			select {
			case <-stop:
				log4go.Info("Console caught SIGINT")
			case <-shutdownChannel:
			}
			stopper.stop()
		}()

		//
		// Listen and Serve
		//
		defer shutdownWaitGroup.Done() // this offsets the Add(1) above
		server := http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: neg,
		}
		log4go.Info("Console starting up address http://127.0.0.1:%d/", port)

		// this call will block until stopper.stop() is called
		server.Serve(stopper)

		log4go.Info("Console stopped listening to http://127.0.0.1:%d/", port)
		log4go.Info("Console shutting down ...")
		return
	}()
}

func Run() {
	Start()
	shutdownWaitGroup.Wait()
	log4go.Info("Console shutdown complete")
}

//Stop will stop the console from running. Unfortunately there is not
//easy way to timeout this call. So if you need to stop no-matter-what
//you should call StopChan.
func Stop() {
	close(shutdownChannel)
	shutdownWaitGroup.Wait()
	log4go.Info("Console shutdown complete")
}

//StopTimeout will try to stop the server. It will return in no more than d-duration.
func StopTimeout(d time.Duration) bool {
	c := make(chan struct{})

	go func() {
		close(shutdownChannel)
		shutdownWaitGroup.Wait()
		close(c)
	}()

	log4go.Info("Console trying shutdown sequence")
	select {
	case <-c:
		log4go.Info("Console shutdown complete")
		return true
	case <-time.After(d):
		log4go.Info("Console shutdown NOT COMPLETE: waitgroup time out")
		return false
	}
}
