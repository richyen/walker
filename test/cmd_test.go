package test

import (
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/iParadigms/walker"
	"github.com/stretchr/testify/mock"
)

func TestCrawlCommand(t *testing.T) {
	handler := &MockHandler{}
	walker.Cmd.Handler = handler

	datastore := &MockDatastore{}
	datastore.On("ClaimNewHost").Return("")
	walker.Cmd.Datastore = datastore

	dispatcher := &MockDispatcher{}
	dispatcher.On("StartDispatcher").Return(nil)
	dispatcher.On("StopDispatcher").Return(nil)
	walker.Cmd.Dispatcher = dispatcher

	orig := os.Args
	defer func() { os.Args = orig }()
	os.Args = []string{os.Args[0], "crawl"}

	go func() {
		time.Sleep(5 * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGINT)
	}()
	walker.Cmd.Execute()

	handler.AssertExpectations(t)
	datastore.AssertExpectations(t)
	dispatcher.AssertExpectations(t)
}

func TestFetchCommand(t *testing.T) {
	handler := &MockHandler{}
	walker.Cmd.Handler = handler

	datastore := &MockDatastore{}
	datastore.On("ClaimNewHost").Return("")
	walker.Cmd.Datastore = datastore

	// Set the dispatcher to ensure it doesn't receive any calls
	dispatcher := &MockDispatcher{}
	walker.Cmd.Dispatcher = dispatcher

	orig := os.Args
	defer func() { os.Args = orig }()
	os.Args = []string{os.Args[0], "fetch"}

	go func() {
		time.Sleep(5 * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGINT)
	}()
	walker.Cmd.Execute()

	handler.AssertExpectations(t)
	datastore.AssertExpectations(t)
	dispatcher.AssertExpectations(t)
}

func TestDispatchCommand(t *testing.T) {
	// Set a handler and datastore to ensure they doesn't receive any calls
	handler := &MockHandler{}
	walker.Cmd.Handler = handler

	datastore := &MockDatastore{}
	walker.Cmd.Datastore = datastore

	dispatcher := &MockDispatcher{}
	dispatcher.On("StartDispatcher").Return(nil)
	dispatcher.On("StopDispatcher").Return(nil)
	walker.Cmd.Dispatcher = dispatcher

	orig := os.Args
	defer func() { os.Args = orig }()
	os.Args = []string{os.Args[0], "dispatch"}

	go func() {
		time.Sleep(5 * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGINT)
	}()
	walker.Cmd.Execute()

	handler.AssertExpectations(t)
	datastore.AssertExpectations(t)
	dispatcher.AssertExpectations(t)
}

func TestSeedCommand(t *testing.T) {
	u, _ := walker.ParseURL("http://test.com")
	datastore := &MockDatastore{}
	datastore.On("StoreParsedURL", u, mock.AnythingOfType("*walker.FetchResults")).Return("")
	walker.Cmd.Datastore = datastore

	orig := os.Args
	defer func() { os.Args = orig }()
	os.Args = []string{os.Args[0], "seed", "--url=" + u.String()}

	go func() {
		time.Sleep(5 * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGINT)
	}()
	walker.Cmd.Execute()

	datastore.AssertExpectations(t)
}
