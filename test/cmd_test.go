package test

import (
	"io/ioutil"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/iParadigms/walker"
	"github.com/stretchr/testify/mock"
)

var WalkerCommands = []string{"crawl", "fetch", "dispatch", "seed"}

func TestCommandsReadConfig(t *testing.T) {
	orig := os.Args
	defer func() {
		os.Args = orig
		// Reset config for the remaining tests
		loadTestConfig("test-walker.yaml")
	}()

	handler := &MockHandler{}
	walker.Cmd.Handler = handler

	datastore := &MockDatastore{}
	datastore.On("ClaimNewHost").Return("")
	datastore.On("ClaimNewHost").Return("")
	datastore.On("StoreParsedURL", mock.Anything, mock.Anything).Return()
	walker.Cmd.Datastore = datastore

	dispatcher := &MockDispatcher{}
	dispatcher.On("StartDispatcher").Return(nil)
	dispatcher.On("StopDispatcher").Return(nil)
	walker.Cmd.Dispatcher = dispatcher

	for _, cmd := range WalkerCommands {
		loadTestConfig("test-walker.yaml")
		expectedDefaultAgent := "Walker (http://github.com/iParadigms/walker)"
		if walker.Config.UserAgent != expectedDefaultAgent {
			t.Fatalf("Failed to reset default config value (user_agent), expected: %v\nBut got: %v",
				expectedDefaultAgent, walker.Config.UserAgent)
		}

		switch cmd {
		case "seed":
			os.Args = []string{os.Args[0], cmd, "--url=http://test.com", "--config=test-walker2.yaml"}
		default:
			os.Args = []string{os.Args[0], cmd, "--config=test-walker2.yaml"}
		}

		go func() {
			time.Sleep(5 * time.Millisecond)
			syscall.Kill(os.Getpid(), syscall.SIGINT)
		}()
		walker.Cmd.Execute()

		expectedTestAgent := "Test Agent (set in yaml)"
		if walker.Config.UserAgent != expectedTestAgent {
			t.Errorf("Failed to set config value (user_agent) via yaml, expected: %v\nBut got: %v",
				expectedTestAgent, walker.Config.UserAgent)
		}
	}
}

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

func TestSchemaCommand(t *testing.T) {
	orig := os.Args
	defer func() { os.Args = orig }()
	os.Args = []string{os.Args[0], "schema", "--out=test.cql"}
	walker.Cmd.Execute()
	defer os.Remove("test.cql")

	f, err := ioutil.ReadFile("test.cql")
	if err != nil {
		t.Fatalf("Failed to read test.cql: %v", err)
	}
	if !strings.HasPrefix(string(f), "-- The schema file for walker") {
		t.Fatalf("test.cql has unexpected contents: %v", f)
	}
}
