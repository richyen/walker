package test

import (
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"code.google.com/p/log4go"
	"github.com/iParadigms/walker"
)

func init() {
	setupCtrlCHandler()
}

// FakeDial makes connections to localhost, no matter what addr was given.
func FakeDial(network, addr string) (net.Conn, error) {
	_, port, _ := net.SplitHostPort(addr)
	return net.Dial(network, net.JoinHostPort("localhost", port))
}

// GetFakeTransport gets a http.RoundTripper that uses FakeDial
func GetFakeTransport() http.RoundTripper {
	return &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		Dial:                FakeDial,
		TLSHandshakeTimeout: 10 * time.Second,
	}
}

// parse is a helper to just get a URL object from a string we know is a safe
// url (ParseURL requires us to deal with potential errors)
func parse(ref string) *walker.URL {
	u, err := walker.ParseURL(ref)
	if err != nil {
		panic("Failed to parse walker.URL: " + ref)
	}
	return u
}

// urlParse is similar to `parse` but gives a Go builtin URL type (not a walker
// URL)
func urlParse(ref string) *url.URL {
	u, err := url.Parse(ref)
	if err != nil {
		panic("Failed to parse url.URL: " + ref)
	}
	return u
}

func response404() *http.Response {
	return &http.Response{
		Status:        "404",
		StatusCode:    404,
		Proto:         "HTTP/1.0",
		ProtoMajor:    1,
		ProtoMinor:    0,
		Header:        http.Header{"Content-Type": []string{"text/html"}},
		Body:          ioutil.NopCloser(strings.NewReader("")),
		ContentLength: -1,
	}
}

func response307(link string) *http.Response {
	return &http.Response{
		Status:        "307",
		StatusCode:    307,
		Proto:         "HTTP/1.0",
		ProtoMajor:    1,
		ProtoMinor:    0,
		Header:        http.Header{"Location": []string{link}, "Content-Type": []string{"text/html"}},
		Body:          ioutil.NopCloser(strings.NewReader("")),
		ContentLength: -1,
	}
}

func response200() *http.Response {
	return &http.Response{
		Status:     "200 OK",
		StatusCode: 200,
		Proto:      "HTTP/1.0",
		ProtoMajor: 1,
		ProtoMinor: 0,
		Header:     http.Header{"Content-Type": []string{"text/html"}},
		Body: ioutil.NopCloser(strings.NewReader(
			`<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>No Links</title>
</head>
<div id="menu">
</div>
</html>`)),
		ContentLength: -1,
	}
}

// mapRoundTrip maps input links --> http.Response. See TestRedirects for example.
type mapRoundTrip struct {
	responses map[string]*http.Response
}

func (mrt *mapRoundTrip) RoundTrip(req *http.Request) (*http.Response, error) {
	res, resOk := mrt.responses[req.URL.String()]
	if !resOk {
		return response404(), nil
	}
	return res, nil
}

//
// If you want your test to support exiting on Ctrl-C, call setupCtrlCHandler
// in your fixture. Use enableCtrlCHandler and disableCtrlCHandler to turn the
// exit handler on and off.
//
var initCtrlCHandler sync.Once
var disableCtrlCHandlerChan = make(chan chan string)

func setupCtrlCHandler() {
	initCtrlCHandler.Do(func() {
		stop := make(chan os.Signal)
		signal.Notify(stop, syscall.SIGINT)

		go func() {
			disabled := false
			for {
				select {
				case <-stop:
					if !disabled {
						log4go.Info("setupCtrlCHandler was asked to exit this process")
						os.Exit(1)
					}
				case c := <-disableCtrlCHandlerChan:
					m := <-c
					switch m {
					case "enable":
						disabled = false
						c <- "ok"
					case "disable":
						disabled = true
						c <- "ok"
					default:
						log4go.Error("Ctrl-C handler got an unknown message %q", m)
						c <- "ok"
					}
				}
			}
		}()
	})
}

func disableCtrlCHandler() {
	c := make(chan string)
	disableCtrlCHandlerChan <- c
	c <- "disable"
	<-c
	close(c)
}

func enableCtrlCHandler() {
	c := make(chan string)
	disableCtrlCHandlerChan <- c
	c <- "enable"
	<-c
	close(c)
}
