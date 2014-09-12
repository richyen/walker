package test

import (
	"net"
	"net/http"
	"net/url"
	"path"
	"runtime"
	"time"

	"github.com/iParadigms/walker"
)

func init() {
	loadTestConfig("test-walker.yaml")
}

// loadTestConfig loads the given test config yaml file. The given path is
// assumed to be relative to the `walker/test/` directory, the location of this
// test file.
func loadTestConfig(filename string) {
	_, thisname, _, ok := runtime.Caller(0)
	if !ok {
		panic("Failed to get location of test source file")
	}
	walker.ReadConfigFile(path.Join(path.Dir(thisname), filename))
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
// url (url.Parse requires us to deal with potential errors)
func parse(link string) *url.URL {
	u, _ := url.Parse(link)
	return u
}
