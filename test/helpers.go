package test

import (
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/iParadigms/walker"
)

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
