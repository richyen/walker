package test

import (
	"net"
	"net/http"
	"net/url"
	"time"
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
// url (url.Parse requires us to deal with potential errors)
func parse(link string) *url.URL {
	u, _ := url.Parse(link)
	return u
}
