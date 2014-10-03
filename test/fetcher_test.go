// +build sudo

package test

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/iParadigms/walker"
	"github.com/stretchr/testify/mock"
)

const html_body string = `<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>Test norobots site</title>
</head>

<div id="menu">
	<a href="/dir1/">Dir1</a>
	<a href="/dir2/">Dir2</a>
	<a id="other" href="http://other.com/" title="stuff">Other</a>
</div>
</html>`

const html_body_nolinks string = `<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>No Links</title>
</head>
<div id="menu">
</div>
</html>`

func TestBasicFetchManagerRun(t *testing.T) {
	ds := &MockDatastore{}
	ds.On("ClaimNewHost").Return("norobots.com").Once()
	ds.On("LinksForHost", "norobots.com").Return([]*walker.URL{
		parse("http://norobots.com/page1.html"),
		parse("http://norobots.com/page2.html"),
		parse("http://norobots.com/page3.html"),
	})
	ds.On("UnclaimHost", "norobots.com").Return()

	ds.On("ClaimNewHost").Return("robotsdelay1.com").Once()
	ds.On("LinksForHost", "robotsdelay1.com").Return([]*walker.URL{
		parse("http://robotsdelay1.com/page4.html"),
		parse("http://robotsdelay1.com/page5.html"),
	})
	ds.On("UnclaimHost", "robotsdelay1.com").Return()

	ds.On("ClaimNewHost").Return("accept.com").Once()
	ds.On("LinksForHost", "accept.com").Return([]*walker.URL{
		parse("http://accept.com/accept_html.html"),
		parse("http://accept.com/accept_text.txt"),
		parse("http://accept.com/donthandle"),
	})
	ds.On("UnclaimHost", "accept.com").Return()

	// This last call will make ClaimNewHost return "" on each subsequent call,
	// which will put the fetcher to sleep.
	ds.On("ClaimNewHost").Return("")

	ds.On("StoreURLFetchResults", mock.AnythingOfType("*walker.FetchResults")).Return()
	ds.On("StoreParsedURL",
		mock.AnythingOfType("*walker.URL"),
		mock.AnythingOfType("*walker.FetchResults")).Return()

	h := &MockHandler{}
	h.On("HandleResponse", mock.Anything).Return()

	rs, err := NewMockRemoteServer()
	if err != nil {
		t.Fatal(err)
	}
	rs.SetResponse("http://norobots.com/robots.txt", &MockResponse{Status: 404})
	rs.SetResponse("http://norobots.com/page1.html", &MockResponse{
		Body: html_body,
	})
	rs.SetResponse("http://robotsdelay1.com/robots.txt", &MockResponse{
		Body: "User-agent: *\nCrawl-delay: 1\n",
	})

	walker.Config.AcceptFormats = []string{"text/html", "text/plain"}
	rs.SetResponse("http://accept.com/robots.txt", &MockResponse{Status: 404})
	rs.SetResponse("http://accept.com/accept_html.html", &MockResponse{
		ContentType: "text/html",
		Body:        html_body_nolinks,
	})
	rs.SetResponse("http://accept.com/accept_text.txt", &MockResponse{
		ContentType: "text/plain",
	})
	rs.SetResponse("http://accept.com/donthandle", &MockResponse{
		ContentType: "foo/bar",
	})
	manager := &walker.FetchManager{
		Datastore: ds,
		Handler:   h,
		Transport: GetFakeTransport(),
	}

	go manager.Start()
	time.Sleep(time.Second * 3)
	manager.Stop()

	rs.Stop()
	recvTextHtml := false
	recvTextPlain := false
	for _, call := range h.Calls {
		fr := call.Arguments.Get(0).(*walker.FetchResults)
		switch fr.URL.String() {
		case "http://norobots.com/page1.html":
			contents, _ := ioutil.ReadAll(fr.Response.Body)
			if string(contents) != html_body {
				t.Errorf("For %v, expected:\n%v\n\nBut got:\n%v\n",
					fr.URL, html_body, string(contents))
			}
		case "http://norobots.com/page2.html":
		case "http://norobots.com/page3.html":
		case "http://robotsdelay1.com/page4.html":
		case "http://robotsdelay1.com/page5.html":
		case "http://accept.com/accept_html.html":
			recvTextHtml = true
		case "http://accept.com/accept_text.txt":
			recvTextPlain = true
		default:
			t.Errorf("Got a Handler.HandleResponse call we didn't expect: %v", fr)
		}
	}
	if !recvTextHtml {
		t.Errorf("Failed to handle explicit Content-Type: text/html")
	}
	if !recvTextPlain {
		t.Errorf("Failed to handle Content-Type: text/plain")
	}

	ds.AssertExpectations(t)
	h.AssertExpectations(t)
}

func TestFetcherBlacklistsPrivateIPs(t *testing.T) {
	orig := walker.Config.BlacklistPrivateIPs
	defer func() { walker.Config.BlacklistPrivateIPs = orig }()
	walker.Config.BlacklistPrivateIPs = true

	ds := &MockDatastore{}
	ds.On("ClaimNewHost").Return("private.com").Once()
	ds.On("UnclaimHost", "private.com").Return()
	ds.On("ClaimNewHost").Return("")

	h := &MockHandler{}

	rs, err := NewMockRemoteServer()
	if err != nil {
		t.Fatal(err)
	}

	manager := &walker.FetchManager{
		Datastore: ds,
		Handler:   h,
		Transport: GetFakeTransport(),
	}

	go manager.Start()
	time.Sleep(time.Second * 1)
	manager.Stop()
	rs.Stop()

	if len(h.Calls) != 0 {
		t.Error("Did not expect any handler calls due to host resolving to private IP")
	}

	ds.AssertExpectations(t)
	h.AssertExpectations(t)
	ds.AssertNotCalled(t, "LinksForHost", "private.com")
}
