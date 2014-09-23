// +build sudo

package test

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/iParadigms/walker"
	"github.com/stretchr/testify/mock"
)

const norobots_page1 string = `<!DOCTYPE html>
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

func TestBasicFetchManagerRun(t *testing.T) {
	ds := &MockDatastore{}
	ds.On("ClaimNewHost").Return("norobots.com").Once()
	ds.On("LinksForHost", "norobots.com").Return([]*walker.URL{
		parse("http://norobots.com/page1.html"),
		parse("http://norobots.com/page2.html"),
		parse("http://norobots.com/page3.html"),
	})
	ds.On("UnclaimHost", "norobots.com").Return()
	ds.On("ClaimNewHost").Return("robotsdelay1.com")
	ds.On("LinksForHost", "robotsdelay1.com").Return([]*walker.URL{
		parse("http://robotsdelay1.com/page4.html"),
		parse("http://robotsdelay1.com/page5.html"),
	})
	ds.On("UnclaimHost", "robotsdelay1.com").Return()

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
		Body: norobots_page1,
	})
	rs.SetResponse("http://robotsdelay1.com/robots.txt", &MockResponse{
		Body: "User-agent: *\nCrawl-delay: 1\n",
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

	for _, call := range h.Calls {
		fr := call.Arguments.Get(0).(*walker.FetchResults)
		switch fr.URL.String() {
		case "http://norobots.com/page1.html":
			contents, _ := ioutil.ReadAll(fr.Response.Body)
			if string(contents) != norobots_page1 {
				t.Errorf("For %v, expected:\n%v\n\nBut got:\n%v\n",
					fr.URL, norobots_page1, string(contents))
			}
		case "http://norobots.com/page2.html":
		case "http://norobots.com/page3.html":
		case "http://robotsdelay1.com/page4.html":
		case "http://robotsdelay1.com/page5.html":
		default:
			t.Errorf("Got a Handler.HandleResponse call we didn't expect: %v", fr)
		}
	}

	ds.AssertExpectations(t)
	h.AssertExpectations(t)
}
