package test

import (
	"net/url"

	"testing"
	"time"

	"github.com/iParadigms/walker"
	"github.com/stretchr/testify/mock"
)

func TestBasicRun(t *testing.T) {
	ds := &MockDatastore{}
	ds.On("ClaimNewHost").Return("norobots.com").Once()
	ds.On("LinksForHost", "norobots.com").Return([]*url.URL{
		parse("http://norobots.com/page1.html"),
		parse("http://norobots.com/page2.html"),
		parse("http://norobots.com/page3.html"),
	})
	ds.On("ClaimNewHost").Return("robotsdelay2.com")
	ds.On("LinksForHost", "robotsdelay2.com").Return([]*url.URL{
		parse("http://robotsdelay2.com/page4.html"),
		parse("http://robotsdelay2.com/page5.html"),
		parse("http://robotsdelay2.com/page6.html"),
	})

	ds.On("StoreURLFetchResults", mock.AnythingOfType("*walker.FetchResults")).Return()
	ds.On("StoreParsedURL",
		mock.AnythingOfType("*url.URL"),
		mock.AnythingOfType("*walker.FetchResults")).Return()

	h := &MockHandler{}
	h.On("HandleResponse", mock.Anything).Return()

	rs, err := NewMockRemoteServer()
	if err != nil {
		t.Fatal(err)
	}
	rs.SetResponse("http://norobots.com/robots.txt", &MockResponse{Status: 404})
	rs.SetResponse("http://norobots.com/page1.html", &MockResponse{
		Body: `<!DOCTYPE html>
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
</html>`,
	})
	rs.SetResponse("http://robotsdelay2.com/robots.txt", &MockResponse{
		Body: "User-agent: *\nCrawl-delay: 2\n",
	})

	manager := walker.NewCrawlManager()
	manager.SetDatastore(ds)
	manager.AddHandler(h)
	manager.Transport = GetFakeTransport()
	go manager.Start()
	time.Sleep(time.Millisecond * 4)
	manager.Stop()
	rs.Stop()

	ds.AssertExpectations(t)
	h.AssertExpectations(t)
}
