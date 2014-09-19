package test

import (
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/iParadigms/walker"
)

func TestSimpleWriterHandler(t *testing.T) {
	h := &walker.SimpleWriterHandler{}

	page1URL := parse("http://test.com/page1.html")
	page1Contents := []byte("<html>stuff</html>")
	page1Fetch := &walker.FetchResults{
		Url:              page1URL,
		Contents:         page1Contents,
		ExcludedByRobots: false,
		Res: &http.Response{
			Status:        "200 OK",
			StatusCode:    200,
			Proto:         "HTTP/1.1",
			ProtoMajor:    1,
			ProtoMinor:    1,
			ContentLength: 18,
			Request: &http.Request{
				Method:        "GET",
				URL:           page1URL.URL,
				Proto:         "HTTP/1.1",
				ProtoMajor:    1,
				ProtoMinor:    1,
				ContentLength: 18,
				Host:          "test.com",
			},
		},
	}

	h.HandleResponse(page1Fetch)
	file := "test.com/page1.html"
	contents, err := ioutil.ReadFile(file)
	if err != nil {
		t.Fatalf("Could not read expected file(%v): %v", file, err)
	}
	if string(contents) != string(page1Contents) {
		t.Errorf("Page contents not correctly written to file, expected %v\nBut got: %v",
			string(page1Contents), string(contents))
	}
	os.Remove(file)
}

func TestSimpleWriterHandlerIgnoresOnRobots(t *testing.T) {
	h := &walker.SimpleWriterHandler{}

	page2URL := parse("http://test.com/page2.html")
	page2Contents := []byte("<html>stuff</html>")
	page2Fetch := &walker.FetchResults{
		Url:              page2URL,
		Contents:         page2Contents,
		ExcludedByRobots: true,
		Res: &http.Response{
			Status:        "200 OK",
			StatusCode:    200,
			Proto:         "HTTP/1.1",
			ProtoMajor:    1,
			ProtoMinor:    1,
			ContentLength: 18,
			Request: &http.Request{
				Method:        "GET",
				URL:           page2URL.URL,
				Proto:         "HTTP/1.1",
				ProtoMajor:    1,
				ProtoMinor:    1,
				ContentLength: 18,
				Host:          "test.com",
			},
		},
	}

	h.HandleResponse(page2Fetch)
	file := "test.com-page2.html"
	_, err := ioutil.ReadFile(file)
	if err == nil {
		t.Errorf("File should not have been created due to robots.txt: %v", file)
	}
}

func TestSimpleWriterHandlerIgnoresBadHTTPCode(t *testing.T) {
	h := &walker.SimpleWriterHandler{}

	page3URL := parse("http://test.com/page3.html")
	page3Contents := []byte("<html>stuff</html>")
	page3Fetch := &walker.FetchResults{
		Url:              page3URL,
		Contents:         page3Contents,
		ExcludedByRobots: false,
		Res: &http.Response{
			Status:        "404 NOT FOUND",
			StatusCode:    404,
			Proto:         "HTTP/1.1",
			ProtoMajor:    1,
			ProtoMinor:    1,
			ContentLength: 18,
			Request: &http.Request{
				Method:        "GET",
				URL:           page3URL.URL,
				Proto:         "HTTP/1.1",
				ProtoMajor:    1,
				ProtoMinor:    1,
				ContentLength: 18,
				Host:          "test.com",
			},
		},
	}

	h.HandleResponse(page3Fetch)
	file := "test.com-page3.html"
	_, err := ioutil.ReadFile(file)
	if err == nil {
		t.Errorf("File should not have been created due http error code: %v", file)
	}
}