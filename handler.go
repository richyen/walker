package walker

import (
	"os"
	"strings"

	"code.google.com/p/log4go"
)

// Handler defines the interface for objects that will be set as handlers on a
// CrawlManager.
type Handler interface {
	// HandleResponse will be called by fetchers as they make requests.
	// Handlers can do whatever they want with responses. HandleResponse will
	// be called as long as the request successfully reached the remote server
	// and got an HTTP code. This means there should never be a FetchError set
	// on the FetchResults.
	HandleResponse(res *FetchResults)
}

// SimpleWriterHandler just writes returned pages as files locally, naming the
// file after the URL of the request.
type SimpleWriterHandler struct{}

func (h *SimpleWriterHandler) HandleResponse(res *FetchResults) {
	if res.ExcludedByRobots {
		log4go.Debug("Excluded by robots.txt, ignoring url: %v", res.Url)
		return
	}
	if res.Res.StatusCode < 200 || res.Res.StatusCode >= 300 {
		log4go.Debug("Returned %v ignoring url: %v", res.Res.StatusCode, res.Url)
		return
	}

	fname := strings.TrimPrefix(res.Url.String(), res.Url.Scheme+"://")
	fname = strings.Replace(fname, "/", "-", -1)

	out, err := os.Create(fname)
	if err != nil {
		log4go.Error("Failed to create output file(%v): %v", fname, err)
		return
	}
	defer func() {
		err := out.Close()
		if err != nil {
			log4go.Error("Failed to close output file(%v): %v", fname, err)
		}
	}()
	_, err = out.Write(res.Contents)
	if err != nil {
		log4go.Error("Failed to write all of file(%v): %v", fname, err)
		return
	}
}
