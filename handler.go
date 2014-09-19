package walker

import (
	"os"
	"path/filepath"

	"code.google.com/p/log4go"
)

// Handler defines the interface for objects that will be set as handlers on a
// FetchManager.
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

	path := filepath.Join(res.Url.Host, res.Url.RequestURI())
	dir, _ := filepath.Split(path)
	if err := os.MkdirAll(dir, 0777); err != nil {
		log4go.Error(err.Error())
		return
	}

	out, err := os.Create(path)
	if err != nil {
		log4go.Error(err.Error())
		return
	}
	defer func() {
		err := out.Close()
		if err != nil {
			log4go.Error(err.Error())
		}
	}()
	_, err = out.Write(res.Contents)
	if err != nil {
		log4go.Error(err.Error())
		return
	}
}
