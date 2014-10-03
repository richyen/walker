package console

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"code.google.com/p/log4go"
	"github.com/gorilla/mux"
	"github.com/unrolled/render"
)

var DS DataStore

var renderer = render.New(render.Options{
	Layout:        "layout",
	IndentJSON:    true,
	IsDevelopment: true,
})

func doRenderFull(w http.ResponseWriter, template string, status int, keyValues ...interface{}) {
	if len(keyValues)%2 != 0 {
		panic(fmt.Errorf("INTERNAL ERROR: poorly used doRender: keyValues does not have even number of elements"))
	}
	mp := map[string]interface{}{}
	for i := 0; i < len(keyValues); i = i + 2 {
		protokey := keyValues[i]
		key, keyok := protokey.(string)
		if !keyok {
			panic(fmt.Errorf("INTERNAL ERROR: poorly used doRender: found a non-string in keyValues"))
		}
		value := keyValues[i+1]
		mp[key] = value
	}
	renderer.HTML(w, status, template, mp)
}

func doRender(w http.ResponseWriter, template string, keyValues ...interface{}) {
	doRenderFull(w, template, http.StatusOK, keyValues...)
}

type Route struct {
	Path    string
	Handler func(w http.ResponseWriter, req *http.Request)
}

func Routes() []Route {
	return []Route{
		Route{Path: "/", Handler: home},
		Route{Path: "/domains", Handler: listDomainsHandler},
		Route{Path: "/domains/", Handler: listDomainsHandler},
		Route{Path: "/domains/{seed}", Handler: listDomainsHandler},
		Route{Path: "/addLink", Handler: addLinkIndexHandler},
	}
}

func renderServerError(w http.ResponseWriter, err error) {
	log4go.Error("Rendering 500: %v", err)
	doRenderFull(w, "serverError", http.StatusInternalServerError,
		"anErrorHappend", true,
		"theError", err.Error())
}

func home(w http.ResponseWriter, req *http.Request) {
	doRender(w, "home")
}

func listDomainsHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	seed := vars["seed"]
	if seed == "" {
		seed = DontSeedDomain
	} else {
		var err error
		seed, err = url.QueryUnescape(seed)
		if err != nil {
			seed = DontSeedDomain
		}
	}

	dinfos, err := DS.ListDomains(seed, PageWindowLength)
	if err != nil {
		err = fmt.Errorf("ListDomains failed: %v", err)
		renderServerError(w, err)
		return
	}

	nextDomain := ""
	hasNext := false
	if len(dinfos) == PageWindowLength {
		nextDomain = url.QueryEscape(dinfos[len(dinfos)-1].Domain)
		hasNext = true
	}
	doRender(w, "domains",
		"Domains", dinfos,
		"HasNext", hasNext,
		"Next", nextDomain)
}

type UrlInfo struct {
	// url string
	Link string

	// when the url was last crawled (could be zero for uncrawled url)
	CrawledOn time.Time
}

func domainLookupHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	domain := vars["domain"]

	linfos, err := DS.ListLinks(domain, DontSeedUrl, 0)
	if err != nil {
		log4go.Error("Failed to get count of domains: %v", err)
		renderer.HTML(w, http.StatusInternalServerError, "domain/info", nil)
		return
	}
	//XXX: eventually the template will use the linfos directly: this is temporary
	var urls []UrlInfo
	for _, l := range linfos {
		urls = append(urls, UrlInfo{Link: l.Url, CrawledOn: l.CrawlTime})
	}
	doRender(w, "domain/info",
		"Domain", domain,
		"Links", urls)
}

func addLinkIndexHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method == "POST" {
		err := req.ParseForm()
		if err != nil {
			log4go.Info("Failed to parse form in addLink %v", err)
		} else {
			linksExt, ok := req.Form["links"]
			if !ok {
				log4go.Info("Failed to find 'links' in form submission")
			} else {
				lines := strings.Split(linksExt[0], "\n")
				links := make([]string, 0, len(lines))
				for i := range lines {
					t := strings.TrimSpace(lines[i])
					if t != "" {
						links = append(links, t)
					}
				}
				for _, l := range links {
					log4go.Info("LINK ENTER: %v", l)
				}
			}
		}
	}
	doRender(w, "addLink")
}
