package console

import (
	"fmt"
	"net/http"

	"code.google.com/p/log4go"
	"github.com/gorilla/mux"
	"github.com/unrolled/render"
)

var renderer = render.New(render.Options{
	Layout:        "layout",
	IndentJSON:    true,
	IsDevelopment: true,
})

func doRender(w http.ResponseWriter, template string, keyValues ...interface{}) {
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
	renderer.HTML(w, http.StatusOK, template, mp)
}

type Route struct {
	Path    string
	Handler func(w http.ResponseWriter, req *http.Request)
}

func Routes() []Route {
	return []Route{
		Route{Path: "/", Handler: home},
		Route{Path: "/domain", Handler: domainHandler},
		Route{Path: "/domain/{domain}", Handler: domainLookupHandler},
	}
}

func home(w http.ResponseWriter, req *http.Request) {
	doRender(w, "home")
}

func domainHandler(w http.ResponseWriter, req *http.Request) {
	domains, err := DS.ListLinkDomains()
	if err != nil {
		log4go.Error("Failed to get count of domains: %v", err)
		renderer.HTML(w, http.StatusInternalServerError, "domain/index", nil)
		return
	}
	log4go.Info("Got %v", domains)
	doRender(w, "domain/index", "Domains", domains)
}

func domainLookupHandler(w http.ResponseWriter, req *http.Request) {
	var urls []UrlInfo
	vars := mux.Vars(req)
	domain := vars["domain"]

	urls, err := DS.LinksForDomain(domain)
	if err != nil {
		log4go.Error("Failed to get count of domains: %v", err)
		renderer.HTML(w, http.StatusInternalServerError, "domain/info", nil)
		return
	}
	doRender(w, "domain/info", "Domain", domain, "Links", urls)
}
