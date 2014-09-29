package console

import (
	"net/http"

	"code.google.com/p/log4go"
	"github.com/gorilla/mux"
	"github.com/unrolled/render"
)

//PERM HOME????
var rend = render.New(render.Options{
	Layout:        "layout",
	IndentJSON:    true,
	IsDevelopment: true,
})

type Route struct {
	Path    string
	Handler func(w http.ResponseWriter, req *http.Request)
}

func Routes() []Route {
	return []Route{
		Route{Path: "/", Handler: home},
		Route{Path: "/domain/", Handler: domainHandler},
		Route{Path: "/domain/{domain}", Handler: domainLookupHandler},
	}
}

func home(w http.ResponseWriter, req *http.Request) {
	rend.HTML(w, http.StatusOK, "home", nil)
}

func domainHandler(w http.ResponseWriter, req *http.Request) {
	domains, err := DS.ListLinkDomains()
	if err != nil {
		log4go.Error("Failed to get count of domains: %v", err)
		rend.HTML(w, http.StatusInternalServerError, "domain/index", nil)
		return
	}
	log4go.Info("Got %v", domains)
	rend.HTML(w, http.StatusOK, "domain/index", map[string]interface{}{"Domains": domains})
}

func domainLookupHandler(w http.ResponseWriter, req *http.Request) {
	var urls []UrlInfo
	vars := mux.Vars(req)
	domain := vars["domain"]

	urls, err := DS.LinksForDomain(domain)
	if err != nil {
		log4go.Error("Failed to get count of domains: %v", err)
		rend.HTML(w, http.StatusInternalServerError, "domain/info", nil)
		return
	}
	rend.HTML(w, http.StatusOK, "domain/info", map[string]interface{}{"Domain": domain, "Links": urls})
}
