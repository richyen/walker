package main

import (
	"fmt"
	"net/http"
	"time"

	"code.google.com/p/log4go"
	"github.com/codegangsta/negroni"
	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
	"github.com/unrolled/render"
)

var walkerDataStore, _ = NewDataStore("localhost")

type DataStore struct {
	cluster *gocql.ClusterConfig
	db      *gocql.Session
}

func NewDataStore(host string) (ds *DataStore, err error) {
	ds = new(DataStore)
	ds.cluster = gocql.NewCluster(host)
	ds.cluster.Keyspace = "walker"
	ds.db, err = ds.cluster.CreateSession()
	return
}

func (ds DataStore) Close() {
	ds.db.Close()
}

type urlInfo struct {
	Link      string
	CrawledOn time.Time
}

func main() {
	defer walkerDataStore.Close()
	rend := render.New(render.Options{
		Layout:        "layout",
		IndentJSON:    true,
		IsDevelopment: true,
	})
	router := mux.NewRouter()
	router.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		rend.HTML(w, http.StatusOK, "home", nil)
	})

	router.HandleFunc("/domain", func(w http.ResponseWriter, req *http.Request) {
		// NOTE: this is a horribly inefficient way of doing this, but at the moment it works
		var domains []string
		var domain string
		i := walkerDataStore.db.Query(`SELECT distinct domain FROM links`).Iter()
		for i.Scan(&domain) {
			domains = append(domains, domain)
		}
		err := i.Close()
		if err != nil {
			log4go.Error("Failed to get count of domains: %v", err)
			rend.HTML(w, http.StatusInternalServerError, "domain/index", nil)
			return
		}
		log4go.Info("Got %v", domains)
		rend.HTML(w, http.StatusOK, "domain/index", map[string]interface{}{"Domains": domains})
	})

	router.HandleFunc("/domain/{domain}", func(w http.ResponseWriter, req *http.Request) {
		var urls []urlInfo
		vars := mux.Vars(req)
		domain := vars["domain"]
		i := walkerDataStore.db.Query(
			`SELECT domain, subdomain, path, protocol, crawl_time
			FROM links WHERE domain = ?
			ORDER BY subdomain, path, protocol, crawl_time`,
			domain,
		).Iter()
		var linkdomain, subdomain, path, protocol string
		var crawl_time time.Time
		for i.Scan(&linkdomain, &subdomain, &path, &protocol, &crawl_time) {
			if subdomain != "" {
				subdomain = subdomain + "."
			}
			url := urlInfo{
				Link:      fmt.Sprintf("%s://%s%s/%s", protocol, subdomain, linkdomain, path),
				CrawledOn: crawl_time,
			}
			urls = append(urls, url)
		}
		err := i.Close()
		if err != nil {
			log4go.Error("Failed to get count of domains: %v", err)
			rend.HTML(w, http.StatusInternalServerError, "domain/info", nil)
			return
		}
		rend.HTML(w, http.StatusOK, "domain/info", map[string]interface{}{"Domain": domain, "Links": urls})
	})

	n := negroni.New(negroni.NewRecovery(), negroni.NewLogger(), negroni.NewStatic(http.Dir("public")))
	n.UseHandler(router)

	n.Run(":3000")
}
