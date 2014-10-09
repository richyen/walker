package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"code.google.com/p/log4go"
	"github.com/codegangsta/negroni"
	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
	"github.com/iParadigms/walker"
	"github.com/iParadigms/walker/console"
)

func fakeCrawlTime() time.Time {
	t := time.Now().AddDate(-rand.Intn(2), -rand.Intn(6), -rand.Intn(7))
	return t
}

var statusSelect []int = []int{
	http.StatusContinue,
	http.StatusSwitchingProtocols,
	http.StatusOK,
	http.StatusCreated,
	http.StatusAccepted,
	http.StatusNonAuthoritativeInfo,
	http.StatusNoContent,
	http.StatusResetContent,
	http.StatusPartialContent,
	http.StatusMultipleChoices,
	http.StatusMovedPermanently,
	http.StatusFound,
	http.StatusSeeOther,
	http.StatusNotModified,
	http.StatusUseProxy,
	http.StatusTemporaryRedirect,
	http.StatusBadRequest,
	http.StatusUnauthorized,
	http.StatusPaymentRequired,
	http.StatusForbidden,
	http.StatusNotFound,
	http.StatusMethodNotAllowed,
	http.StatusNotAcceptable,
	http.StatusProxyAuthRequired,
	http.StatusRequestTimeout,
	http.StatusConflict,
	http.StatusGone,
	http.StatusLengthRequired,
	http.StatusPreconditionFailed,
	http.StatusRequestEntityTooLarge,
	http.StatusRequestURITooLong,
	http.StatusUnsupportedMediaType,
	http.StatusRequestedRangeNotSatisfiable,
	http.StatusExpectationFailed,
	http.StatusInternalServerError,
	http.StatusNotImplemented,
	http.StatusBadGateway,
	http.StatusServiceUnavailable,
	http.StatusGatewayTimeout,
	http.StatusHTTPVersionNotSupported,
}

func fakeStatus() int {
	if rand.Float32() < 0.8 {
		return http.StatusOK
	} else {
		return statusSelect[rand.Intn(len(statusSelect))]
	}
}

var initUuids sync.Once
var selectUuids []gocql.UUID

func fakeUuid() gocql.UUID {
	initUuids.Do(func() {
		for i := 0; i < 5; i++ {
			u, err := gocql.RandomUUID()
			if err != nil {
				panic(err)
			}
			selectUuids = append(selectUuids, u)
		}
	})

	return selectUuids[rand.Intn(len(selectUuids))]
}

func spoofDs() (ds *console.CqlDataStore) {
	walker.Config.Cassandra.Keyspace = "walker_test"
	walker.Config.Cassandra.Hosts = []string{"localhost"}
	walker.Config.Cassandra.ReplicationFactor = 1

	err := walker.CreateCassandraSchema()
	if err != nil {
		panic(err)
	}

	ds, err = console.NewCqlDataStore()
	if err != nil {
		panic(fmt.Errorf("Failed to start data source: %v", err))
	}
	db := ds.Db

	//
	// Clear out the tables first
	//
	tables := []string{"links", "segments", "domain_info", "domains_to_crawl"}
	for _, table := range tables {
		err := db.Query(fmt.Sprintf(`TRUNCATE %v`, table)).Exec()
		if err != nil {
			panic(fmt.Errorf("Failed to truncate table %v: %v", table, err))
		}
	}

	rand.Seed(42)

	insertDomainInfo := `INSERT INTO domain_info (domain, excluded, exclude_reason, mirror_for) VALUES (?, ?, ?, ?)`
	insertDomainToCrawl := `INSERT INTO domains_to_crawl (domain, crawler_token, priority, claim_time) VALUES (?, ?, ?, ?)`
	insertSegment := `INSERT INTO segments (domain, subdomain, path, protocol) VALUES (?, ?, ?, ?)`
	insertLink := `INSERT INTO links (domain, subdomain, path, protocol, crawl_time, status, error, robots_excluded) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

	for i := 0; i < 100; i++ {
		domain := fmt.Sprintf("x%d.com", i)
		err := db.Query(insertDomainInfo, domain, false, "", "").Exec()
		if err != nil {
			panic(err)
		}
		crawlTime := fakeCrawlTime()
		status := fakeStatus()
		excluded := false
		if rand.Float32() < 0.1 {
			status = http.StatusOK
			crawlTime = walker.NotYetCrawled
			excluded = true
		}
		err = db.Query(insertLink, domain, "subd", "/page1.html", "http", crawlTime, status, "", excluded).Exec()
		if err != nil {
			panic(err)
		}
	}

	excludeBC := []string{
		"Don't like cut of jib",
		"Think you smell funny",
		"Didn't have permissions to access",
		"Because I said so",
	}

	for i := 0; i < 10; i++ {
		domain := fmt.Sprintf("y%d.com", i)
		excluded := false
		excludeReason := ""
		if rand.Float32() < 0.1 {
			excluded = true
			excludeReason = excludeBC[rand.Intn(len(excludeBC))]
		}
		err := db.Query(insertDomainInfo, domain, excluded, excludeReason, "").Exec()
		if err != nil {
			panic(err)
		}
		if excluded {
			continue
		}

		for i := 0; i < 100; i++ {
			crawlTime := fakeCrawlTime()
			status := fakeStatus()
			excluded = false
			if rand.Float32() < 0.1 {
				status = http.StatusOK
				crawlTime = walker.NotYetCrawled
				excluded = true
			}
			page := fmt.Sprintf("/page%d.html", i)
			err = db.Query(insertLink, domain, "link", page, "http", crawlTime, status, "", excluded).Exec()
			if err != nil {
				panic(err)
			}
		}
	}

	errorBC := []string{
		"Something very bad happened",
		"Program failed to parse message 5",
		"All your base are belong to us",
		"The Tragically Hip sensor failed",
	}

	for i := 0; i < 10; i++ {
		domain := fmt.Sprintf("h%d.com", i)
		err := db.Query(insertDomainInfo, domain, false, "", "").Exec()
		if err != nil {
			panic(err)
		}

		crawlTime := time.Now()
		for i := 0; i < 20; i++ {
			crawlTime = crawlTime.AddDate(0, 0, -rand.Intn(30))
			status := fakeStatus()
			fakeError := ""
			if rand.Float32() < 0.1 {
				status = http.StatusOK
				fakeError = errorBC[rand.Intn(len(errorBC))]
			}
			err = db.Query(insertLink, domain, "link", "/page1.html", "http", crawlTime, status, fakeError, false).Exec()
			if err != nil {
				panic(err)
			}
		}
	}

	for i := 0; i < 10; i++ {
		domain := fmt.Sprintf("t%d.com", i)
		uuid := fakeUuid()
		err := db.Query(insertDomainInfo, domain, false, "", "").Exec()
		if err != nil {
			panic(err)
		}
		for i := 0; i < 20; i++ {
			page := fmt.Sprintf("/page%d.html", i)
			err = db.Query(insertLink, domain, "link", page, "http", walker.NotYetCrawled, http.StatusOK, "", false).Exec()
			if err != nil {
				panic(err)
			}
			err = db.Query(insertDomainToCrawl, domain, uuid, 0, time.Now()).Exec()
			if err != nil {
				panic(err)
			}

			err = db.Query(insertSegment, domain, "", page, "http").Exec()
			if err != nil {
				panic(err)
			}
		}
	}

	return
}

func main() {
	var ds *console.CqlDataStore
	if true {
		ds = spoofDs()
	} else {
		var err error
		ds, err = console.NewCqlDataStore()
		if err != nil {
			panic(fmt.Errorf("Failed to start data source: %v", err))
		}
	}

	console.DS = ds
	defer ds.Close()

	router := mux.NewRouter()
	routes := console.Routes()
	for _, route := range routes {
		log4go.Info("Registering path %s", route.Path)
		router.HandleFunc(route.Path, route.Controller)
	}

	n := negroni.New(negroni.NewRecovery(), negroni.NewLogger(), negroni.NewStatic(http.Dir("public")))
	n.UseHandler(router)
	n.Run(":3000")
}
