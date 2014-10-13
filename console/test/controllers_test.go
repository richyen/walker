package test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PuerkitoBio/goquery"
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

var schemaCreate sync.Once
var spoofRun sync.Once

func spoofData() {
	spoofRun.Do(func() {
		spoofDataLong()
	})
}

func spoofDataLong() {
	schemaCreate.Do(func() {
		walker.Config.Cassandra.Keyspace = "walker_test"
		walker.Config.Cassandra.Hosts = []string{"localhost"}
		walker.Config.Cassandra.ReplicationFactor = 1
		walker.Config.Console.TemplateDirectory = "../templates"

		console.BuildRender()
		err := walker.CreateCassandraSchema()
		if err != nil {
			panic(err)
		}
	})

	ds, err := console.NewCqlDataStore()
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

	//DS is used in the handlers. Notice that in the current incarnation, this handle
	//is never closed. Just like the real application
	console.DS = ds

	return
}

var htmlBody string = `
<!DOCTYPE html>
<html>
<body>

<h1>My First Heading</h1>

<p>My first paragraph.</p>

</body>
</html>
`

func callController(url string, body string, urlPattern string, controller func(w http.ResponseWriter, req *http.Request)) (*goquery.Document, string, int) {
	var bodyBuff io.Reader = nil
	method := "GET"
	ct := ""
	if body != "" {
		bodyBuff = bytes.NewBufferString(body)
		method = "POST"
		ct = "application/x-www-form-urlencoded; param=value"
	}
	req, err := http.NewRequest(method, url, bodyBuff)
	if err != nil {
		panic(err)
	}
	if ct != "" {
		req.Header.Set("Content-Type", ct)
	}

	// Need to build a router to get the Gorrilla mux Vars correct
	router := mux.NewRouter()
	router.HandleFunc(urlPattern, controller)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	status := w.Code
	output := w.Body.String()

	outputReader := strings.NewReader(output)
	doc, err := goquery.NewDocumentFromReader(outputReader)
	if err != nil {
		panic(err)
	}

	return doc, output, status
}

func TestLayout(t *testing.T) {
	spoofData()
	doc, body, status := callController("http://localhost:3000/", "", "/", console.HomeController)
	if status != http.StatusOK {
		t.Errorf("TestHome bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.Fail()
	}

	// Make sure the main menu is there
	mainLinks := map[string]string{
		"/list":      "List",
		"/find":      "Find Domains",
		"/findLinks": "Find Links",
		"/add":       "Add",
	}
	doc.Find("nav ul li a").Each(func(index int, sel *goquery.Selection) {
		link, linkOk := sel.Attr("href")
		if !linkOk {
			t.Errorf("[nav ul li a] Failed to find href attribute in main menu list")
			return
		}
		text := strings.TrimSpace(sel.Text())
		found, foundOk := mainLinks[link]

		if !foundOk {
			t.Errorf("[nav ul li a] Failed to find link '%s' in menu list", link)
			return
		}

		if found != text {
			t.Errorf("[nav ul li a] Failed to find text '%s' for link %s", text, link)
			return
		}

		delete(mainLinks, link)
	})
	for k, v := range mainLinks {
		t.Errorf("[nav ul li a] Unfound link %v (%v)", k, v)
	}

	cssLinks := map[string]bool{
		"/css/bootstrap.css": true,
		"/css/custom.css":    true,
	}
	if doc.Find("head link").Size() <= 0 {
		t.Errorf("[nav ul li a] Failed to find anything")
	}
	doc.Find("head link").Each(func(index int, sel *goquery.Selection) {
		link, linkOk := sel.Attr("href")
		if !linkOk {
			t.Errorf("[head link] Failed to find href")
			return
		}
		if !cssLinks[link] {
			t.Errorf("[head link] Failed to find link %s", link)
			return
		}

		delete(cssLinks, link)
	})
	for k, v := range mainLinks {
		t.Errorf("[head link] Unfound link %v (%v)", k, v)
	}

	jsLinks := map[string]bool{
		"/js/jquery-2.1.1.js": true,
		"/js/bootstrap.js":    true,
	}
	doc.Find("head script").Each(func(index int, sel *goquery.Selection) {
		link, linkOk := sel.Attr("src")
		if !linkOk {
			t.Errorf("[head script] Failed to find src")
			return
		}
		if !jsLinks[link] {
			t.Errorf("[head script] Failed to find link %s", link)
			return
		}

		delete(jsLinks, link)
	})
	for k, v := range mainLinks {
		t.Errorf("[head script] Unfound link %v (%v)", k, v)
	}
}

func TestHome(t *testing.T) {
	spoofData()
	doc, body, status := callController("http://localhost:3000/", "", "/", console.HomeController)
	if status != http.StatusOK {
		t.Errorf("TestHome bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.Fail()
	}

	numPP := doc.Find(".container p").Size()
	if numPP != 1 {
		t.Errorf("[.container p] Expected 1 paragraph, found %d", numPP)
	}
	doc.Find(".container p").Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		if !strings.Contains(text, "Walker Console") {
			t.Errorf("[.container p] Expected string containing Walker Console: Got '%v'", text)
		}
	})
}

func TestListDomains(t *testing.T) {
	spoofData()
	doc, body, status := callController("http://localhost:3000/list", "", "/list", console.ListDomainsController)
	if status != http.StatusOK {
		t.Errorf("TestListDomains bad status code got %d, expected %d", status, http.StatusOK)
		body = ""
		t.Log(body)
		t.Fail()
	}
	header := []string{
		"Domain",
		"LinksTotal",
		"LinksQueued",
		"Excluded",
		"TimeQueued",
	}
	failed := false
	doc.Find(".container table thead td").Each(func(index int, sel *goquery.Selection) {
		if failed {
			return
		}

		text := strings.TrimSpace(sel.Text())
		if len(header) == 0 || text != header[0] {
			e := ""
			if len(header) > 0 {
				e = header[0]
			}
			t.Errorf("[.container table thead td] Bad order got '%v' expected '%v'", text, e)
			failed = true
			return
		}
		header = header[1:]
	})

	failed = false
	count := 0
	doc.Find(".container table tbody tr td a").Each(func(index int, sel *goquery.Selection) {
		if failed {
			return
		}

		link, linkOk := sel.Attr("href")
		if !linkOk {
			t.Errorf("[.container table tbody tr td a] Failed to find href")
			failed = true
			return
		}
		text := strings.TrimSpace(sel.Text())
		elink := "/links/" + text
		if elink != link {
			t.Errorf("[.container table tbody tr td a] link mismatch expected '%v' got '%v'", elink, link)
			failed = true
			return
		}
		count++
	})

	minCount := 10
	if !failed && count < minCount {
		t.Errorf("[.container table tbody tr td a] Had less than %d elements", minCount)
	}
}

func TestListLinks(t *testing.T) {
	spoofData()
	doc, body, status := callController("http://localhost:3000/links/t1.com", "", "/links/{domain}", console.LinksController)
	if status != http.StatusOK {
		t.Errorf("TestListLinks bad status code got %d, expected %d", status, http.StatusOK)
		body = ""
		t.Log(body)
		t.Fail()
	}

	// Sanity check headers
	h2 := []string{
		"Domain information for t1.com",
		"Links for domain t1.com",
	}
	failed := false
	doc.Find(".container h2").Each(func(index int, sel *goquery.Selection) {
		if failed {
			return
		}

		text := strings.TrimSpace(sel.Text())
		if len(h2) == 0 || text != h2[0] {
			e := ""
			if len(h2) > 0 {
				e = h2[0]
			}
			t.Errorf("[.container h2] Failed got '%s', expected '%s'", text, e)
			failed = true
		}

		h2 = h2[1:]
	})

	// Nab the tables
	tables := doc.Find(".container table")
	if tables.Size() != 2 {
		t.Fatalf("[.container table] Bad size got %d, expected %d", tables.Size(), 2)
	}
	domainTable := tables.First()
	linksTable := tables.Last()

	//
	// Domain section
	//
	domainKeys := []string{
		"Domain",
		"ExcludeReason",
		"TimeQueued",
		"UuidOfQueued",
		"NumberLinksTotal",
		"NumberLinksQueued",
	}

	failed = false
	domainTable.Find("tr > td:nth-child(1)").Each(func(index int, sel *goquery.Selection) {
		if failed {
			return
		}

		text := strings.TrimSpace(sel.Text())
		if len(domainKeys) == 0 || text != domainKeys[0] {
			e := ""
			if len(domainKeys) > 0 {
				e = domainKeys[0]
			}
			t.Errorf("[.container table tr > td:nth-child(1)] Failed got '%s', expected '%s'", text, e)
			failed = true
		}

		domainKeys = domainKeys[1:]
	})

	secondColSize := domainTable.Find("tr > td:nth-child(2)").Size()
	if secondColSize != 6 {
		t.Errorf("[.container table tr > td:nth-child(2)] Wrong size got %d, expected %s", secondColSize, 6)
	}

	thirdColSize := domainTable.Find("tr > td:nth-child(3)").Size()
	if thirdColSize != 0 {
		t.Errorf("[.container table tr > td:nth-child(3)] Wrong size got %d, expected %s", thirdColSize, 0)
	}

	//
	// Links
	//
	linksColHeaders := []string{
		"Link",
		"Status",
		"Error",
		"Excluded",
		"Fetched",
	}
	failed = false
	linksTable.Find("thead th").Each(func(index int, sel *goquery.Selection) {
		if failed {
			return
		}
		text := strings.TrimSpace(sel.Text())
		if len(linksColHeaders) == 0 || text != linksColHeaders[0] {
			e := ""
			if len(linksColHeaders) > 0 {
				e = linksColHeaders[0]
			}
			t.Errorf("[.container table thead th] Col name mismatch got '%s', expected '%s'", text, e)
			failed = true
		}

		linksColHeaders = linksColHeaders[1:]
	})

	linkRows := linksTable.Find("tbody tr td a")
	if linkRows.Size() < 5 {
		t.Errorf("[.container table tbody tr td a] not enough rows")
	}
	failed = false
	linkRows.Each(func(index int, sel *goquery.Selection) {
		if failed {
			return
		}
		link, linkOk := sel.Attr("href")
		if !linkOk {
			t.Errorf("[.container table tbody tr td a] Failed to find href")
			failed = true
			return
		}
		if !strings.HasPrefix(link, "/historical") {
			t.Errorf("[.container table tbody tr td a] Failed to find prefix /historical in href (%s)", link)
			failed = true
			return
		}
	})

	//
	// Buttons
	//
	buttons := []string{
		"Previous",
		"Next",
	}
	failed = false
	doc.Find(".container a").FilterFunction(func(index int, sel *goquery.Selection) bool {
		return sel.HasClass("btn")
	}).Each(func(index int, sel *goquery.Selection) {
		if failed {
			return
		}

		text := strings.TrimSpace(sel.Text())
		if len(buttons) == 0 || text != buttons[0] {
			e := ""
			if len(buttons) > 0 {
				e = buttons[0]
			}
			t.Errorf("[.container a <buttons>] Failed text '%s', expected '%s'", text, e)
			failed = true
		}

		if text == "Previous" {
			if !sel.HasClass("disabled") {
				t.Errorf("[.container a <buttons>] Failed disabled for %s", text)
				failed = true
			}
		} else {
			if sel.HasClass("disabled") {
				t.Errorf("[.container a <buttons>] Failed disabled for %s", text)
				failed = true
			}
		}

		buttons = buttons[1:]
	})
}

func TestListLinksSecondPage(t *testing.T) {
	spoofData()

	//
	// First find the second page link
	//
	doc, body, status := callController("http://localhost:3000/links/t1.com", "", "/links/{domain}", console.LinksController)
	if status != http.StatusOK {
		t.Errorf("TestListLinksSecondPage bad status code got %d, expected %d", status, http.StatusOK)
		//body = ""
		t.Log(body)
		t.Fail()
	}
	nextButton := doc.Find(".container a").FilterFunction(func(index int, sel *goquery.Selection) bool {
		return sel.HasClass("btn") && strings.Contains(sel.Text(), "Next")
	})
	if nextButton.Size() != 1 {
		t.Fatalf("[.container a <buttons>] Failed to find next button")
		return
	}
	nextPagePath, nextPageOk := nextButton.Attr("href")
	if !nextPageOk {
		t.Fatalf("[.container a <buttons>] Failed to find next button href")
		return
	}

	//
	// OK now click on the next button
	//
	nextPage := "http://localhost:3000" + nextPagePath
	doc, body, status = callController(nextPage, "", "/links/{domain}/{seedUrl}", console.LinksController)
	if status != http.StatusOK {
		t.Errorf("TestListLinks bad status code got %d, expected %d", status, http.StatusOK)
		//body = ""
		t.Log(body)
		t.Fail()
	}

	// Nab the tables
	tables := doc.Find(".container table")
	if tables.Size() != 1 {
		t.Fatalf("[.container table] Bad size got %d, expected %d", tables.Size(), 2)
	}
	linksTable := tables.Last()

	//
	// Links Table
	//
	linksColHeaders := []string{
		"Link",
		"Status",
		"Error",
		"Excluded",
		"Fetched",
	}
	failed := false
	linksTable.Find("thead th").Each(func(index int, sel *goquery.Selection) {
		if failed {
			return
		}
		text := strings.TrimSpace(sel.Text())
		if len(linksColHeaders) == 0 || text != linksColHeaders[0] {
			e := ""
			if len(linksColHeaders) > 0 {
				e = linksColHeaders[0]
			}
			t.Errorf("[.container table thead th] Col name mismatch got '%s', expected '%s'", text, e)
			failed = true
		}

		linksColHeaders = linksColHeaders[1:]
	})

	linkRows := linksTable.Find("tbody tr td a")
	if linkRows.Size() < 5 {
		t.Errorf("[.container table tbody tr td a] not enough rows")
	}
	failed = false
	linkRows.Each(func(index int, sel *goquery.Selection) {
		if failed {
			return
		}
		link, linkOk := sel.Attr("href")
		if !linkOk {
			t.Errorf("[.container table tbody tr td a] Failed to find href")
			failed = true
			return
		}
		if !strings.HasPrefix(link, "/historical") {
			t.Errorf("[.container table tbody tr td a] Failed to find prefix /historical in href (%s)", link)
			failed = true
			return
		}
	})

	//
	// Buttons
	//
	buttons := []string{
		"Previous",
		"Next",
	}
	failed = false
	doc.Find(".container a").FilterFunction(func(index int, sel *goquery.Selection) bool {
		return sel.HasClass("btn")
	}).Each(func(index int, sel *goquery.Selection) {
		if failed {
			return
		}
		text := strings.TrimSpace(sel.Text())
		if len(buttons) == 0 || text != buttons[0] {
			e := ""
			if len(buttons) > 0 {
				e = buttons[0]
			}
			t.Errorf("[.container a <buttons>] Failed text '%s', expected '%s'", text, e)
			failed = true
		}

		if text == "Previous" {
			if sel.HasClass("disabled") {
				t.Errorf("[.container a <buttons>] Failed disabled for %s", text)
				failed = true
			}
		} else {
			if !sel.HasClass("disabled") {
				t.Errorf("[.container a <buttons>] Failed disabled for %s", text)
				failed = true
			}
		}

		buttons = buttons[1:]
	})
}

func TestListHistorical(t *testing.T) {
	spoofData()

	//
	// First get the domain page, and find the historical link
	//
	doc, body, status := callController("http://localhost:3000/links/h1.com", "", "/links/{domain}", console.LinksController)
	if status != http.StatusOK {
		t.Errorf("TestListHistorical bad status code got %d, expected %d", status, http.StatusOK)
		body = ""
		t.Log(body)
		t.Fatalf("")
	}

	// Nab the tables
	tables := doc.Find(".container table")
	if tables.Size() != 2 {
		t.Fatalf("[.container table] Bad size got %d, expected %d", tables.Size(), 2)
	}
	linksTable := tables.Last()

	linkRows := linksTable.Find("tbody tr td a")
	if linkRows.Size() < 1 {
		t.Fatalf("[.container table tbody tr td a] not enough rows")
	}
	historicalLink := ""
	linkRows.First().Each(func(index int, sel *goquery.Selection) {
		link, linkOk := sel.Attr("href")
		if !linkOk {
			t.Fatalf("[.container table tbody tr td a] Failed to find href")
			return
		}
		if !strings.HasPrefix(link, "/historical") {
			t.Fatalf("[.container table tbody tr td a] Failed to find prefix /historical in href (%s)", link)
			return
		}

		historicalLink = link
	})

	//
	// Now lets fetch the historical page
	//
	nextPage := "http://localhost:3000" + historicalLink
	doc, body, status = callController(nextPage, "", "/historical/{url}", console.LinksHistoricalController)
	if status != http.StatusOK {
		t.Errorf("TestListLinks bad status code got %d, expected %d", status, http.StatusOK)
		//body = ""
		t.Log(body)
		t.Fail()
	}

	doc.Find(".container h2").First().Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		if !strings.HasPrefix(text, "History for Link") {
			t.Errorf("[.container h2] Bad prefix got %s, expected 'History for Link'", text)
		}
	})

	tables = doc.Find(".container table")
	if tables.Size() != 1 {
		t.Fatalf("[.container table] Bad size got %d, expected %d", tables.Size(), 1)
		return
	}

	colHeaders := []string{
		"Fetched On",
		"Robots Excluded",
		"Status",
		"Error",
	}
	failed := false
	tables.Find("thead th").Each(func(index int, sel *goquery.Selection) {
		if failed {
			return
		}
		if len(colHeaders) == 0 {
			t.Errorf("[.container table thead th] Ran out of colHeaders")
			failed = true
			return
		}
		text := strings.TrimSpace(sel.Text())
		if text != colHeaders[0] {
			t.Errorf("[.container table thead th] Column header got '%s', expected '%s'", text, colHeaders[0])
			failed = true
		}

		colHeaders = colHeaders[1:]
	})

	nrows := tables.Find("tbody tr").Size()
	if nrows < 5 {
		t.Fatalf("[.container table tbody tr] Size mismatch got %d, expected >%d", nrows, 5)
	}

	failed = false
	tables.Find("tbody tr").Each(func(index int, sel *goquery.Selection) {
		if failed {
			return
		}
		ncol := sel.Children().Size()
		if ncol != 4 {
			t.Errorf("[.container table tbody tr] Wrong column count got %d, expected %d", ncol, 4)
			failed = true
			return
		}
	})
}

func TestFindDomains(t *testing.T) {
	spoofData()

	//
	// First get the domain page, and find the historical link
	//
	doc, body, status := callController("http://localhost:3000/find", "", "/find", console.FindDomainController)
	if status != http.StatusOK {
		t.Errorf("TestFindDomains bad status code got %d, expected %d", status, http.StatusOK)
		body = ""
		t.Log(body)
		t.Fatalf("")
	}

	doc.Find(".container h2").Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		if text != "Find domains" {
			t.Errorf("[.container h2] Got '%s', expected '%s'", text, "Find domains")
		}
	})

	form := doc.Find(".container form")
	textarea := form.Find("textarea")
	input := form.Find("input")
	if textarea.Size() != 1 {
		t.Fatalf("[.container form textarea] Count got %d, expected 1", textarea.Size())
	}
	placeholder, placeholderOk := textarea.Attr("placeholder")
	if !placeholderOk {
		t.Errorf("[.container form textarea] Failed to find placeholder attribute")
	} else {
		e := "Enter domains: one per line"
		if placeholder != e {
			t.Errorf("[.container form textarea] Bad placeholder attribute got %s, expected %s", placeholder, e)
		}
	}

	if input.Size() != 1 {
		t.Fatalf("[.container form input] Count got %d, expected 1", input.Size())
	}
	typ, typOk := input.Attr("type")
	if !typOk {
		t.Fatalf("[.container form input] Failed to find type attribute")
	} else if typ != "submit" {
		t.Errorf("[.container form input] Bad type got %s, expected submit", typ)
	}

	//
	// Lets submit a find request
	//
	rawBody := "targets=t1.com%0D%0At2.com%0D%0At3.com"
	doc, body, status = callController("http://localhost:3000/find", rawBody, "/find", console.FindDomainController)
	if status != http.StatusOK {
		t.Errorf("TestFindDomains bad status code got %d, expected %d", status, http.StatusOK)
		body = ""
		t.Log(body)
		t.Fatalf("")
	}
	expLinks := []string{
		"/links/t1.com",
		"/links/t2.com",
		"/links/t3.com",
	}
	links := doc.Find(".container table tbody tr a")
	if links.Size() != 3 {
		t.Fatalf("[.container table tbody tr a] Expected 3 elements")
	}
	count := 0
	links.Each(func(index int, sel *goquery.Selection) {
		href, hrefOk := sel.Attr("href")
		if !hrefOk {
			t.Fatalf("[.container table tbody tr a] Failed to find href attribute")
		} else if expLinks[count] != href {
			t.Fatalf("[.container table tbody tr a] href link mismatch, got '%v', expected '%v'", href, expLinks[count])
		}

		count++
	})

	//
	// Lets submit a bad find request
	//
	rawBody = "targets=NOTTHERE1.com%0D%0ANOTTHERE2.com%0D%0ANOTTHERE3.com"
	doc, body, status = callController("http://localhost:3000/find", rawBody, "/find", console.FindDomainController)
	if status != http.StatusOK {
		t.Errorf("TestFindDomains bad status code got %d, expected %d", status, http.StatusOK)
		body = ""
		t.Log(body)
		t.Fatalf("")
	}

	expMessages := []string{
		"Failed to find domain NOTTHERE1.com",
		"Failed to find domain NOTTHERE2.com",
		"Failed to find domain NOTTHERE3.com",
	}
	messages := doc.Find(".container > ul li")
	if messages.Size() != 4 {
		t.Fatalf("[.container ul li] Message mismatch: got %d, expected 4", messages.Size())
	}
	count = 0
	messages.Each(func(index int, sel *goquery.Selection) {
		if count >= 3 {
			return
		}
		text := strings.TrimSpace(sel.Text())
		if expMessages[count] != text {
			t.Fatalf("[.container ul li] Text mismatch, got '%v', expected '%v'", text, expMessages[count])
		}
		count++
	})

}

func TestFindLinks(t *testing.T) {
	spoofData()

	//
	// First get the domain page, and find the historical link
	//
	doc, body, status := callController("http://localhost:3000/findLinks", "", "/findLinks", console.FindLinksController)
	if status != http.StatusOK {
		t.Errorf("TestFindDomains bad status code got %d, expected %d", status, http.StatusOK)
		body = ""
		t.Log(body)
		t.Fatalf("")
	}

	doc.Find(".container h2").Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		if text != "Find Links" {
			t.Errorf("[.container h2] Header mismatch got '%s', expected '%s'", text, "Find Links")
		}
	})

	form := doc.Find(".container form")
	textarea := form.Find("textarea")
	input := form.Find("input")
	if textarea.Size() != 1 {
		t.Fatalf("[.container form textarea] Count got %d, expected 1", textarea.Size())
	}
	placeholder, placeholderOk := textarea.Attr("placeholder")
	if !placeholderOk {
		t.Errorf("[.container form textarea] Failed to find placeholder attribute")
	} else {
		e := "Enter links: one per line"
		if placeholder != e {
			t.Errorf("[.container form textarea] Bad placeholder attribute got %s, expected %s", placeholder, e)
		}
	}

	if input.Size() != 1 {
		t.Fatalf("[.container form input] Count got %d, expected 1", input.Size())
	}
	typ, typOk := input.Attr("type")
	if !typOk {
		t.Fatalf("[.container form input] Failed to find type attribute")
	} else if typ != "submit" {
		t.Errorf("[.container form input] Bad type got %s, expected submit", typ)
	}

	//
	// Submit a find request
	//
	rawBody := "links=http://link.t1.com/page0.html%0D%0A"
	doc, body, status = callController("http://localhost:3000/findLinks", rawBody, "/findLinks", console.FindLinksController)
	if status != http.StatusOK {
		t.Errorf("TestFindDomains bad status code got %d, expected %d", status, http.StatusOK)
		body = ""
		t.Log(body)
		t.Fatalf("")
	}
	res := doc.Find(".container table tbody tr td")
	if res.Size() != 5 {
		t.Errorf("[.container table tbody tr td] Bad size got %d, expected 1", res.Size())
	}
	res.First().Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		e := "http://link.t1.com/page0.html"
		if text != e {
			t.Fatalf("[.container table tbody tr td] Mismatched text got '%v', expected '%v'", text, e)
		}
	})
}

func TestAddLinks(t *testing.T) {
	spoofData()

	//
	// First get the domain page, and find the historical link
	//
	doc, body, status := callController("http://localhost:3000/add", "", "/add", console.AddLinkIndexController)
	if status != http.StatusOK {
		t.Errorf("TestFindDomains bad status code got %d, expected %d", status, http.StatusOK)
		body = ""
		t.Log(body)
		t.Fatalf("")
	}

	doc.Find(".container h2").Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		e := "Add Links"
		if text != e {
			t.Errorf("[.container h2] Header mismatch got '%s', expected '%s'", text, e)
		}
	})

	form := doc.Find(".container form")
	textarea := form.Find("textarea")
	input := form.Find("input")
	if textarea.Size() != 1 {
		t.Fatalf("[.container form textarea] Count got %d, expected 1", textarea.Size())
	}
	placeholder, placeholderOk := textarea.Attr("placeholder")
	if !placeholderOk {
		t.Errorf("[.container form textarea] Failed to find placeholder attribute")
	} else {
		e := "Enter links: one per line"
		if placeholder != e {
			t.Errorf("[.container form textarea] Bad placeholder attribute got %s, expected %s", placeholder, e)
		}
	}

	if input.Size() != 1 {
		t.Fatalf("[.container form input] Count got %d, expected 1", input.Size())
	}
	typ, typOk := input.Attr("type")
	if !typOk {
		t.Fatalf("[.container form input] Failed to find type attribute")
	} else if typ != "submit" {
		t.Errorf("[.container form input] Bad type got %s, expected submit", typ)
	}

	//
	// Submit an add request
	//
	randDomain := fmt.Sprintf("rand%d.com", rand.Uint32())
	randLink := fmt.Sprintf("http://sub.%s.com/page0.html", randDomain)
	rawBody := "links=" + randLink + "%0D%0A"
	doc, body, status = callController("http://localhost:3000/add", rawBody, "/add", console.AddLinkIndexController)
	if status != http.StatusOK {
		t.Errorf("TestFindDomains bad status code got %d, expected %d", status, http.StatusOK)
		body = ""
		t.Log(body)
		t.Fatalf("")
	}
	res := doc.Find(".container > ul li")
	if res.Size() != 1 {
		t.Fatalf("[.container ul li] Bad size got %d, expected 1", res.Size())
	}
	res.Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		e := "All links added"
		if text != e {
			t.Fatalf("[.container ul li] Mismatched text got '%v', expected '%v'", text, e)
		}
	})

	//
	// Find the link
	//
	doc, body, status = callController("http://localhost:3000/findLinks", rawBody, "/findLinks", console.FindLinksController)
	if status != http.StatusOK {
		t.Errorf("TestFindDomains bad status code got %d, expected %d", status, http.StatusOK)
		body = ""
		t.Log(body)
		t.Fatalf("")
	}
	res = doc.Find(".container table tbody tr td")
	if res.Size() != 5 {
		t.Errorf("[.container table tbody tr td] Bad size got %d, expected 5", res.Size())
	}
	res.First().Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		if text != randLink {
			t.Fatalf("[.container table tbody tr td] Mismatched text got '%v', expected '%v'", text, randLink)
		}
	})

}
