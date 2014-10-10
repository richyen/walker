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

func newDoc(html string) *goquery.Document {
	reader := strings.NewReader(html)
	doc, err := goquery.NewDocumentFromReader(reader)
	if err != nil {
		panic(err)
	}
	return doc
}

func callController(url string, body string, urlPattern string, controller func(w http.ResponseWriter, req *http.Request)) (*goquery.Document, string, int) {
	var bodyBuff io.Reader = nil
	method := "GET"
	if body != "" {
		bodyBuff = bytes.NewBufferString(body)
		method = "POST"
	}
	req, err := http.NewRequest(method, url, bodyBuff)
	if err != nil {
		panic(err)
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
		if text != header[0] {
			t.Errorf("[.container table thead td] Bad order got '%v' expected '%v'", text, header[0])
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
		//body = ""
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
		if text != h2[0] {
			t.Errorf("[.container h2] Failed got '%s', expected '%s'", text, h2[0])
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
		if text != domainKeys[0] {
			t.Errorf("[.container table tr > td:nth-child(1)] Failed got '%s', expected '%s'", text, domainKeys[0])
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
		if text != linksColHeaders[0] {
			t.Errorf("[.container table thead th] Col name mismatch got '%s', expected '%s'", text, linksColHeaders[0])
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
		if text != buttons[0] {
			t.Errorf("[.container a <buttons>] Failed text '%s', expected '%s'", text, buttons[0])
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
		t.Errorf("TestListLinks bad status code got %d, expected %d", status, http.StatusOK)
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
		if text != linksColHeaders[0] {
			t.Errorf("[.container table thead th] Col name mismatch got '%s', expected '%s'", text, linksColHeaders[0])
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
		if text != buttons[0] {
			t.Errorf("[.container a <buttons>] Failed text '%s', expected '%s'", text, buttons[0])
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

}
