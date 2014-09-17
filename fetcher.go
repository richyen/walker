package walker

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/temoto/robotstxt.go"

	"net/http"
	"net/url"
	"time"

	"code.google.com/p/go.net/html"
	"code.google.com/p/go.net/html/charset"
	"code.google.com/p/go.net/publicsuffix"
	"code.google.com/p/log4go"
)

// Epoch is a convenience for time.Unix(0, 0), used as the epoch time in
// Walker, for example to indicate that a link has not been fetched yet.
var Epoch time.Time

func init() {
	Epoch = time.Unix(0, 0)
}

// FetchResults contains all relevant context and return data from an
// individual fetch. Handlers receive this to process results.
type FetchResults struct {
	// Url that was fetched; will always be populated
	Url *URL

	// Response object; nil if there was a FetchError or ExcludedByRobots is
	// true. Response.Body will be read and closed internally by walker; to get
	// the content use `FetchResults.Contents`
	Res *http.Response

	// Contents is Response.Body read into a []byte. This should be used by
	// Handlers etc. instead of Response.Body, which walker will read and close
	// internally.
	Contents []byte

	// FetchError if the net/http request had an error (non-2XX HTTP response
	// codes are not considered errors)
	FetchError error

	// Time at the beginning of the request (if a request was made)
	FetchTime time.Time

	// True if we did not request this link because it is excluded by
	// robots.txt rules
	ExcludedByRobots bool
}

// URL is the walker URL object, which embeds *url.URL but has extra data and
// capabilities used by walker. Note that LastCrawled should not be set to it's
// zero value, it should be set to the Epoch.
type URL struct {
	*url.URL

	// LastCrawled is the last time we crawled this URL, for example to use a
	// Last-Modified header.
	LastCrawled time.Time
}

// CreateURL creates a walker URL from values usually pulled out of the
// datastore. subdomain may optionally include a trailing '.', and path may
// optionally include a prefixed '/'.
func CreateURL(domain, subdomain, path, protocol string, lastcrawled time.Time) (*URL, error) {
	if subdomain != "" && !strings.HasSuffix(subdomain, ".") {
		subdomain = subdomain + "."
	}
	if path != "" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	ref := fmt.Sprintf("%s://%s%s%s", protocol, subdomain, domain, path)
	u, err := ParseURL(ref)
	if err != nil {
		return nil, err
	}
	u.LastCrawled = lastcrawled
	return u, nil
}

// ParseURL is the walker.URL equivalent of url.Parse
func ParseURL(ref string) (*URL, error) {
	u, err := url.Parse(ref)
	return &URL{URL: u, LastCrawled: Epoch}, err
}

func (u *URL) ToplevelDomainPlusOne() string {
	domain, err := publicsuffix.EffectiveTLDPlusOne(u.Host)
	if err != nil {
		log4go.Error("Error trying to get TLD+1 from %v, error: %v", *u, err)
		return u.Host
	}
	return domain
}

func (u *URL) Subdomain() string {
	tld := u.ToplevelDomainPlusOne()
	if len(u.Host) == len(tld) {
		return ""
	}
	return strings.TrimSuffix(u.Host, "."+tld)
}

type fetcher struct {
	manager    *CrawlManager
	host       string
	httpclient *http.Client
	quit       chan bool
	robots     *robotstxt.Group
	crawldelay time.Duration
}

func newFetcher(m *CrawlManager) *fetcher {
	f := new(fetcher)
	f.manager = m
	f.httpclient = &http.Client{
		Transport: m.Transport,
	}
	f.quit = make(chan bool)
	return f
}

func (f *fetcher) start() {
	log4go.Debug("Starting new fetcher")
	for {
		select {
		case <-f.quit:
			return
		default:
		}

		if f.host != "" {
			//TODO: ensure that this unclaim will happen... probably want the
			//logic below in a function where the Unclaim is deferred
			f.manager.ds.UnclaimHost(f.host)
		}
		f.host = f.manager.ds.ClaimNewHost()
		if f.host == "" {
			time.Sleep(time.Second)
			continue
		}

		f.fetchRobots(f.host)
		f.crawldelay = time.Duration(Config.DefaultCrawlDelay) * time.Second
		if f.robots != nil && int(f.robots.CrawlDelay) > Config.DefaultCrawlDelay {
			f.crawldelay = f.robots.CrawlDelay
		}
		log4go.Debug("Crawling host: %v with crawl delay %v", f.host, f.crawldelay)

		for link := range f.manager.ds.LinksForHost(f.host) {

			//TODO: check <-f.quit and clean up appropriately

			fr := &FetchResults{Url: link}

			if f.robots != nil && !f.robots.Test(link.String()) {
				fr.ExcludedByRobots = true
				f.manager.ds.StoreURLFetchResults(fr)
				continue
			}

			time.Sleep(f.crawldelay)

			fr.FetchTime = time.Now()
			fr.Res, fr.FetchError = f.fetch(link)
			if fr.FetchError != nil {
				log4go.Debug("Error fetching %v: %v", link, fr.FetchError)
				f.manager.ds.StoreURLFetchResults(fr)
				continue
			}

			//TODO: limit to reading Config.MaxHTTPContentSizeBytes
			fr.Contents, fr.FetchError = ioutil.ReadAll(fr.Res.Body)
			if fr.FetchError != nil {
				log4go.Debug("Error reading body of %v: %v", link, fr.FetchError)
				f.manager.ds.StoreURLFetchResults(fr)
				continue
			}

			log4go.Debug("Fetched %v -- %v", link, fr.Res.Status)
			f.manager.ds.StoreURLFetchResults(fr)
			for _, h := range f.manager.handlers {
				h.HandleResponse(fr)
			}

			//TODO: check for other types based on config
			if isHTML(fr) {
				log4go.Debug("Parsing as HTML")
				outlinks, err := getLinks(fr.Contents)
				if err != nil {
					log4go.Warn("error parsing HTML for page %v: %v", link, err)
					continue
				}
				for _, outlink := range outlinks {
					if outlink.Scheme == "" {
						outlink.Scheme = link.Scheme
					}
					if outlink.Host == "" {
						outlink.Host = link.Host
					}
					log4go.Debug("Parsed link: %v", outlink)
					f.manager.ds.StoreParsedURL(outlink, fr)
				}
			} else {
				log4go.Debug("Not parsing due to content type: %v", fr.Res.Header["Content-Type"])
			}
		}
	}
}

func (f *fetcher) stop() {
	f.quit <- true
}

func (f *fetcher) fetchRobots(host string) {
	u := &URL{
		URL: &url.URL{
			Scheme: "http",
			Host:   host,
			Path:   "robots.txt",
		},
	}
	res, err := f.fetch(u)
	if err != nil {
		log4go.Info("Could not fetch %v, assuming there is no robots.txt (error: %v)", u, err)
		f.robots = nil
		return
	}
	robots, err := robotstxt.FromResponse(res)
	res.Body.Close()
	if err != nil {
		log4go.Info("Error parsing robots.txt (%v) assuming there is no robots.txt: %v", u, err)
		f.robots = nil
		return
	}
	f.robots = robots.FindGroup("Turnitinbot")
}

func (f *fetcher) fetch(u *URL) (*http.Response, error) {
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to create new request object for %v): %v", u, err)
	}

	req.Header.Set("User-Agent", Config.UserAgent)
	//TODO: set headers? req.Header[] = ...

	// Do the request.
	res, err := f.httpclient.Do(req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// getLinks parses the response for links, doing it's best with bad HTML.
func getLinks(contents []byte) ([]*URL, error) {
	utf8Reader, err := charset.NewReader(bytes.NewReader(contents), "text/html")
	if err != nil {
		return nil, err
	}
	tokenizer := html.NewTokenizer(utf8Reader)

	var links []*URL
	tags := getIncludedTags()

	for {
		tokenType := tokenizer.Next()
		switch tokenType {
		case html.ErrorToken:
			//TODO: should use tokenizer.Err() to see if this is io.EOF
			//		(meaning success) or an actual error
			return links, nil
		case html.StartTagToken:

			tagName, hasAttrs := tokenizer.TagName()
			if hasAttrs && tags[string(tagName)] {
				links = parseAnchorAttrs(tokenizer, links)
			}
		}
	}

	return links, nil
}

// getIncludedTags gets a map of tags we should check for outlinks. It uses
// ignored_tags in the config to exclude ones we don't want. Tags are []byte
// types (not strings) because []byte is what the parser uses.
func getIncludedTags() map[string]bool {
	tags := map[string]bool{
		"a":      true,
		"area":   true,
		"form":   true,
		"frame":  true,
		"iframe": true,
		"script": true,
		"link":   true,
		"img":    true,
	}
	for _, t := range Config.IgnoreTags {
		delete(tags, t)
	}
	return tags
}

// parseAnchorAttrs iterates over all of the attributes in the current anchor token.
// If a href is found, it adds the link value to the links slice.
// Returns the new link slice.
func parseAnchorAttrs(tokenizer *html.Tokenizer, links []*URL) []*URL {
	//TODO: rework this to be cleaner, passing in `links` to be appended to
	//isn't great
	for {
		key, val, moreAttr := tokenizer.TagAttr()
		if bytes.Compare(key, []byte("href")) == 0 {
			u, err := ParseURL(string(val))
			if err == nil {
				links = append(links, u)
			}
		}
		if !moreAttr {
			return links
		}
	}
}

func isHTML(fr *FetchResults) bool {
	for _, contenttype := range fr.Res.Header["Content-Type"] {
		if strings.HasPrefix(contenttype, "text/html") {
			return true
		}
	}
	return false
}
