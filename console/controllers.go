package console

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/gorilla/mux"
)

type Route struct {
	Path    string
	Handler func(w http.ResponseWriter, req *http.Request)
}

func Routes() []Route {
	return []Route{
		Route{Path: "/", Handler: homeHandler},
		Route{Path: "/list", Handler: listDomainsHandler},
		Route{Path: "/list/", Handler: listDomainsHandler},
		Route{Path: "/list/{seed}", Handler: listDomainsHandler},
		Route{Path: "/find", Handler: findDomainHandler},
		Route{Path: "/find/", Handler: findDomainHandler},
		Route{Path: "/add", Handler: addLinkIndexHandler},
		Route{Path: "/add/", Handler: addLinkIndexHandler},
		Route{Path: "/links/{domain}", Handler: linksHandler},
		Route{Path: "/links/{domain}/{seedUrl}", Handler: linksHandler},
		Route{Path: "/historical/{url}", Handler: linksHistoricalHandler},
	}
}

func homeHandler(w http.ResponseWriter, req *http.Request) {
	reply(w, "home")
	return
}

func listDomainsHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	seed := vars["seed"]
	prevButtonClass := ""
	if seed == "" {
		seed = DontSeedDomain
		prevButtonClass = "disabled"
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
		replyServerError(w, err)
		return
	}

	nextDomain := ""
	nextButtonClass := "disabled"
	if len(dinfos) == PageWindowLength {
		nextDomain = url.QueryEscape(dinfos[len(dinfos)-1].Domain)
		nextButtonClass = ""
	}
	reply(w, "list",
		"PrevButtonClass", prevButtonClass,
		"NextButtonClass", nextButtonClass,
		"Domains", dinfos,
		"Next", nextDomain)
}

func findDomainHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		reply(w, "find")
		return
	}

	err := req.ParseForm()
	if err != nil {
		replyServerError(w, err)

	}
	targetAll, ok := req.Form["targets"]
	if !ok {
		reply(w, "find")
		return
	}

	lines := strings.Split(targetAll[0], "\n")
	targets := []string{}
	for i := range lines {
		t := strings.TrimSpace(lines[i])
		if t != "" {
			targets = append(targets, t)
		}
	}

	if len(targets) <= 0 {
		replyWithInfo(w, "find", fmt.Sprintf("Failed to specify any targets"))
		return
	}

	var dinfos []DomainInfo
	var errs []string
	var info []string
	for _, target := range targets {
		t, err := url.QueryUnescape(target)
		if err != nil {
			errs = append(errs, fmt.Sprintf("Failed to decode domain %s", target))
			continue
		}
		target = t

		dinfo, err := DS.FindDomain(target)
		if err != nil {
			errs = append(errs, fmt.Sprintf("FindDomain failed: %v", err))
			continue
		}

		if dinfo == nil {
			info = append(info, fmt.Sprintf("Failed to find domain %s", target))
			continue
		}

		dinfos = append(dinfos, *dinfo)
	}

	hasInfoMessage := len(info) > 0
	hasErrorMessage := len(errs) > 0

	if len(dinfos) == 0 {
		info = append(info, "Didn't find any links on previous try")
		hasInfoMessage = true
		reply(w, "find",
			"HasInfoMessage", hasInfoMessage,
			"InfoMessage", info,
			"HasErrorMessage", hasErrorMessage,
			"ErrorMessage", errs)
	} else {
		reply(w, "list",
			"PrevButtonClass", "disabled",
			"NextButtonClass", "disabled",
			"Domains", dinfos,
			"HasNext", false,
			"HasInfoMessage", hasInfoMessage,
			"InfoMessage", info,
			"HasErrorMessage", hasErrorMessage,
			"ErrorMessage", errs)
	}
}

func addLinkIndexHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		reply(w, "add")
		return
	}

	err := req.ParseForm()
	if err != nil {
		replyServerError(w, err)
		return
	}

	linksExt, ok := req.Form["links"]
	if !ok {
		replyServerError(w, err)
		return
	}

	lines := strings.Split(linksExt[0], "\n")
	links := make([]string, 0, len(lines))
	for i := range lines {
		t := strings.TrimSpace(lines[i])
		if t == "" {
			continue
		}

		if strings.Index(t, "http://") != 0 && strings.Index(t, "https://") != 0 {
			t = "http://" + t
		}

		links = append(links, t)
	}

	errList := DS.InsertLinks(links)
	if len(errList) != 0 {
		var s []string
		for _, x := range errList {
			s = append(s, x.Error())
		}
		replyWithErrorList(w, "add", s)
		return
	} else {
		replyWithInfo(w, "add", "All links added")
		return
	}
}

//IMPL NOTE: Why does linksHandler encode the seedUrl in base32, rather than URL encode it?
// The reason is that various components along the way are tripping on the appearance of the
// seedUrl argument. First, it appears that the browser is unencoding the link BEFORE submitting it
// to the server. That looks like a problem with the browser to me. But in addition, the server appears
// to be choking on the url-encoded text as well. For example if the url encoded seedUrl ends with
// .html, it appears that this is causing the server to throw a 301. Unknown why that is. But the net effect
// is that, if I totally disguise the link in base32, everything works.

func linksHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	domain := vars["domain"]
	if domain == "" {
		replyServerError(w, fmt.Errorf("User failed to specify domain for linksHandler"))
		return
	}
	dinfo, err := DS.FindDomain(domain)
	if err != nil {
		replyServerError(w, fmt.Errorf("FindDomain: %v", err))
		return
	}

	if dinfo == nil {
		replyServerError(w, fmt.Errorf("User failed to specify domain for linksHandler"))
		return
	}

	seedUrl := vars["seedUrl"]
	needHeader := false
	windowLength := PageWindowLength
	prevButtonClass := ""
	if seedUrl == "" {
		needHeader = true
		windowLength /= 2
		prevButtonClass = "disabled"
	} else {
		ss, err := decode32(seedUrl)
		if err != nil {
			replyServerError(w, fmt.Errorf("QueryUnescape: %v", err))
			return
		}
		seedUrl = ss
	}

	linfos, err := DS.ListLinks(domain, seedUrl, windowLength)
	if err != nil {
		replyServerError(w, fmt.Errorf("ListLinks: %v", err))
		return
	}

	nextSeedUrl := ""
	nextButtonClass := "disabled"
	if len(linfos) == windowLength {
		nextSeedUrl = encode32(linfos[len(linfos)-1].Url)
		nextButtonClass = ""
	}

	var historyLinks []string
	for _, linfo := range linfos {
		path := "/historical/" + encode32(linfo.Url)
		historyLinks = append(historyLinks, path)
	}

	reply(w, "links",
		"Dinfo", dinfo,
		"HasHeader", needHeader,
		"HasLinks", len(linfos) > 0,
		"Linfos", linfos,
		"NextSeedUrl", nextSeedUrl,
		"NextButtonClass", nextButtonClass,
		"PrevButtonClass", prevButtonClass,
		"HistoryLinks", historyLinks,
	)

	return
}

func linksHistoricalHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	url := vars["url"]
	if url == "" {
		replyServerError(w, fmt.Errorf("linksHistoricalHandler called without url"))
		return
	}
	nurl, err := decode32(url)
	if err != nil {
		replyServerError(w, fmt.Errorf("decode32 (%s): %v", url, err))
		return
	}
	url = nurl

	//ListLinkHistorical(linkUrl string, seedIndex int, limit int) ([]LinkInfo, int, error)
	linfos, _, err := DS.ListLinkHistorical(url, DontSeedIndex, 500)
	if err != nil {
		replyServerError(w, fmt.Errorf("ListLinkHistorical (%s): %v", url, err))
		return
	}

	reply(w, "historical",
		"LinkTopic", url,
		"Linfos", linfos)
}
