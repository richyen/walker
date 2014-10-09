package console

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/gorilla/mux"
	"github.com/iParadigms/walker"
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
		Route{Path: "/findLinks", Handler: findLinksHandler},
	}
}

func homeHandler(w http.ResponseWriter, req *http.Request) {
	mp := map[string]interface{}{}
	Render.HTML(w, http.StatusOK, "home", mp)
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

	mp := map[string]interface{}{
		"PrevButtonClass": prevButtonClass,
		"NextButtonClass": nextButtonClass,
		"Domains":         dinfos,
		"Next":            nextDomain,
	}
	Render.HTML(w, http.StatusOK, "list", mp)
}

func findDomainHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		mp := map[string]interface{}{}
		Render.HTML(w, http.StatusOK, "find", mp)
		return
	}

	err := req.ParseForm()
	if err != nil {
		replyServerError(w, err)
		return
	}
	targetAll, ok := req.Form["targets"]
	if !ok {
		mp := map[string]interface{}{}
		Render.HTML(w, http.StatusOK, "find", mp)
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
		mp := map[string]interface{}{
			"HasInfoMessage": true,
			"InfoMessage":    "Failed to specify any targets",
		}
		Render.HTML(w, http.StatusOK, "find", mp)
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
		mp := map[string]interface{}{
			"HasInfoMessage":  hasInfoMessage,
			"InfoMessage":     info,
			"HasErrorMessage": hasErrorMessage,
			"ErrorMessage":    errs,
		}
		Render.HTML(w, http.StatusOK, "find", mp)
	} else {
		mp := map[string]interface{}{
			"PrevButtonClass": "disabled",
			"NextButtonClass": "disabled",
			"Domains":         dinfos,
			"HasNext":         false,
			"HasInfoMessage":  hasInfoMessage,
			"InfoMessage":     info,
			"HasErrorMessage": hasErrorMessage,
			"ErrorMessage":    errs,
		}
		Render.HTML(w, http.StatusOK, "list", mp)
	}
}

// TODO: I think that we should have a confirm page after you add the links. But thats
// an advanced feature.
func addLinkIndexHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		mp := map[string]interface{}{}
		Render.HTML(w, http.StatusOK, "add", mp)
		return
	}

	err := req.ParseForm()
	if err != nil {
		replyServerError(w, err)
		return
	}

	linksExt, ok := req.Form["links"]
	if !ok {
		replyServerError(w, fmt.Errorf("Corrupt POST message: no links field"))
		return
	}

	text := linksExt[0]
	lines := strings.Split(text, "\n")
	links := make([]string, 0, len(lines))
	var errs []string
	for i := range lines {
		u := strings.TrimSpace(lines[i])
		if u == "" {
			continue
		}

		uc := urlCleanse(u)
		if uc == "" {
			errs = append(errs, fmt.Sprintf("Unacceptable scheme for '%v'", u))
			continue
		}
		u = uc

		links = append(links, u)
	}

	if len(errs) > 0 {
		mp := map[string]interface{}{
			"HasText":         true,
			"Text":            text,
			"HasInfoMessage":  true,
			"InfoMessage":     []string{"No links added"},
			"HasErrorMessage": true,
			"ErrorMessage":    errs,
		}
		Render.HTML(w, http.StatusOK, "add", mp)
		return
	}

	errList := DS.InsertLinks(links)
	if len(errList) != 0 {
		for _, e := range errList {
			errs = append(errs, e.Error())
		}
		mp := map[string]interface{}{
			"HasErrorMessage": true,
			"ErrorMessage":    errs,
		}
		Render.HTML(w, http.StatusOK, "add", mp)
		return
	}

	mp := map[string]interface{}{
		"HasInfoMessage": true,
		"InfoMessage":    []string{"All links added"},
	}
	Render.HTML(w, http.StatusOK, "add", mp)
	return
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

	mp := map[string]interface{}{
		"Dinfo":           dinfo,
		"HasHeader":       needHeader,
		"HasLinks":        len(linfos) > 0,
		"Linfos":          linfos,
		"NextSeedUrl":     nextSeedUrl,
		"NextButtonClass": nextButtonClass,
		"PrevButtonClass": prevButtonClass,
		"HistoryLinks":    historyLinks,
	}
	Render.HTML(w, http.StatusOK, "links", mp)
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

	linfos, _, err := DS.ListLinkHistorical(url, DontSeedIndex, 500)
	if err != nil {
		replyServerError(w, fmt.Errorf("ListLinkHistorical (%s): %v", url, err))
		return
	}

	mp := map[string]interface{}{
		"LinkTopic": url,
		"Linfos":    linfos,
	}
	Render.HTML(w, http.StatusOK, "historical", mp)
}

func findLinksHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		mp := map[string]interface{}{}
		Render.HTML(w, http.StatusOK, "findLinks", mp)
		return
	}

	err := req.ParseForm()
	if err != nil {
		replyServerError(w, err)
		return
	}

	linksExt, ok := req.Form["links"]
	if !ok {
		replyServerError(w, fmt.Errorf("Corrupt POST message: no links field"))
		return
	}

	text := linksExt[0]
	lines := strings.Split(text, "\n")
	var info []string
	var errs []string
	var linfos []LinkInfo
	for i := range lines {
		u := strings.TrimSpace(lines[i])
		if u == "" {
			continue
		}

		uc := urlCleanse(u)
		if uc == "" {
			errs = append(errs, fmt.Sprintf("Unacceptable scheme for '%v'", u))
			continue
		}
		u = uc

		linfo, err := DS.FindLink(u)
		if err != nil {
			errs = append(errs, fmt.Sprintf("FindLinks error: %v", err))
			continue
		} else if linfo == nil {
			info = append(info, fmt.Sprintf("Failed to find link '%v'", u))
			continue
		}
		linfos = append(linfos, *linfo)
	}

	needErr := len(errs) > 0
	needInf := len(info) > 0

	if len(linfos) == 0 {
		info = append(info, "Failed to find any links")
		mp := map[string]interface{}{
			"Text":            text,
			"HasError":        needErr,
			"HasInfoMessage":  true,
			"InfoMessage":     info,
			"HasErrorMessage": needErr,
			"ErrorMessage":    errs,
		}
		Render.HTML(w, http.StatusOK, "findLinks", mp)
		return
	}

	var historyLinks []string
	for _, linfo := range linfos {
		path := "/historical/" + encode32(linfo.Url)
		historyLinks = append(historyLinks, path)
	}

	mp := map[string]interface{}{
		"HasLinks":       true,
		"Linfos":         linfos,
		"DisableButtons": true,
		"AltTitle":       true,
		"HistoryLinks":   historyLinks,

		"HasInfoMessage":  needInf,
		"InfoMessage":     info,
		"HasErrorMessage": needErr,
		"ErrorMessage":    errs,
	}

	Render.HTML(w, http.StatusOK, "links", mp)
	return
}

// UTILITY
// urlCleanse returns a URL with an acceptable scheme, or the empty string
// if no acceptable scheme is present. In the event the scheme is not provided,
// http is assumed. NOTE: There is a similar function in walker proper. When
// we get to the point of refactoring the data models together, this can be
// merged.
func urlCleanse(url string) string {
	index := strings.LastIndex(url, ":")
	if index < 0 {
		return "http://" + url
	}

	scheme := url[:index]
	for _, f := range walker.Config.AcceptProtocols {
		if scheme == f {
			return url
		}
	}

	return ""
}
