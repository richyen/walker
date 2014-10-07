package console

import (
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strings"
	"time"

	"code.google.com/p/log4go"
	"encoding/base32"
	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
	"github.com/unrolled/render"
)

var DS DataStore

var zeroTime = time.Time{}
var zeroUuid = gocql.UUID{}
var timeFormat = "2006-01-02 15:04:05 -0700"

func yesOnFilledFunc(s string) string {
	if s == "" {
		return ""
	} else {
		return "yes"
	}
}

func yesOnTrueFunc(q bool) string {
	if q {
		return "yes"
	} else {
		return ""
	}

}

func activeSinceFunc(t time.Time) string {
	if t == zeroTime {
		return "-"
	} else {
		return t.Format(timeFormat)
	}
}

func ftimeFunc(t time.Time) string {
	if t == zeroTime {
		return ""
	} else {
		return t.Format(timeFormat)
	}
}

func fuuidFunc(u gocql.UUID) string {
	return u.String()
}

// func statusText(status int) string {
// 	return http.StatusText(status)
// }

var renderer = render.New(render.Options{
	Layout:        "layout",
	IndentJSON:    true,
	IsDevelopment: true,
	Funcs: []template.FuncMap{
		template.FuncMap{
			"yesOnFilled": yesOnFilledFunc,
			"activeSince": activeSinceFunc,
			"ftime":       ftimeFunc,
			"fuuid":       fuuidFunc,
			"statusText":  http.StatusText,
			"yesOnTrue":   yesOnTrueFunc,
		},
	},
})

func replyFull(w http.ResponseWriter, template string, status int, keyValues ...interface{}) {
	if len(keyValues)%2 != 0 {
		panic(fmt.Errorf("INTERNAL ERROR: poorly used reply: keyValues does not have even number of elements"))
	}
	mp := map[string]interface{}{}
	for i := 0; i < len(keyValues); i = i + 2 {
		protokey := keyValues[i]
		key, keyok := protokey.(string)
		if !keyok {
			panic(fmt.Errorf("INTERNAL ERROR: poorly used reply: found a non-string in keyValues"))
		}
		value := keyValues[i+1]
		mp[key] = value
	}
	renderer.HTML(w, status, template, mp)
}

func reply(w http.ResponseWriter, template string, keyValues ...interface{}) {
	replyFull(w, template, http.StatusOK, keyValues...)
}

func replyServerError(w http.ResponseWriter, err error) {
	log4go.Error("Rendering 500: %v", err)
	replyFull(w, "serverError", http.StatusInternalServerError,
		"anErrorHappend", true,
		"theError", err.Error())
}
func replyWithInfo(w http.ResponseWriter, template string, message string) {
	replyFull(w, template, http.StatusOK,
		"HasInfoMessage", true,
		"InfoMessage", []string{message})
}

func replyWithError(w http.ResponseWriter, template string, message string) {
	log4go.Info("Rendered user error message %v", message)
	replyFull(w, template, http.StatusOK,
		"HasErrorMessage", true,
		"ErrorMessage", []string{message})
}

func replyWithErrorList(w http.ResponseWriter, template string, messages []string) {
	log4go.Info("Rendered user error messages %v", messages)
	replyFull(w, template, http.StatusOK,
		"HasErrorMessage", true,
		"ErrorMessage", messages)
}

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
	}
}

func homeHandler(w http.ResponseWriter, req *http.Request) {
	reply(w, "home")
	return
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
		replyServerError(w, err)
		return
	}

	nextDomain := ""
	hasNext := false
	if len(dinfos) == PageWindowLength {
		nextDomain = url.QueryEscape(dinfos[len(dinfos)-1].Domain)
		hasNext = true
	}
	reply(w, "list",
		"Domains", dinfos,
		"HasNext", hasNext,
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
	target := targets[0]
	//XXX: change so that it handles several domains

	if target == "" {
		reply(w, "find")
		return
	}

	t, err := url.QueryUnescape(target)
	if err != nil {
		replyWithError(w, "find", fmt.Sprintf("Failed to decode domain %s", target))
		return
	}
	target = t

	dinfo, err := DS.FindDomain(target)
	if err != nil {
		err = fmt.Errorf("FindDomain failed: %v", err)
		replyServerError(w, err)
		return
	}

	if dinfo == nil {
		replyWithInfo(w, "find", fmt.Sprintf("Failed to find domain %s", target))
		return
	}

	dinfos := []DomainInfo{*dinfo}

	reply(w, "list",
		"Domains", dinfos,
		"HasNext", false)
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
func decode32(s string) (string, error) {
	b, err := base32.StdEncoding.DecodeString(s)
	return string(b), err
}

func encode32(s string) string {
	b := base32.StdEncoding.EncodeToString([]byte(s))
	return string(b)
}

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
	if seedUrl != "" {
		ss, err := decode32(seedUrl)
		if err != nil {
			replyServerError(w, fmt.Errorf("QueryUnescape: %v", err))
			return
		}
		seedUrl = ss
	}

	linfos, err := DS.ListLinks(domain, seedUrl, PageWindowLength)
	if err != nil {
		replyServerError(w, fmt.Errorf("ListLinks: %v", err))
		return
	}

	nextSeedUrl := ""
	hasNext := false
	if len(linfos) == PageWindowLength {
		nextSeedUrl = encode32(linfos[len(linfos)-1].Url)

		hasNext = true
	}
	reply(w, "links",
		"Dinfo", dinfo,
		"HasLinks", len(linfos) > 0,
		"Linfos", linfos,
		"HasNext", hasNext,
		"NextSeedUrl", nextSeedUrl)

	return
}
