package console

import (
	"fmt"
	"html/template"
	"net/http"
	"time"

	"code.google.com/p/log4go"
	"encoding/base32"
	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
	"github.com/unrolled/render"
)

var DS DataStore
var zeroTime = time.Time{}
var zeroUuid = gocql.UUID{}
var timeFormat = "2006-01-02 15:04:05 -0700"

const PageWindowLength = 15

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
		return ""
	} else {
		return t.Format(timeFormat)
	}
}

func ftimeFunc(t time.Time) string {
	if t == zeroTime || t.Equal(walker.NotYetCrawled) {
		return "Not yet crawled"
	} else {
		return t.Format(timeFormat)
	}
}

func ftime2Func(t time.Time) string {
	if t == zeroTime || t.Equal(walker.NotYetCrawled) {
		return ""
	} else {
		return t.Format(timeFormat)
	}
}

func fuuidFunc(u gocql.UUID) string {
	if u == zeroUuid {
		return ""
	} else {
		return u.String()
	}
}

// func statusText(status int) string {
//  return http.StatusText(status)
// }

var Render = render.New(render.Options{
	Layout:        "layout",
	IndentJSON:    true,
	IsDevelopment: true,
	Funcs: []template.FuncMap{
		template.FuncMap{
			"yesOnFilled": yesOnFilledFunc,
			"activeSince": activeSinceFunc,
			"ftime":       ftimeFunc,
			"ftime2":      ftime2Func,
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
	Render.HTML(w, status, template, mp)
}

// func reply(w http.ResponseWriter, template string, keyValues ...interface{}) {
// 	replyFull(w, template, http.StatusOK, keyValues...)
// }

func replyServerError(w http.ResponseWriter, err error) {
	log4go.Error("Rendering 500: %v", err)
	replyFull(w, "serverError", http.StatusInternalServerError,
		"anErrorHappend", true,
		"theError", err.Error())
}

// func replyWithInfo(w http.ResponseWriter, template string, message string) {
// 	replyFull(w, template, http.StatusOK,
// 		"HasInfoMessage", true,
// 		"InfoMessage", []string{message})
// }

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

// Some Utilities
func decode32(s string) (string, error) {
	b, err := base32.StdEncoding.DecodeString(s)
	return string(b), err
}

func encode32(s string) string {
	b := base32.StdEncoding.EncodeToString([]byte(s))
	return string(b)
}
