package console

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"code.google.com/p/log4go"
	"github.com/gorilla/mux"
	"github.com/iParadigms/walker"
)

func RestRoutes() []Route {
	return []Route{
		Route{Path: "/rest/foo", Controller: RestFoo},
		Route{Path: "/rest/add", Controller: RestAdd},
	}
}

func decodeGeneric(body io.Reader) (map[string]interface{}, error) {
	decoder := json.NewDecoder(req.Body)
	mp := map[string]interface{}{}
	err := decoder.Decode(&mp)
	return mp, err
}

type restReply struct {
	typ string `json:"type"`
}

type restError struct {
	restReply
	message string
}

func buildError(tag string, format string, args ...interface{}) restError {
	return restError{
		typ:     "error: " + tag,
		message: fmt.Sprintf(format, args...),
	}
}

var restOk = restReply{typ: "ok"}

func ResFoo(w http.ResponseWriter, req *http.Request) {
	input, err := decodeGeneric(req.Body)
	if err != nil {
		Render.JSON(w, http.StatusOK, buildError("Json decode", err.Error()))
		return
	}
	input["output"] = "Pete is a pimp!"
	Render.JSON(w, http.StatusOK, input)
}

func RestAdd(w http.ResponseWriter, req *http.Request) {
	Render.JSON(w, http.StatusOK, &restOk)
	return
}
