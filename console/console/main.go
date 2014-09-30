package main

import (
	"fmt"
	"net/http"

	"github.com/codegangsta/negroni"
	"github.com/gorilla/mux"
	"github.com/iParadigms/walker/console"
)

func main() {
	ds, err := console.NewCqlDataStore()
	//ds, err := console.NewSpoofDataSource()

	if err != nil {
		panic(fmt.Errorf("Failed to start data source: %v", err))
	}
	console.DS = ds
	defer ds.Close()

	router := mux.NewRouter()
	routes := console.Routes()
	for _, route := range routes {
		fmt.Printf("Registering path %s\n", route.Path)
		router.HandleFunc(route.Path, route.Handler)
	}

	n := negroni.New(negroni.NewRecovery(), negroni.NewLogger(), negroni.NewStatic(http.Dir("public")))
	n.UseHandler(router)

	n.Run(":3000")
}
