package main

import (
	"fmt"
	"net/http"

	"code.google.com/p/log4go"
	"github.com/codegangsta/negroni"
	"github.com/gorilla/mux"
	"github.com/iParadigms/walker/console"
)

func main() {
	// //DEBUG CODE: leave here for now
	// err := walker.CreateCassandraSchema()
	// if err != nil {
	// 	panic(err)
	// }
	// // DURING TESTING THIS WILL REMAIN

	ds, err := console.NewCqlDataStore()
	if err != nil {
		panic(fmt.Errorf("Failed to start data source: %v", err))
	}
	console.DS = ds
	defer ds.Close()

	router := mux.NewRouter()
	routes := console.Routes()
	for _, route := range routes {
		log4go.Info("Registering path %s", route.Path)
		router.HandleFunc(route.Path, route.Handler)
	}

	n := negroni.New(negroni.NewRecovery(), negroni.NewLogger(), negroni.NewStatic(http.Dir("public")))
	n.UseHandler(router)
	n.Run(":3000")
}
