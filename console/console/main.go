package main

import (
	"os"

	"code.google.com/p/log4go"
	"github.com/iParadigms/walker"
	"github.com/iParadigms/walker/console"
)

func modifyConfigMain() {
	walker.Config.Cassandra.Keyspace = "walker_spoofed"
	walker.Config.Cassandra.Hosts = []string{"localhost"}
	walker.Config.Cassandra.ReplicationFactor = 1
}

func main() {
	if true {
		modifyConfigMain()
		console.SpoofData()
	}

	log4go.AddFilter("stdout", log4go.FINE, log4go.NewConsoleLogWriter())
	log4go.Error("Console pid is %d", os.Getpid())

	console.Run()
}
