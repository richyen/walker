package main

import (
	"fmt"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/iParadigms/walker"
	"github.com/spf13/cobra"
)

var config string

func main() {
	walkerCommand := &cobra.Command{
		Use:   "walker",
		Short: "start an all-in-one crawler",
		Run: func(cmd *cobra.Command, args []string) {
			ds, err := walker.NewCassandraDatastore()
			if err != nil {
				fmt.Printf("Failed creating Cassandra datastore: %v", err)
				os.Exit(1)
			}

			h := &walker.SimpleWriterHandler{}

			manager := &walker.CrawlManager{}
			manager.SetDatastore(ds)
			manager.AddHandler(h)
			managerDone := make(chan bool)
			go func() {
				manager.Start()
				managerDone <- true
			}()

			dispatcher := &walker.Dispatcher{}
			dispatcherDone := make(chan bool)
			go func() {
				dispatcher.Start()
				dispatcherDone <- true
			}()

			sig := make(chan os.Signal)
			signal.Notify(sig, syscall.SIGINT)
			<-sig

			dispatcher.Stop()
			manager.Stop()
			<-dispatcherDone
			<-managerDone
		},
	}
	walkerCommand.Flags().StringVar(&config, "config", "", "path to a config file to load")

	fetchCommand := &cobra.Command{
		Use:   "fetch",
		Short: "start a fetcher (no dispatcher)",
		Run: func(cmd *cobra.Command, args []string) {
			//TODO
		},
	}
	walkerCommand.AddCommand(fetchCommand)

	dispatchCommand := &cobra.Command{
		Use:   "dispatch",
		Short: "start a dispatcher (no fetcher)",
		Run: func(cmd *cobra.Command, args []string) {
			//TODO
		},
	}
	walkerCommand.AddCommand(dispatchCommand)
	walkerCommand.Execute()
}
