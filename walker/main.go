package main

import (
	"fmt"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/iParadigms/walker"
	"github.com/spf13/cobra"
)

func fatalf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	os.Exit(1)
}

func main() {

	var config string
	walkerCommand := &cobra.Command{
		Use: "walker",
	}
	walkerCommand.Flags().StringVar(&config, "config", "", "path to a config file to load")

	crawlCommand := &cobra.Command{
		Use:   "crawl",
		Short: "start an all-in-one crawler downloading to the current directory",
		Run: func(cmd *cobra.Command, args []string) {
			ds, err := walker.NewCassandraDatastore()
			if err != nil {
				fatalf("Failed creating Cassandra datastore: %v", err)
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
	walkerCommand.AddCommand(crawlCommand)

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

	var seedURL string
	seedCommand := &cobra.Command{
		Use:   "seed",
		Short: "add a seed URL to the datastore",
		Run: func(cmd *cobra.Command, args []string) {
			if seedURL == "" {
				fatalf("Seed URL needed to execute; add on with --url/-u")
			}
			u, err := url.Parse(seedURL)
			if err != nil {
				fatalf("Could not parse %v as a url: %v", seedURL, err)
			}

			ds, err := walker.NewCassandraDatastore()
			if err != nil {
				fatalf("Failed creating Cassandra datastore: %v", err)
			}

			ds.StoreParsedURL(u, nil)
		},
	}
	seedCommand.Flags().StringVarP(&seedURL, "url", "u", "", "URL to add as a seed")
	walkerCommand.AddCommand(seedCommand)

	walkerCommand.Execute()
}
