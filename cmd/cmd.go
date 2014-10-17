package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/iParadigms/walker"
	"github.com/iParadigms/walker/console"
	"github.com/spf13/cobra"
)

// cmd provides an easy interface for creating walker binaries that use their
// own Handler, Datastore, or Dispatcher. A crawler that uses the default for
// each of these requires simply:
//
//  func main() {
//      cmd.Execute()
//  }
//
// To create your own binary that uses walker's flags but has its own handler:
//
//  func main() {
//      cmd.Handler(NewMyHandler())
//      cmd.Execute()
//  }
//
// Likewise if you want to set your own Datastore and Dispatcher:
//
//  func main() {
//      cmd.DataStore(NewMyDatastore())
//      cmd.Dispatcher(NewMyDatastore())
//      cmd.Execute()
//  }
//
// cmd.Execute() blocks until the program has completed (usually by
// being shutdown gracefully via SIGINT).

var commander struct {
	*cobra.Command
	Handler    walker.Handler
	Datastore  walker.Datastore
	Dispatcher walker.Dispatcher
}

func Handler(h walker.Handler) {
	commander.Handler = h
}

func Datastore(d walker.Datastore) {
	commander.Datastore = d
}

func Dispatcher(d walker.Dispatcher) {
	commander.Dispatcher = d
}

func Execute() {
	commander.Execute()
}

func fatalf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Println()
	os.Exit(1)
}

func init() {
	walkerCommand := &cobra.Command{
		Use: "walker",
	}

	var config string
	walkerCommand.PersistentFlags().StringVarP(&config,
		"config", "c", "", "path to a config file to load")
	readConfig := func() {
		if config != "" {
			if err := walker.ReadConfigFile(config); err != nil {
				panic(err.Error())
			}
		}
	}

	var noConsole bool = false
	crawlCommand := &cobra.Command{
		Use:   "crawl",
		Short: "start an all-in-one crawler",
		Run: func(cmd *cobra.Command, args []string) {
			readConfig()

			if commander.Datastore == nil {
				ds, err := walker.NewCassandraDatastore()
				if err != nil {
					fatalf("Failed creating Cassandra datastore: %v", err)
				}
				commander.Datastore = ds
				commander.Dispatcher = &walker.CassandraDispatcher{}
			}

			if commander.Handler == nil {
				commander.Handler = &walker.SimpleWriterHandler{}
			}

			manager := &walker.FetchManager{
				Datastore: commander.Datastore,
				Handler:   commander.Handler,
			}
			go manager.Start()

			if commander.Dispatcher != nil {
				go func() {
					err := commander.Dispatcher.StartDispatcher()
					if err != nil {
						panic(err.Error())
					}
				}()
			}

			if !noConsole {
				console.Start()
			}

			sig := make(chan os.Signal)
			signal.Notify(sig, syscall.SIGINT)
			<-sig

			if commander.Dispatcher != nil {
				commander.Dispatcher.StopDispatcher()
			}
			manager.Stop()
		},
	}
	crawlCommand.Flags().BoolVarP(&noConsole, "no-console", "C", false, "Do not start the console")
	walkerCommand.AddCommand(crawlCommand)

	fetchCommand := &cobra.Command{
		Use:   "fetch",
		Short: "start only a walker fetch manager",
		Run: func(cmd *cobra.Command, args []string) {
			readConfig()

			if commander.Datastore == nil {
				ds, err := walker.NewCassandraDatastore()
				if err != nil {
					fatalf("Failed creating Cassandra datastore: %v", err)
				}
				commander.Datastore = ds
				commander.Dispatcher = &walker.CassandraDispatcher{}
			}

			if commander.Handler == nil {
				commander.Handler = &walker.SimpleWriterHandler{}
			}

			manager := &walker.FetchManager{
				Datastore: commander.Datastore,
				Handler:   commander.Handler,
			}
			go manager.Start()

			sig := make(chan os.Signal)
			signal.Notify(sig, syscall.SIGINT)
			<-sig

			manager.Stop()
		},
	}
	walkerCommand.AddCommand(fetchCommand)

	dispatchCommand := &cobra.Command{
		Use:   "dispatch",
		Short: "start only a walker dispatcher",
		Run: func(cmd *cobra.Command, args []string) {
			readConfig()

			if commander.Dispatcher == nil {
				commander.Dispatcher = &walker.CassandraDispatcher{}
			}

			go func() {
				err := commander.Dispatcher.StartDispatcher()
				if err != nil {
					panic(err.Error())
				}
			}()

			sig := make(chan os.Signal)
			signal.Notify(sig, syscall.SIGINT)
			<-sig

			commander.Dispatcher.StopDispatcher()
		},
	}
	walkerCommand.AddCommand(dispatchCommand)

	var seedURL string
	seedCommand := &cobra.Command{
		Use:   "seed",
		Short: "add a seed URL to the datastore",
		Long: `Seed is useful for:
    - Adding starter links to bootstrap a broad crawl
    - Adding links when add_new_domains is false
    - Adding any other link that needs to be crawled soon

This command will insert the provided link and also add its domain to the
crawl, regardless of the add_new_domains configuration setting.`,
		Run: func(cmd *cobra.Command, args []string) {
			readConfig()

			orig := walker.Config.AddNewDomains
			defer func() { walker.Config.AddNewDomains = orig }()
			walker.Config.AddNewDomains = true

			if seedURL == "" {
				fatalf("Seed URL needed to execute; add on with --url/-u")
			}
			u, err := walker.ParseURL(seedURL)
			if err != nil {
				fatalf("Could not parse %v as a url: %v", seedURL, err)
			}

			if commander.Datastore == nil {
				ds, err := walker.NewCassandraDatastore()
				if err != nil {
					fatalf("Failed creating Cassandra datastore: %v", err)
				}
				commander.Datastore = ds
			}

			commander.Datastore.StoreParsedURL(u, nil)
		},
	}
	seedCommand.Flags().StringVarP(&seedURL, "url", "u", "", "URL to add as a seed")
	walkerCommand.AddCommand(seedCommand)

	var outfile string
	schemaCommand := &cobra.Command{
		Use:   "schema",
		Short: "output the walker schema",
		Long: `Schema prints the walker schema to stdout, substituting
schema-relevant configuration items (ex. keyspace, replication factor).
Useful for something like:
    $ <edit walker.yaml as desired>
    $ walker schema -o schema.cql
    $ <edit schema.cql further as desired>
    $ cqlsh -f schema.cql
`,
		Run: func(cmd *cobra.Command, args []string) {
			readConfig()
			if outfile == "" {
				fatalf("An output file is needed to execute; add with --out/-o")
			}

			out, err := os.Create(outfile)
			if err != nil {
				panic(err.Error())
			}
			defer out.Close()

			schema, err := walker.GetCassandraSchema()
			if err != nil {
				panic(err.Error())
			}
			fmt.Fprint(out, schema)
		},
	}
	schemaCommand.Flags().StringVarP(&outfile, "out", "o", "", "File to write output to")
	walkerCommand.AddCommand(schemaCommand)

	var spoofData bool = false
	consoleCommand := &cobra.Command{
		Use:   "console",
		Short: "Start up the walker console",
		Run: func(cmd *cobra.Command, args []string) {
			if spoofData {
				walker.Config.Cassandra.Keyspace = "walker_spoofed"
				walker.Config.Cassandra.Hosts = []string{"localhost"}
				walker.Config.Cassandra.ReplicationFactor = 1
				console.SpoofData()
			}
			readConfig()
			console.Run()
		},
	}
	consoleCommand.Flags().BoolVarP(&spoofData, "spoof", "s", false, "Populate Cassandra with some test data (temporary flag, will go away soon)")
	walkerCommand.AddCommand(consoleCommand)

	commander.Command = walkerCommand
}
