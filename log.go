package walker

import (
	"os"
	"os/signal"
	"syscall"

	"code.google.com/p/log4go"
)

const logname = "log4go.xml"

// init sets the default log4go configuration and attempts to read a log4go.xml
// file if available
func init() {
	log4go.AddFilter("stdout", log4go.INFO, log4go.NewConsoleLogWriter())
	loadLog4goConfig()
	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGHUP)
	go func() {
		for {
			<-sig
			loadLog4goConfig()
		}
	}()
}

func loadLog4goConfig() {
	log4go.Debug("Loading configuration")
	_, err := os.Stat(logname)
	if os.IsNotExist(err) {
		return
	}
	log4go.LoadConfiguration(logname)
}
