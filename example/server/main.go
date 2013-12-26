package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/quintans/goBus"
	"github.com/quintans/toolkit/log"
)

var logger = log.LoggerFor("main")

func init() {
	/*
	 * ===================
	 * BEGIN CONFIGURATION
	 * ===================
	 */
	logLevelName := flag.String("logLevel", log.ERROR.String(), "log level. values are DEBUG, INFO, WARN, ERROR, FATAL, NONE. default: ERROR")
	flag.Parse()
	logLevel := log.ParseLevel(*logLevelName, log.ERROR)
	if logLevel <= log.INFO {
		log.ShowCaller(true)
	}

	log.Register("/", logLevel, log.NewConsoleAppender(false))

	//log.SetLevel("gobus", log.DEBUG)

	fmt.Println("log level:", logLevel)
	/*
	 * ===================
	 * END CONFIGURATION
	 * ===================
	 */
}

func main() {
	server := gobus.NewServer(nil)
	logger.Infof("listening at port 6000")
	err := server.Start("tcp", "6000")
	if err != nil {
		logger.Errorf("%s", err.Error())
		os.Exit(1)
	}

	//sleep forever
	select {}
}
