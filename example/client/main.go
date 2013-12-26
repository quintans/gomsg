package main

import (
	"flag"
	"fmt"
	"time"

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
	/*
		go func() {
			server := gobus.NewServer(nil)
			go server.Start("tcp", "6000")

			logger.Infof("listening at port 6000")

			//sleep forever
			select {}
		}()
	*/

	var err error

	cli1 := gobus.NewClient("tcp", "127.0.0.1:6000", nil)
	//cli1.OnDisconnect = func() {
	//	logger.Debugf("===> disconnecting client #1")
	//}
	//cli1.OnConnect = func() {
	//	logger.Debugf("===> connecting client #1: %s", cli1.GetConnection().LocalAddr())
	//}

	cli2 := gobus.NewClient("tcp", "127.0.0.1:6000", nil)
	//cli2.OnDisconnect = func() {
	//	logger.Debugf("===> disconnecting client #2")
	//}
	//cli2.OnConnect = func() {
	//	logger.Debugf("===> connecting client #2: %s", cli2.GetConnection().LocalAddr())
	//}
	cli3 := gobus.NewClient("tcp", "127.0.0.1:6000", nil)

	// give time to client startup
	time.Sleep(time.Second)

	/***********************
	PUBLISH-SUBSCRIBE
	************************/
	// subsribe
	//cli1.Subscribe("hello", func(msg string) {
	//	logger.Debugf("===> Client #1 received message: %s", msg)
	//})
	//cli2.Subscribe("hello", func(msg string) {
	//	logger.Debugf("===> Client #2 received message: %s", msg)
	//})
	//cli3.Subscribe("hello", func(msg string) {
	//	logger.Debugf("===> Client #3 received message: %s", msg)
	//})
	////time.Sleep(time.Second)
	//// publish
	//logger.Debugf("publishing on client #1")
	//err = cli1.Publish("hello", "Hello from #1", errorHandler)
	//errorHandler(err)
	/*
		cli2.Publish("hello", "Hello from #2", func(err error) {
			logger.Errorf("%s", err.Error())
		})
	*/

	/***********************
	REQUEST-REPLY
	************************/
	// Reply multiple
	err = cli1.ReplyAllways("lucky", func() int {
		return 1
	})
	errorHandler(err)

	err = cli2.ReplyAllways("lucky", func() int {
		return 2
	})
	errorHandler(err)

	//err = cli3.CallMany("lucky", nil,
	//	func(result []int) {
	//		logger.Debugf("lucky result: %v", result)
	//	},
	//	errorHandler)
	//errorHandler(err)

	//err = cli3.CallMany("lucky", nil,
	//	func(result []int) {
	//		logger.Debugf("lucky result: %v", result)
	//	},
	//	errorHandler)
	//errorHandler(err)

	err = cli3.CallOne("lucky", nil,
		func(result int) {
			logger.Debugf("lucky result: %v", result)
		},
		errorHandler)
	errorHandler(err)

	//sleep for a while so that the program doesn’t exit immediately
	time.Sleep(1 * time.Second)

	// Stop all
	//cli1.Stop()
	//cli2.Stop()
	//cli3.Stop()
	//server.Stop()

	//sleep for a while so that the program doesn’t exit immediately
	//time.Sleep(time.Second)
	//sleep forever
	select {}
}

func errorHandler(err error) {
	if err != nil {
		logger.Errorf("%s", err.Error())
	} else {
		logger.CallerAt(1).Errorf("Hello")
	}
}
