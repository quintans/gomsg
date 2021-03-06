package main

import (
	"fmt"
	"time"

	"github.com/quintans/gomsg"
	"github.com/quintans/toolkit/log"
)

func init() {
	log.Register("/", log.DEBUG).ShowCaller(true)
}

func main() {
	var logger = log.LoggerFor("/").SetCallerAt(2)
	server := gomsg.NewServer()
	server.SetLogger(log.Wrap{logger, "{server}"})
	server.Listen(":7777")
	cli := gomsg.NewClient()
	cli.SetLogger(log.Wrap{logger, "{cli}"})
	cli.Connect("localhost:7777")

	// this late server handler must propagate to the client
	server.Handle("XPTO", func(m string) {
		fmt.Println("<=== handling pull:", m)
	})
	time.Sleep(time.Millisecond * 100)

	e := <-cli.Publish("XPTO", "teste")
	if e != nil {
		fmt.Println("===> XPTO error:", e)
	}

	// this late client handler must propagate to the server
	cli.Handle("SUB", func(m string) {
		fmt.Println("<=== handling pub-sub:", m)
	})
	time.Sleep(time.Millisecond * 100)

	e = <-server.Publish("SUB", "teste")
	if e != nil {
		fmt.Println("===> SUB error:", e)
	}

	time.Sleep(time.Millisecond * 100)
}
