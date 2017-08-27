package main

import (
	"fmt"
	"time"

	. "github.com/quintans/gomsg"
	"github.com/quintans/toolkit/log"
)

const (
	SERVER_PORT_1 = ":7777"
	SERVER_1      = "localhost" + SERVER_PORT_1
	SERVER_PORT_2 = ":7778"
	SERVER_2      = "localhost" + SERVER_PORT_2

	FORMAT_ERROR = "Expected '%v', got '%v'."

	MESSAGE  = "hello"
	PRODUCER = "PRODUCER"
	CONSUMER = "CONSUMER"
)

func wait() {
	time.Sleep(time.Millisecond * 100)
}

func waitASecond() {
	time.Sleep(time.Second)
}

func init() {
	log.Register("/", log.DEBUG).ShowCaller(true)
}

var logger = log.LoggerFor("/").SetCallerAt(2)

func main() {
	var request string
	server := NewServer()
	server.SetLogger(log.Wrap{logger, "{server}"})
	server.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< handling client pub:", m)
		request = m
	})

	go func() {
		if err := <-server.Listen(SERVER_PORT_1); err != nil {
			panic(fmt.Sprintf("Failed when listening. %+v", err))
		}
		fmt.Println("Destroying server")
	}()

	// give time to connect
	wait()

	cli := NewClient()
	cli.SetLogger(log.Wrap{logger, "{cli}"})
	if err := <-cli.Connect(SERVER_1); err != nil {
		panic(fmt.Sprintf("Failed when trying to connect. %+v", err))
	}
	// give time to connect
	wait()

	e := <-cli.Publish(CONSUMER, MESSAGE)
	// publish does wait to see if the message was delivered,
	// so we have wait if we want to check the delivery
	wait()

	if e != nil {
		panic(fmt.Sprintf("Error: %s", e))
	}
	if request != MESSAGE {
		panic(fmt.Sprintf(FORMAT_ERROR, MESSAGE, request))
	}

	request = "" // reset
	// this late client handler must propagate to the server
	cli.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< handling server pub:", m)
		request = m
	})
	// give time to inform the server of this new topic
	wait()

	server.Publish(CONSUMER, MESSAGE)
	wait()

	if request != MESSAGE {
		panic(fmt.Sprintf(FORMAT_ERROR, MESSAGE, request))
	}

	fmt.Println("Destroying client...")
	cli.Destroy()
	fmt.Println("Destroying server...")
	server.Destroy()
}
