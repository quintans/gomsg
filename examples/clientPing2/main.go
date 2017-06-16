package main

import (
	"fmt"
	"time"

	"github.com/quintans/gomsg"
)

const (
	PING = "PING"
)

func wait() {
	time.Sleep(time.Millisecond * 10)
}

func main() {
	var server = gomsg.NewServer()
	server.Handle(PING, func() string {
		return "PONG"
	})
	server.Listen(":7777")
	wait()

	var cli1 = gomsg.NewClient()
	cli1.Handle(PING, func() string {
		return "#1: ERROR"
	})
	<-cli1.Connect("localhost:7777")

	var cli2 = gomsg.NewClient()
	cli2.Handle(PING, func() string {
		return "#2: ERROR"
	})
	<-cli2.Connect("localhost:7777")

	// only the server should responde
	var reply string
	<-cli2.Request(PING, nil, func(str string) {
		fmt.Println("<", str)
		reply = str
	})

	if reply != "PONG" {
		fmt.Printf("ERROR: Got %s, expected PONG\n", reply)
	}
}
