package main

import (
	"fmt"
	"time"

	"github.com/quintans/gomsg"
)

func main() {
	server := gomsg.NewServer()
	server.Listen(":7777")
	cli := gomsg.NewClient().Connect("localhost:7777")

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
