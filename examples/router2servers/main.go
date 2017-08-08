package main

import (
	"fmt"
	"time"

	"github.com/quintans/gomsg"
)

const (
	MESSAGE = "World!"
	REPLY   = "Hello World!"
)

func wait() {
	time.Sleep(time.Millisecond * 10)
}

func main() {
	// routing requests between server 1 and server 2
	server1 := gomsg.NewServer()
	server1.SetTimeout(time.Second)
	server1.Listen(":7777")
	server2 := gomsg.NewServer()
	server2.SetTimeout(time.Second)
	server2.Listen(":7778")
	wait()

	// all (*) messages arriving to server 1 are routed to server 2
	gomsg.Route("*", server1, server2, time.Second,
		func(ctx *gomsg.Request) bool {
			fmt.Println("[*] ===>routing incoming msg:", string(ctx.Payload()))
			return true
		},
		nil)

	// client 1 connects to server 1
	cli := gomsg.NewClient()
	<-cli.Connect("localhost:7777")

	cli2 := gomsg.NewClient()
	cli2.Handle("HELLO", func(ctx *gomsg.Request, m string) (string, error) {
		if m != MESSAGE {
			fmt.Printf("[2] ###> EXPECTED '%s'. RECEIVED '%s'.\n", MESSAGE, m)
		}
		fmt.Println("[2] <=== processing:", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("Hello %s", m), nil
	})
	// client 2 connects to server 2
	<-cli2.Connect("localhost:7778")

	var err error
	/*
		err = <-cli.Push("XPTO", "PUSH: One")
		if err != nil {
			fmt.Println("W: error:", err)
		}
	*/
	err = <-cli.Request("HELLO", MESSAGE, func(ctx gomsg.Response, r string, e error) {
		if r != REPLY {
			fmt.Printf("[1] ###> EXPECTED '%s'. RECEIVED '%s'.\n", REPLY, r)
		}
		fmt.Println("[1] =================> reply:", r, e, "from", ctx.Connection().RemoteAddr())
	})
	if err != nil {
		fmt.Println("===> error:", err)
	}

	time.Sleep(time.Second * 3)
	cli.Destroy()
	wait()
}
