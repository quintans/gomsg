package main

import (
	"fmt"
	"time"

	"github.com/quintans/gomsg"
)

func main() {
	// routing between client 1 and client 2
	server1 := gomsg.NewServer()
	server1.Listen(":7777")
	server2 := gomsg.NewServer()
	server2.Listen(":7778")
	gomsg.Route("*", server1, server2, time.Second,
		func(ctx *gomsg.Request) bool {
			fmt.Println("=====>incoming")
			return true
		})

	cli := gomsg.NewClient().Connect("localhost:7777")
	cli2 := gomsg.NewClient()
	cli2.Handle("HELLO", func(ctx gomsg.Response, m string) (string, error) {
		fmt.Println("<=== processing:", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("Hello %s", m), nil
	})
	cli2.Connect("localhost:7778")

	var err error
	/*
		err = <-cli.Push("XPTO", "PUSH: One")
		if err != nil {
			fmt.Println("W: error:", err)
		}
	*/
	err = <-cli.Request("HELLO", "World!", func(ctx gomsg.Response, r string, e error) {
		fmt.Println("=================> reply:", r, e, "from", ctx.Connection().RemoteAddr())
	})
	if err != nil {
		fmt.Println("===> error:", err)
	}

	time.Sleep(time.Millisecond * 100)
	cli.Destroy()
	time.Sleep(time.Millisecond * 100)
}
