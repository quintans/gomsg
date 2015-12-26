package main

import (
	"fmt"
	"time"

	"github.com/quintans/gomsg"
)

func main() {
	// all messages arriving to the server are routed to the clients
	server := gomsg.NewServer()
	server.Listen(":7777")
	server.Route("*", time.Second, nil, nil)

	cli := gomsg.NewClient()
	cli.Handle("HELLO", func(ctx *gomsg.Request, m string) (string, error) {
		fmt.Println("<=== [1] processing:", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("[1] Hello %s", m), nil
	})
	// sends multi reply
	cli.Handle("PIPE", func(ctx *gomsg.Request) {
		ctx.SendReply([]byte("\"Pipe #1\""))
		ctx.SendReply([]byte("\"Pipe #2\""))
		ctx.Terminate()
	})
	cli.Connect("localhost:7777")

	// just to get in the way
	cli3 := gomsg.NewClient()
	cli3.Connect("localhost:7777")

	cli2 := gomsg.NewClient()
	cli2.Handle("HELLO", func(ctx *gomsg.Request, m string) (string, error) {
		fmt.Println("<=== [2] processing:", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("[2] Hello %s", m), nil
	})
	cli2.Handle("PIPE", func(ctx *gomsg.Request) string {
		return "Pipe #201"
	})
	cli2.Connect("localhost:7777")

	<-cli3.RequestAll("HELLO", "World!", func(ctx gomsg.Response, r string, e error) {
		fmt.Println("===HELLO===> reply:", r, e, "from", ctx.Connection().RemoteAddr())
	})
	fmt.Println("===============")
	<-cli3.RequestAll("PIPE", nil, func(ctx gomsg.Response, s string) {
		fmt.Println("===PIPE===> reply:", ctx.Kind, string(ctx.Reply()), s)
	})
	fmt.Println("===============")
	<-cli3.Request("PIPE", nil, func(ctx gomsg.Response, s string) {
		fmt.Println("===PIPE===> reply:", ctx.Kind, string(ctx.Reply()), s)
	})
	/*
		cli.Request("HELLO", "World!", func(ctx gomsg.IResponse, r string, e error) {
			fmt.Println("=================> reply:", r, e, "from", ctx.Connection().RemoteAddr())
		})

		// check to see if the server is working
		server.Request("HELLO", "World!", func(ctx gomsg.IResponse, r string, e error) {
			fmt.Println("=================> reply:", r, e, "from", ctx.Connection().RemoteAddr())
		})
		server.Request("HELLO", "World!", func(ctx gomsg.IResponse, r string, e error) {
			fmt.Println("=================> reply:", r, e, "from", ctx.Connection().RemoteAddr())
		})
	*/

	time.Sleep(time.Millisecond * 100)
	cli.Destroy()
	time.Sleep(time.Millisecond * 100)
}
