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
	server.Route("*", time.Second, nil)
	/*
		server.Route("*", time.Second,
			func(ctx gomsg.IRequest) {
				fmt.Println("=====>incoming: ", string(ctx.Request()))
			},
			func(ctx gomsg.IResponse) {
				fmt.Println("<=====returning: ", string(ctx.Reply()))
			})
	*/

	cli := gomsg.NewClient()
	cli.Handle("HELLO", func(ctx *gomsg.Request, m string) (string, error) {
		fmt.Println("<=== [1] processing:", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("[1] Hello %s", m), nil
	})
	// sends multi reply
	cli.Handle("PIPE", func(ctx *gomsg.Request) {
		fmt.Println("<=== piping")
		ctx.SendReply([]byte(fmt.Sprint("Pipe #1")))
		ctx.SendReply([]byte(fmt.Sprint("Pipe #2")))
		/*
			ctx.SendReply([]byte(fmt.Sprint("Pipe #3")))
			ctx.SendReply([]byte(fmt.Sprint("Pipe #4")))
		*/
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
	cli2.Connect("localhost:7777")

	<-cli3.RequestAll("HELLO", "World!", func(ctx gomsg.Response, r string, e error) {
		fmt.Println("=================> reply:", r, e, "from", ctx.Connection().RemoteAddr())
	})

	<-cli3.Request("PIPE", nil, func(ctx gomsg.Response, s string) {
		fmt.Println("=================> reply:", ctx.Kind, string(ctx.Reply()), s)
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
