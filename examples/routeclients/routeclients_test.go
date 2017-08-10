package main

import (
	"fmt"
	"os"
	"time"

	"github.com/quintans/gomsg"
	"github.com/quintans/toolkit/log"
)

func init() {
	log.Register("/", log.DEBUG).ShowCaller(true)
	gomsg.SetLogger(log.LoggerFor("github.com/quintans/gmsg"))
}

func wait() {
	time.Sleep(time.Millisecond * 20)
}

func main() {
	// all messages arriving to the server are routed to the clients
	server := gomsg.NewServer()
	server.Route("*", time.Second, nil, nil)
	server.Listen(":7777")
	wait()

	var pipecount = 0
	var hellocount = 0

	cli := gomsg.NewClient()
	cli.Handle("HELLO", func(ctx *gomsg.Request, m string) (string, error) {
		fmt.Println("<=== [1] processing:", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("[1] Hello %s", m), nil
	})
	// sends multi reply
	cli.Handle("PIPE", func(ctx *gomsg.Request) {
		ctx.SendReply([]byte("\"Pipe #101\""))
		ctx.SendReply([]byte("\"Pipe #102\""))
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

	wait()

	var err = <-cli3.RequestAll("HELLO", "World!", func(ctx gomsg.Response, r string, e error) {
		hellocount++
		fmt.Println("===HELLO===> reply:", r, e, "from", ctx.Connection().RemoteAddr())
	})
	check(err)
	wait()
	if hellocount != 3 {
		fmt.Println("Expected 3 hellocount, got", hellocount)
		os.Exit(1)
	}

	fmt.Println("===============")
	err = <-cli3.RequestAll("PIPE", nil, func(ctx gomsg.Response, s string) {
		pipecount++
		fmt.Println("===PIPE===> reply:", ctx.Kind, string(ctx.Reply()), "=", s)
	})
	check(err)
	fmt.Println("===============")
	err = <-cli3.Request("PIPE", nil, func(ctx gomsg.Response, s string) {
		fmt.Println("===PIPE===> reply:", ctx.Kind, string(ctx.Reply()), "=", s)
	})
	check(err)
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

	wait()

	if pipecount != 4 {
		fmt.Println("Expected 4 server hits, got", pipecount)
		os.Exit(1)
	}

	cli.Destroy()
	wait()
}

func check(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
