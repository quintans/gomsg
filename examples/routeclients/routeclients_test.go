package routeclients

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/quintans/gomsg"
	"github.com/quintans/toolkit/log"
)

func init() {
	log.Register("/", log.DEBUG).ShowCaller(true)
}

func wait() {
	time.Sleep(time.Millisecond * 20)
}

func TestRouteClients(t *testing.T) {
	var l = log.LoggerFor("github.com/quintans/gmsg").SetCallerAt(2)
	// all messages arriving to the server are routed to the clients
	server := gomsg.NewServer()
	server.SetLogger(log.Wrap{l, "{server} "})
	server.Route("*", time.Second, nil, nil)
	server.Listen(":7777")
	wait()

	var pipecount = 0
	var hellocount = 0

	cli := gomsg.NewClient()
	cli.SetLogger(log.Wrap{l, "{cli} "})
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
	cli3.SetLogger(log.Wrap{l, "{cli3} "})
	cli3.Connect("localhost:7777")

	cli2 := gomsg.NewClient()
	cli2.SetLogger(log.Wrap{l, "{cli2} "})
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
	check(err, t)
	wait()
	if hellocount != 3 {
		fmt.Println("Expected 3 hellocount, got", hellocount)
		os.Exit(1)
	}

	fmt.Println("=======REQUEST ALL PIPEs========")
	err = <-cli3.RequestAll("PIPE", nil, func(ctx gomsg.Response, s string) {
		pipecount++
		fmt.Println("===PIPE===> reply:", ctx.Kind, string(ctx.Reply()), "=", s)
	})
	check(err, t)
	if pipecount != 4 {
		t.Fatal("Expected 4 server hits, got", pipecount)
	}

	fmt.Println("===============")
	err = <-cli3.Request("PIPE", nil, func(ctx gomsg.Response, s string) {
		fmt.Println("===PIPE===> reply:", ctx.Kind, string(ctx.Reply()), "=", s)
	})
	check(err, t)
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

	cli.Destroy()
	wait()
}

func check(err error, t *testing.T) {
	if err != nil {
		t.Fatal(err)
	}
}
