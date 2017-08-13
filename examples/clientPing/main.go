package main

import (
	"fmt"
	"net"
	"time"

	"github.com/quintans/gomsg"
)

const (
	CLEAN_CYCLE = time.Second
)

func wait() {
	time.Sleep(time.Millisecond * 20)
}

func main() {
	server := gomsg.NewServer()
	server.Name = "Server"
	// keep alive
	var timeout = gomsg.NewTimeout(CLEAN_CYCLE, time.Second, func(o interface{}) {
		c := o.(net.Conn)
		fmt.Println("=====> killing connection from", c.RemoteAddr())
		server.Wires.Kill(c)
	})

	server.OnConnect = func(w *gomsg.Wire) {
		// starts monitoring
		timeout.Delay(w.Conn())
	}
	// for PING messages the server will reply immediatly,
	// without relaying the message to the clients.
	server.Route("*", time.Second,
		func(ctx *gomsg.Request) bool {
			// any message received, delays the timeout
			timeout.Delay(ctx.Connection())

			if ctx.Name == "PING" {
				ctx.SetReply([]byte("PONG"))
				return false
			}

			return true
		},
		nil)

	server.Listen(":7777")
	wait()

	cli := gomsg.NewClient()
	cli.Name = "Cli#1"
	cli.SetReconnectInterval(0)
	cli.Handle("HELLO", func(ctx *gomsg.Request, m string) (string, error) {
		fmt.Println("<=== [1] processing:", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("[1] Hello %s", m), nil
	})
	<-cli.Connect("localhost:7777")

	// just to get in the way
	cli3 := gomsg.NewClient()
	cli3.Name = "Cli#3"
	cli3.SetReconnectInterval(0)
	<-cli3.Connect("localhost:7777")

	cli2 := gomsg.NewClient()
	cli2.Name = "Cli#2"
	cli2.SetReconnectInterval(0)
	cli2.Handle("HELLO", func(ctx *gomsg.Request, m string) (string, error) {
		fmt.Println("<=== [2] processing:", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("[2] Hello %s", m), nil
	})
	<-cli2.Connect("localhost:7777")

	var err = <-cli3.RequestAll("HELLO", "Hello", func(ctx gomsg.Response, r string, e error) {
		fmt.Println("=[3]==HELLO===> reply:", r, e, "from", ctx.Connection().RemoteAddr())
	})
	if err != nil {
		fmt.Println("Error on calling the first HELLO:", err)
	}

	time.Sleep(time.Millisecond * 2000)
	err = <-cli3.RequestAll("HELLO", "World!", func(ctx gomsg.Response, r string, e error) {
		fmt.Println("=[3]==HELLO===> reply:", r, e, "from", ctx.Connection().RemoteAddr())
	})
	if err != nil {
		fmt.Println("Expected error on calling the second HELLO:", err)
	}
}
