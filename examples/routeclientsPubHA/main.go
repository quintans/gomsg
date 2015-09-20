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

	cli := gomsg.NewClient()
	cli.Handle("HELLO", func(m string) {
		fmt.Println("<=== [1] processing:", m)
	})
	cli.SetGroupId("HA")
	cli.Connect("localhost:7777")

	cli2 := gomsg.NewClient()
	cli2.Handle("HELLO", func(m string) {
		fmt.Println("<=== [2] processing:", m)
	})
	cli2.SetGroupId("HA")
	cli2.Connect("localhost:7777")

	// just to get in the way
	cli3 := gomsg.NewClient()
	cli3.Connect("localhost:7777")

	// Only one element of the group HA will process each message, alternately (round robin).
	cli3.Publish("HELLO", "Hello World!")
	cli3.Publish("HELLO", "Olá Mundo!")
	cli3.Publish("HELLO", "YESSSS!")
	cli3.Publish("HELLO", "one")
	cli3.Publish("HELLO", "two")
	cli3.Publish("HELLO", "three")
	cli3.Publish("HELLO", "test")

	//time.Sleep(time.Millisecond * 100)
	//cli.Destroy()
	time.Sleep(time.Millisecond * 100)
}
