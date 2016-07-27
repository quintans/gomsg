package main

import (
	"fmt"
	"time"

	"github.com/quintans/gomsg"
)

func main() {
	server := gomsg.NewServer()
	server.SetTimeout(time.Second)
	server.Listen(":7777")

	cli := gomsg.NewClient()
	cli.Connect("localhost:7777")

	time.Sleep(time.Second)

	fmt.Println("=========== Reconnecting ==============")
	cli.Reconnect()

	time.Sleep(time.Second)
}
