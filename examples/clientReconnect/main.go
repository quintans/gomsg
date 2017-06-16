package main

import (
	"fmt"
	"time"

	"github.com/quintans/gomsg"
)

func main() {
	server := gomsg.NewServer()
	server.SetTimeout(time.Second)
	server.Handle("PING", func() {})
	server.Listen(":7777")

	time.Sleep(time.Second)

	cli := gomsg.NewClient()
	<-cli.Connect("localhost:7777")
	<-cli.Request("PING", nil, func() {
		fmt.Println("===> ping OK")
	})

	time.Sleep(time.Second)

	fmt.Println("=========== Reconnecting ==============")
	cli.Disconnect()

	time.Sleep(time.Second)

	fmt.Println("=========== PING (again) ==============")
	cli.Request("PING", nil, func() {
		fmt.Println("===> ping OK")
	})

	time.Sleep(time.Second)
}
