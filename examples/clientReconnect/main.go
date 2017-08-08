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
	server.AddNewTopicListener(func(event gomsg.TopicEvent) {
		fmt.Printf("===> New topic %s at %s\n", event.Name, event.SourceAddr)
	})
	server.AddDropTopicListener(func(event gomsg.TopicEvent) {
		fmt.Printf("===> Droped topic %s at %s\n", event.Name, event.SourceAddr)
	})
	server.Listen(":7777")

	time.Sleep(time.Second)

	cli := gomsg.NewClient()
	<-cli.Connect("localhost:7777")
	cli.Handle("CENAS", func() {})
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
