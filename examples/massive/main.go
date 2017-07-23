package main

import (
	"fmt"
	"time"

	"github.com/quintans/gomsg"
	"github.com/quintans/toolkit/log"
)

func init() {
	log.Register("/", log.WARN).ShowCaller(true)
}

const TOPIC = "ToPic"

func wait() {
	time.Sleep(time.Millisecond * 10)
}

func createClient(reply string) *gomsg.Client {
	var cli = gomsg.NewClient()
	cli.Handle(TOPIC, func() string {
		return reply
	})
	<-cli.Connect("localhost:7777")
	return cli
}

func main() {
	server := gomsg.NewServer()
	server.SetTimeout(time.Second)
	server.Listen(":7777")

	createClient("One")
	createClient("Two")
	createClient("Three")
	createClient("Four")

	var now = time.Now()
	for i := 0; i < 10000; i++ {
		<-server.RequestAll(TOPIC, "X", func(s string) {
			var str = s
			if s == "" {
				str = "[END]"
			}
			fmt.Println("=====>", str)
		}, time.Second)
	}
	fmt.Println(time.Now().Sub(now))
}
