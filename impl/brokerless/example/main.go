package main

import (
	"fmt"
	"os"
	"time"

	"github.com/quintans/gomsg"
	"github.com/quintans/gomsg/impl/brokerless"
	"github.com/quintans/toolkit/log"
)

func wait() {
	time.Sleep(time.Millisecond * 100)
}

func init() {
	log.Register("/", log.INFO).ShowCaller(true)
}

const (
	SERVICE_GREETING = "GREETING"
)

var codec = gomsg.JsonCodec{}

func main() {
	var cli1 = brokerless.NewPeer(gomsg.NewUUID())
	cli1.Handle(SERVICE_GREETING, func(greeting string) string {
		return "#1: hi " + greeting
	})
	cli1.Connect(":7001")

	/*
		var cli2 = brokerless.NewPeer(uuid())
		cli2.Handle(SERVICE_GREETING, func(greeting string) string {
			return "#2: hi " + greeting
		})
		cli2.Connect(":7002")
	*/

	wait()

	var uuid3 = gomsg.NewUUID()
	var cli3 = brokerless.NewPeer(uuid3)
	// the same as cli2
	cli3.Handle(SERVICE_GREETING, func(r *gomsg.Request) {
		var greeting string
		codec.Decode(r.Payload(), &greeting)
		//return "hi from #3"
		// in json format because is going to be decoded
		r.SetReply([]byte("\"#3: hi " + greeting + "\""))
	})
	cli3.Connect(":7003")
	wait()

	time.Sleep(time.Second * 2)

	var cnt = 0
	<-cli1.RequestAll(SERVICE_GREETING, "#1", func(reply string) {
		cnt++
		var str = reply
		if reply == "" {
			str = "[END]"
		}
		fmt.Println("=====>", str)
	})
	if cnt != 2 {
		fmt.Println("ERROR =====> expected 2, got", cnt)
		os.Exit(1)
	}

	// replies should rotate
	/*
		for i := 0; i < 3; i++ {
			<-cli2.Request(SERVICE_GREETING, "#2", func(r gomsg.Response) {
				fmt.Println("=====>", string(r.Reply()))
			})
		}
	*/

	cli3.Destroy()
	fmt.Println("Waiting...")
	time.Sleep(time.Second * 2)
	// does it reconnect?
	cli3 = brokerless.NewPeer(uuid3)
	cli3.Connect(":7003")
	time.Sleep(time.Second * 7)
}
