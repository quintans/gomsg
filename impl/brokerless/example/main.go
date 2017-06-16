package main

import (
	"fmt"
	"math/rand"
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

func uuid() []byte {
	var b = make([]byte, 16)
	rand.Read(b)
	return b
}

const (
	SERVICE_GREETING = "GREETING"
)

var codec = gomsg.JsonCodec{}

func main() {
	var cli1 = brokerless.NewPeer(uuid())
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

	var uuid3 = uuid()
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

	<-cli1.RequestAll(SERVICE_GREETING, "#1", func(reply string) {
		var str = reply
		if reply == "" {
			str = "[END]"
		}
		fmt.Println("=====>", str)
	})

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
