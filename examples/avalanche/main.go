package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quintans/gomsg"
	"github.com/quintans/toolkit/log"
)

func init() {
	log.Register("/", log.WARN).ShowCaller(true)
	gomsg.SetLogger(log.LoggerFor("/"))
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

	var wg sync.WaitGroup
	var now = time.Now()
	var replies int32 = 0
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			<-server.RequestAll(TOPIC, "X", func(s string) {
				if s != "" { // end mark
					atomic.AddInt32(&replies, 1)
				}
				/*
					var str = s
					if s == "" {
						str = "[END]"
					}
					fmt.Println("=====>", str)
				*/
			}, time.Second)
			wg.Done()
		}()
	}
	wg.Wait()
	var delta = time.Now().Sub(now)
	fmt.Printf("1000 requests => %v replies in %v\n", replies, delta)
}
