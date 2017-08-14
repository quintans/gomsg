package main

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quintans/gomsg"
	"github.com/quintans/toolkit"
	"github.com/quintans/toolkit/log"
)

func init() {
	log.Register("/", log.WARN).ShowCaller(true)
}

const TOPIC = "ToPic"

func wait() {
	time.Sleep(time.Millisecond * 10)
}

func createClient(reply string, l log.ILogger, replies *int32) *gomsg.Client {
	var cli = gomsg.NewClient()
	cli.SetLogger(log.Wrap{l, fmt.Sprintf("{%s} ", reply)})
	cli.Handle(TOPIC, func(s string) string {
		atomic.AddInt32(replies, 1)
		//return reply + "-" + s
		return s
	})
	<-cli.Connect("localhost:7777")
	return cli
}

func main() {
	var l = log.LoggerFor("github.com/quintans/gmsg").SetCallerAt(2)
	server := gomsg.NewServer()
	server.SetLogger(log.Wrap{l, "{server} "})
	server.SetDefaultTimeout(time.Second)
	server.SetRateLimiterFactory(func() toolkit.Rate {
		return toolkit.NewRateLimiter(20000)
	})
	server.Listen(":7777")

	var cli1Replies int32 = 0
	var cli2Replies int32 = 0
	var cli3Replies int32 = 0
	var cli4Replies int32 = 0

	createClient("One", l, &cli1Replies)
	createClient("Two", l, &cli2Replies)
	createClient("Three", l, &cli3Replies)
	createClient("Four", l, &cli4Replies)

	var count = 0
	for {
		var wg sync.WaitGroup
		var now = time.Now()
		var replies int32 = 0
		const requests = 10000
		for i := 0; i < requests; i++ {
			wg.Add(1)
			go func(x int) {
				var err = <-server.RequestAll(TOPIC, fmt.Sprintf("req#%d", x), func(s string) {
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
				})
				if err != nil {
					fmt.Println("===> Unexpected error:", err)
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
		var delta = time.Now().Sub(now)
		if replies != requests*4 {
			fmt.Printf("Expected %d replies, got %d\n", requests*4, replies)
			time.Sleep(time.Second)
			fmt.Printf("cli1Replies %d\n", cli1Replies)
			fmt.Printf("cli2Replies %d\n", cli2Replies)
			fmt.Printf("cli3Replies %d\n", cli3Replies)
			fmt.Printf("cli4Replies %d\n", cli4Replies)
			os.Exit(1)
		}
		count++
		fmt.Printf("[%3d] 4x%d requests => %d replies in %v\n", count, requests, replies, delta)
		time.Sleep(time.Millisecond * 500)
	}
}
