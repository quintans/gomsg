package main

import (
	"fmt"
	"time"

	"github.com/quintans/gomsg"
	"github.com/quintans/toolkit/log"
)

func init() {
	log.Register("/", log.DEBUG).ShowCaller(true)
}

func wait() {
	time.Sleep(time.Millisecond * 10)
}

func main() {
	var logger = log.LoggerFor("/").SetCallerAt(2)
	server := gomsg.NewServer()
	server.SetDefaultTimeout(time.Second)
	server.SetLogger(log.Wrap{logger, "{server}"})
	server.Listen(":7777")
	wait()

	cli := gomsg.NewClient()
	cli.SetLogger(log.Wrap{logger, "{cli}"})
	cli.Handle("REVERSE", func(ctx *gomsg.Request, m string) (string, error) {
		fmt.Println("<=== processing (1):", m, "from", ctx.Connection().RemoteAddr())
		return reverse(m), nil
	})
	var err = <-cli.Connect("localhost:7777")
	if err != nil {
		fmt.Printf("===> ERROR: %+v\n", err)
	}
	wait()

	//time.Sleep(time.Millisecond * 100)
	fmt.Println("====> requesting...")
	var reply string
	err = <-server.Request("REVERSE", "hello", func(ctx gomsg.Response, r string) {
		reply = r
		fmt.Println("===> reply:", r)
	})
	if err != nil {
		fmt.Printf("===> ERROR: %+v\n", err)
	}
	if reply != "olleh" {
		fmt.Println("ERROR: Expected \"olleh\", got", reply)
	}

	wait()
	fmt.Println("I: close...")
	cli.Destroy()
	server.Destroy()
	wait()
}

func reverse(m string) string {
	r := []rune(m)
	for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}
