package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/quintans/gomsg"
	"github.com/quintans/toolkit/log"
)

func init() {
	gomsg.SetLogger(log.LoggerFor("github.com/quintans/gmsg"))
}

func wait() {
	time.Sleep(time.Millisecond * 20)
}

func TestRequestAll(t *testing.T) {
	server := gomsg.NewServer()
	server.SetTimeout(time.Second)
	server.Listen(":7777")
	wait()

	var clicount = 0
	var srvcount = 0
	cli := gomsg.NewClient()
	cli.Handle("REVERSE", func(ctx *gomsg.Request, m string) (string, error) {
		clicount++
		fmt.Println("<=== processing (1):", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("[1]=%s", reverse(m)), nil
	})
	<-cli.Connect("localhost:7777")

	cli2 := gomsg.NewClient()
	cli2.Handle("REVERSE", func(ctx *gomsg.Request, m string) (string, error) {
		clicount++
		fmt.Println("<=== processing (2):", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("[2]=%s", reverse(m)), nil
	})
	<-cli2.Connect("localhost:7777")

	wait()
	// Warning: when requesting many in a server, the last response (end mark) will have a null connection
	server.RequestAll("REVERSE", "hello", func(ctx gomsg.Response, r string) {
		srvcount++
		fmt.Println("===> reply:", r, ", last?", ctx.Last())
	}, time.Second)

	wait()

	if clicount != 2 {
		t.Fatal("Expected 2 client replies, got", clicount)
	}

	if srvcount != 3 {
		t.Fatal("Expected 2 server hits, got", srvcount)
	}

	fmt.Println("I: close...")
	cli.Destroy()
	wait()
}

func reverse(m string) string {
	r := []rune(m)
	for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}
