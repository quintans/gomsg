package main

import (
	"fmt"
	"time"

	"github.com/quintans/gomsg"
)

func main() {
	server := gomsg.NewServer()
	server.Listen(":7777")

	cli := gomsg.NewClient()
	cli.Handle("REVERSE", func(ctx *gomsg.Request, m string) (string, error) {
		fmt.Println("<=== processing (1):", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("[1]=%s", reverse(m)), nil
	})
	cli.Connect("localhost:7777")

	cli2 := gomsg.NewClient()
	cli2.Handle("REVERSE", func(ctx *gomsg.Request, m string) (string, error) {
		fmt.Println("<=== processing (2):", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("[2]=%s", reverse(m)), nil
	})
	cli2.Connect("localhost:7777")

	// just to get in the way
	cli3 := gomsg.NewClient()
	cli3.Connect("localhost:7777")

	// ===============

	//time.Sleep(time.Millisecond * 100)
	/*
		<-server.Request("REVERSE", "hello", func(ctx gomsg.IResponse, r string) {
			fmt.Println("===> reply:", r, ctx.Kind(), "from", ctx.Connection().RemoteAddr())
		})
	*/
	server.RequestAll("REVERSE", "hello", func(ctx *gomsg.Response, r string) {
		fmt.Println("===> reply:", ctx.Kind, r)
	}, time.Second)

	time.Sleep(time.Millisecond * 100)
	fmt.Println("I: close...")
	cli.Destroy()
	time.Sleep(time.Millisecond * 100)
}

func reverse(m string) string {
	r := []rune(m)
	for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}
