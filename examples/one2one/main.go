package main

import (
	"fmt"
	"time"

	"github.com/quintans/gomsg"
	"github.com/quintans/toolkit/log"
)

func init() {
	gomsg.SetLogger(log.LoggerFor("github.com/quintans/gmsg"))
}

func main() {
	server := gomsg.NewServer()
	server.Handle("HELLO", func(ctx *gomsg.Request, m string) (string, error) {
		fmt.Println("<=== processing:", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("Hello %s", m), nil
	})
	server.Handle("XPTO", func(m string) {
		fmt.Println("<=== handling pull:", m)
	})
	server.Listen(":7777")

	cli := gomsg.NewClient()
	cli.Handle("REVERSE", func(ctx *gomsg.Request, m string) (string, error) {
		fmt.Println("<=== processing (1):", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("[1]=%s", reverse(m)), nil
	})
	cli.Handle("SUB", func(m string) {
		fmt.Println("<=== handling pub-sub:", m)
	})
	cli.Connect("localhost:7777")

	cli2 := gomsg.NewClient()
	cli2.Handle("REVERSE", func(ctx *gomsg.Request, m string) (string, error) {
		fmt.Println("<=== processing (2):", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("[2]=%s", reverse(m)), nil
	})
	cli2.Handle("SUB", func(m string) {
		fmt.Println("<=== handling pub-sub:", m)
	})
	cli2.Connect("localhost:7777")

	//var err error
	/*
		messages := []string{"Hello", "World", "Paulo"}
		for _, m := range messages {
			err = <-cli.Request("HELLO", m, func(ctx gomsg.IResponse, r string) {
				fmt.Println("===> reply:", r, "from", ctx.Connection().RemoteAddr())
			})
			if err != nil {
				fmt.Println("===> error:", err)
			}
		}
	*/

	time.Sleep(time.Millisecond * 100)
	// Warning: when requesting many in a server, the last response (end mark) will have a null connection
	server.RequestAll("REVERSE", "hello", func(ctx gomsg.Response, r string) {
		fmt.Println("===> reply:", r)
	}, time.Second)
	/*
		server.Request("REVERSE", "hello", func(ctx gomsg.IResponse, r string) {
			fmt.Println("===> reply:", r, "from", ctx.Connection().RemoteAddr())
		})
		server.Request("REVERSE", "hello", func(ctx gomsg.IResponse, r string) {
			fmt.Println("===> reply:", r, "from", ctx.Connection().RemoteAddr())
		})
		server.Request("REVERSE", "hello", func(ctx gomsg.IResponse, r string) {
			fmt.Println("===> reply:", r, "from", ctx.Connection().RemoteAddr())
		})
	*/

	//server.Publish("SUB", "PUB: La")
	/*
		err = <-cli.Publish("XPTO", "PUB: La")
		err = <-cli.Publish("XPTO", "PUB: Vida")
		err = <-cli.Publish("XPTO", "PUB: Loca")
	*/

	//cli.Push("XPTO", "One")

	/*
		err = <-cli.Push("XPTO", "PUSH: Two")
		err = <-cli.Push("XPTO", "PUSH: Three")
		err = <-cli.Push("XPTO", "PUSH: Four")
		time.Sleep(time.Millisecond * 100)

		err = <-cli.Push("FAIL", 123)
		fmt.Println("I: error:", err)
		if err == nil {
			fmt.Println("E: Should have returned an error")
		}
	*/

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
