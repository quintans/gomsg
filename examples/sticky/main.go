package main

import (
	"fmt"
	"time"

	"github.com/quintans/gomsg"
)

func main() {
	server := gomsg.NewServer()
	server.SetTimeout(time.Second)
	server.Listen(":7777")

	cli := gomsg.NewClient()
	cli2 := gomsg.NewClient()

	fmt.Println("=========== STICKY PUSH =============")

	cli.Handle("PUSHING", func(ctx *gomsg.Request, m string) {
		fmt.Println("<=== processing PUSHING (1):", m, "from", ctx.Connection().RemoteAddr())
	})
	cli.Handle("NOISE", func(ctx *gomsg.Request, m string) {
		fmt.Println("<=== processing NOISE (21):", m, "from", ctx.Connection().RemoteAddr())
	})

	cli2.Handle("PUSHING", func(ctx *gomsg.Request, m string) {
		fmt.Println("<=== processing PUSHING (2):", m, "from", ctx.Connection().RemoteAddr())
	})
	cli2.Handle("NOISE", func(ctx *gomsg.Request, m string) {
		fmt.Println("<=== processing NOISE (22):", m, "from", ctx.Connection().RemoteAddr())
	})

	cli.Connect("localhost:7777")
	cli2.Connect("localhost:7777")

	wait()

	// note the FILTER_TOKEN at the end. This way we can define a range of Stickyness
	server.Stick("PUSH"+gomsg.FILTER_TOKEN, time.Millisecond*500)

	server.Push("PUSHING", "ping")
	wait()
	server.Push("PUSHING", "ping")
	wait()

	server.Push("NOISE", "---noise--- will provoque the rotation of the general cursor")
	wait()
	server.Push("PUSHING", "ping")
	wait()
	server.Push("NOISE", "---noise--- will provoque the rotation of the general cursor")
	wait()
	server.Push("PUSHING", "ping")
	time.Sleep(time.Second)
	server.Push("PUSHING", "change wire")
	wait()

	fmt.Println("=========== STICKY REQUEST =============")

	cli.Handle("REVERSE", func(ctx *gomsg.Request, m string) (string, error) {
		fmt.Println("<=== processing REVERSE (1):", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("[1]=%s", reverse(m)), nil
	})
	cli2.Handle("REVERSE", func(ctx *gomsg.Request, m string) (string, error) {
		fmt.Println("<=== processing REVERSE (2):", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("[2]=%s", reverse(m)), nil
	})
	wait()

	server.Stick("REVERSE", time.Millisecond*500)
	// Warning: when requesting many in a server, the last response (end mark) will have a null connection
	var resultFn = func(ctx gomsg.Response, r string) {
		fmt.Println("===> reply:", r, ", last?", ctx.Last())
	}
	server.Request("REVERSE", "hello", resultFn)
	wait()
	server.Request("REVERSE", "world", resultFn)
	wait()
	server.Request("REVERSE", "ola", resultFn)
	wait()
	server.Request("REVERSE", "mundo", resultFn)
	time.Sleep(time.Second)
	server.Request("REVERSE", "change wire", resultFn)
	wait()

	time.Sleep(time.Second * 2)

	fmt.Println("I: close...")
	cli.Destroy()
	time.Sleep(time.Second * 5)
}

func wait() {
	time.Sleep(time.Millisecond * 10)
}

func reverse(m string) string {
	r := []rune(m)
	for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}
