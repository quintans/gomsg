package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/quintans/gomsg"
)

func TestStickyness(t *testing.T) {
	server := gomsg.NewServer()
	server.SetDefaultTimeout(time.Second)
	server.Listen(":7777")

	cli := gomsg.NewClient()
	cli2 := gomsg.NewClient()

	fmt.Println("=========== STICKY PUSH =============")

	var cliNum = 0
	cli.Handle("PUSHING", func(ctx *gomsg.Request, m string) {
		cliNum = 1
		fmt.Println("<=== processing PUSHING (1):", m, "from", ctx.Connection().RemoteAddr())
	})
	cli.Handle("NOISE", func(ctx *gomsg.Request, m string) {
		cliNum = 21
		fmt.Println("<=== processing NOISE (21):", m, "from", ctx.Connection().RemoteAddr())
	})

	cli2.Handle("PUSHING", func(ctx *gomsg.Request, m string) {
		cliNum = 2
		fmt.Println("<=== processing PUSHING (2):", m, "from", ctx.Connection().RemoteAddr())
	})
	cli2.Handle("NOISE", func(ctx *gomsg.Request, m string) {
		cliNum = 22
		fmt.Println("<=== processing NOISE (22):", m, "from", ctx.Connection().RemoteAddr())
	})

	<-cli.Connect("localhost:7777")
	<-cli2.Connect("localhost:7777")

	// note the FILTER_TOKEN at the end. This way we can define a range of Stickyness
	var lb = gomsg.NewSimpleLB()
	lb.Stick("PUSH"+gomsg.FILTER_TOKEN, time.Millisecond*500)
	server.SetLoadBalancer(lb)

	<-server.Push("PUSHING", "ping") // load#1 = 1
	wait()                           // give time for the client to be executed
	if cliNum != 1 {
		t.Fatal("Expected 1, got", cliNum)
	}

	<-server.Push("PUSHING", "ping") // load#1 = 2
	wait()                           // give time for the client to be executed
	if cliNum != 1 {
		t.Fatal("Expected 1, got", cliNum)
	}

	<-server.Push("NOISE", "---noise---") // load#2 = 1
	wait()                                // give time for the client to be executed
	if cliNum != 22 {
		t.Fatal("Expected 22, got", cliNum)
	}

	<-server.Push("PUSHING", "ping") // load#1 = 3
	wait()                           // give time for the client to be executed
	if cliNum != 1 {
		t.Fatal("Expected 1, got", cliNum)
	}

	<-server.Push("NOISE", "---noise---") // load#2 = 2
	wait()                                // give time for the client to be executed
	if cliNum != 22 {
		t.Fatal("Expected 22, got", cliNum)
	}

	<-server.Push("PUSHING", "ping") // load#1 = 4
	wait()                           // give time for the client to be executed
	if cliNum != 1 {
		t.Fatal("Expected 1, got", cliNum)
	}
	time.Sleep(time.Second)
	<-server.Push("PUSHING", "change wire") // load#2 = 3
	wait()                                  // give time for the client to be executed
	if cliNum != 2 {
		t.Fatal("Expected 2, got", cliNum)
	}

	fmt.Println("=========== STICKY REQUEST =============")

	cli.Handle("REVERSE", func(ctx *gomsg.Request, m string) (string, error) {
		cliNum = 1
		fmt.Println("<=== processing REVERSE (1):", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("[1]=%s", reverse(m)), nil
	})
	cli2.Handle("REVERSE", func(ctx *gomsg.Request, m string) (string, error) {
		cliNum = 2
		fmt.Println("<=== processing REVERSE (2):", m, "from", ctx.Connection().RemoteAddr())
		return fmt.Sprintf("[2]=%s", reverse(m)), nil
	})
	wait()

	lb.Stick("REVERSE", time.Millisecond*500)
	// Warning: when requesting many in a server, the last response (end mark) will have a null connection
	var resultFn = func(ctx gomsg.Response, r string) {
		fmt.Println("===> reply:", r, ", last?", ctx.Last())
	}
	<-server.Request("REVERSE", "hello", resultFn) // load#2 = 4
	wait()                                         // give time for the client to be executed
	if cliNum != 2 {
		t.Fatal("Expected 2, got", cliNum)
	}
	<-server.Request("REVERSE", "world", resultFn) // load#2 = 5
	wait()                                         // give time for the client to be executed
	if cliNum != 2 {
		t.Fatal("Expected 2, got", cliNum)
	}
	time.Sleep(time.Second)
	<-server.Request("REVERSE", "change wire", resultFn) // load#1 = 5
	wait()                                               // give time for the client to be executed
	if cliNum != 1 {
		t.Fatal("Expected 1, got", cliNum)
	}

	time.Sleep(time.Second * 2)

	fmt.Println("I: close...")
	cli.Destroy()
	time.Sleep(time.Second)
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
