package test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	. "github.com/quintans/gomsg"
)

const (
	SERVER_PORT_1 = ":7777"
	SERVER_1      = "localhost" + SERVER_PORT_1
	SERVER_PORT_2 = ":7778"
	SERVER_2      = "localhost" + SERVER_PORT_2

	FORMAT_ERROR = "Expected '%v', got '%v'."

	MESSAGE  = "hello"
	PRODUCER = "PRODUCER"
	CONSUMER = "CONSUMER"
)

func wait() {
	time.Sleep(time.Millisecond * 100)
}

func waitASecond() {
	time.Sleep(time.Second)
}

func TestPubSubOneToOne(t *testing.T) {
	wait() // give time to the previous test to shutdown

	var request string
	server := NewServer()
	server.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< handling client pub:", m)
		request = m
	})
	server.Listen(SERVER_PORT_1)
	defer server.Destroy()

	cli := NewClient()
	defer cli.Destroy()
	<-cli.Connect(SERVER_1)
	// give time to connect
	wait()

	e := <-cli.Publish(CONSUMER, MESSAGE)
	// publish does wait to see if the message was delivered,
	// so we have wait if we want to check the delivery
	wait()

	if e != nil {
		t.Fatalf("Error: %s", e)
	}
	if request != MESSAGE {
		t.Fatalf(FORMAT_ERROR, MESSAGE, request)
	}

	request = "" // reset
	// this late client handler must propagate to the server
	cli.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< handling server pub:", m)
		request = m
	})
	// give time to inform the server of this new topic
	wait()

	server.Publish(CONSUMER, MESSAGE)
	wait()

	if request != MESSAGE {
		t.Fatalf(FORMAT_ERROR, MESSAGE, request)
	}
}

func TestPubSub(t *testing.T) {
	wait() // give time to the previous test to shutdown

	server := NewServer()
	server.Listen(SERVER_PORT_1)
	defer server.Destroy()

	var received = 0
	// client #1
	cli1 := NewClient()
	defer cli1.Destroy()
	cli1.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< client #1: received", m)
		if m == MESSAGE {
			received++
		}
	})
	<-cli1.Connect(SERVER_1)

	// client #2
	cli2 := NewClient()
	defer cli2.Destroy()
	cli2.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< client #2: received", m)
		if m == MESSAGE {
			received++
		}
	})
	<-cli2.Connect(SERVER_1)

	// give time to connect
	wait()

	// all clients will receive the message
	e := <-server.Publish(CONSUMER, MESSAGE)
	if e != nil {
		t.Fatalf("Error: %s", e)
	}
	// publish does not wait to see if the message was delivered,
	// so we have wait if we want to check the delivery
	wait()
	if received != 2 {
		t.Fatalf(FORMAT_ERROR, 2, received)
	}
}

func TestPubSubHA(t *testing.T) {
	wait() // give time to the previous test to shutdown

	server := NewServer()
	server.Listen(SERVER_PORT_1)
	defer server.Destroy()

	var received = 0
	// client #1
	cli1 := NewClient()
	cli1.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< client #1: received", m)
		if m == MESSAGE {
			received++
		}
	})
	// make it belong to a group.
	cli1.SetGroupId("HA")
	defer cli1.Destroy()
	<-cli1.Connect(SERVER_1)

	// client #2
	cli2 := NewClient()
	cli2.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< client #2: received", m)
		if m == MESSAGE {
			received++
		}
	})
	// make it belong to a group.
	cli2.SetGroupId("HA")
	defer cli2.Destroy()
	<-cli2.Connect(SERVER_1)

	// client #3
	cli3 := NewClient()
	defer cli3.Destroy()
	cli3.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< client #3: received", m)
		if m == MESSAGE {
			received++
		}
	})
	<-cli3.Connect(SERVER_1)

	// give time to connect
	wait()

	// group clients will receive the message in turn
	e := <-server.Publish(CONSUMER, MESSAGE)
	if e != nil {
		t.Fatalf("Error: %s", e)
	}
	e = <-server.Publish(CONSUMER, MESSAGE)
	if e != nil {
		t.Fatalf("Error: %s", e)
	}

	wait()
	if received != 4 {
		t.Fatalf(FORMAT_ERROR, 4, received)
	}
}

func TestPushPull(t *testing.T) {
	wait() // give time to the previous test to shutdown

	server := NewServer()
	server.Listen(SERVER_PORT_1)
	defer server.Destroy()

	// client #1
	var received1 = 0
	cli1 := NewClient()
	defer cli1.Destroy()
	cli1.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< client #1: received", m)
		if m == MESSAGE {
			received1++
		}
	})
	<-cli1.Connect(SERVER_1)

	// client #2
	var received2 = 0
	cli2 := NewClient()
	defer cli2.Destroy()
	cli2.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< client #2: received", m)
		if m == MESSAGE {
			received2++
		}
	})
	<-cli2.Connect(SERVER_1)

	// give time to connect
	wait()

	// only one client will receive the message
	e := <-server.Push(CONSUMER, MESSAGE)
	if e != nil {
		t.Fatalf("Error: %s", e)
	}

	// a different client from before (round-robin)
	// will receive the message
	e = <-server.Push(CONSUMER, MESSAGE)
	if e != nil {
		t.Fatalf("Error: %s", e)
	}

	wait() // push delivery is asynchronous

	if received1 != 1 {
		t.Fatalf(FORMAT_ERROR, 1, received1)
	}

	if received2 != 1 {
		t.Fatalf(FORMAT_ERROR, 1, received2)
	}
}

func TestRequestReply(t *testing.T) {
	wait() // give time to the previous test to shutdown

	server := NewServer()
	server.Listen(SERVER_PORT_1)
	defer server.Destroy()

	cli1 := NewClient()
	defer cli1.Destroy()
	cli1.Handle(PRODUCER, func(ctx *Request, m string) (string, error) {
		fmt.Println("<<< client #1 handling", m, "from", ctx.Connection().RemoteAddr())
		return strings.ToUpper(m), nil
	})
	<-cli1.Connect(SERVER_1)

	cli2 := NewClient()
	defer cli2.Destroy()
	cli2.Handle(PRODUCER, func(ctx *Request, m string) (string, error) {
		fmt.Println("<<< client #2 handling", m, "from", ctx.Connection().RemoteAddr())
		return strings.ToUpper(m), nil
	})
	<-cli2.Connect(SERVER_1)

	// give time to connect
	wait()

	var expected = 0
	// using <- makes the call synchronous
	err := <-server.Request(PRODUCER, MESSAGE, func(ctx Response, r string) {
		fmt.Println(">>> reply:", r)
		if r == "HELLO" {
			expected++
		}
	})

	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	if expected != 1 {
		t.Fatalf(FORMAT_ERROR, 1, expected)
	}
}

func TestRequestAllReply(t *testing.T) {
	wait() // give time to the previous test to shutdown

	server := NewServer()
	server.Listen(SERVER_PORT_1)
	defer server.Destroy()
	wait()

	cli1 := NewClient()
	defer cli1.Destroy()
	cli1.Handle(PRODUCER, func(ctx *Request, m string) (string, error) {
		fmt.Println("<<< client #1 handling", m, "from", ctx.Connection().RemoteAddr())
		return strings.ToUpper(m), nil
	})
	<-cli1.Connect(SERVER_1)

	cli2 := NewClient()
	defer cli2.Destroy()
	cli2.Handle(PRODUCER, func(ctx *Request, m string) (string, error) {
		fmt.Println("<<< client #2 handling", m, "from", ctx.Connection().RemoteAddr())
		return strings.ToUpper(m), nil
	})
	<-cli2.Connect(SERVER_1)

	// give time to connect
	wait()

	var expected = 0
	var endMarker = 0
	// using <- makes the call synchronous
	err := <-server.RequestAll(PRODUCER, MESSAGE, func(ctx Response, r string) {
		fmt.Println(">>> reply:", r)
		if r == "HELLO" {
			expected++
		}
		if ctx.Last() {
			endMarker++
		}
	}, time.Second)

	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	if expected != 2 {
		t.Fatalf(FORMAT_ERROR, 2, expected)
	}
	if endMarker != 1 {
		t.Fatalf(FORMAT_ERROR, 1, endMarker)
	}
}

func TestRouteClients(t *testing.T) {
	wait() // give time to the previous test to shutdown

	server := NewServer()
	server.Listen(SERVER_PORT_1)
	defer server.Destroy()
	// all (*) messages arriving to the server are routed to the clients
	server.Route("*", time.Second, nil, nil)

	cli1 := NewClient()
	defer cli1.Destroy()
	cli1.Handle(PRODUCER, func(ctx *Request, m string) (string, error) {
		fmt.Println("<<< client #1 handling", m, "from", ctx.Connection().RemoteAddr())
		return strings.ToUpper(m), nil
	})
	<-cli1.Connect(SERVER_1)

	cli2 := NewClient()
	defer cli2.Destroy()
	cli2.Handle(PRODUCER, func(ctx *Request, m string) (string, error) {
		fmt.Println("<<< client #2 handling", m, "from", ctx.Connection().RemoteAddr())
		return strings.ToUpper(m), nil
	})
	<-cli2.Connect(SERVER_1)

	// give time to connect
	wait()

	cli3 := NewClient()
	<-cli3.Connect(SERVER_1)
	var expected = 0
	var endMarker = 0
	// using <- makes the call synchronous
	err := <-cli3.RequestAll(PRODUCER, MESSAGE, func(ctx Response, r string) {
		fmt.Println(">>> reply:", r)
		if r == "HELLO" {
			expected++
		}
		// we know all was delivered by asking if it was the last one
		if ctx.Last() {
			endMarker++
		}
	})

	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	if expected != 2 {
		t.Fatalf(FORMAT_ERROR, 2, expected)
	}
	if endMarker != 1 {
		t.Fatalf(FORMAT_ERROR, 1, endMarker)
	}
}

func TestRouteClientsHA(t *testing.T) {
	wait() // give time to the previous test to shutdown

	server := NewServer()
	defer server.Destroy()
	server.Listen(SERVER_PORT_1)
	// all messages arriving to the server are routed to the clients
	server.Route("*", time.Second, nil, nil)

	ungrouped := 0
	cli := NewClient()
	defer cli.Destroy()
	cli.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< client #0 handling:", m)
		if m != "" {
			ungrouped++
		}
	})
	<-cli.Connect(SERVER_1)

	group1 := 0
	// Group HA subscriber
	cli1 := NewClient()
	defer cli1.Destroy()
	cli1.SetGroupId("HA")
	cli1.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< client #1 handling:", m)
		if m != "" {
			group1++
		}
	})
	<-cli1.Connect(SERVER_1)

	// Group HA subscriber
	group2 := 0
	cli2 := NewClient()
	defer cli2.Destroy()
	cli2.SetGroupId("HA")
	cli2.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< client #2 handling:", m)
		if m != "" {
			group2++
		}
	})
	<-cli2.Connect(SERVER_1)

	// publisher
	cli3 := NewClient()
	defer cli3.Destroy()
	<-cli3.Connect(SERVER_1)

	wait()

	// Only one element of the group HA will process each message, alternately (round robin).
	cli3.Publish(CONSUMER, "one")
	cli3.Publish(CONSUMER, "two")
	cli3.Publish(CONSUMER, "three")
	cli3.Publish(CONSUMER, "four")

	wait()
	if ungrouped != 4 {
		t.Fatalf(FORMAT_ERROR, 4, ungrouped)
	}
	if group1 != 2 {
		t.Fatalf(FORMAT_ERROR, 2, group1)
	}
	if group2 != 2 {
		t.Fatalf(FORMAT_ERROR, 2, group2)
	}
}

func TestRequestAllReplyHA(t *testing.T) {
	wait() // give time to the previous test to shutdown

	server := NewServer()
	server.Listen(SERVER_PORT_1)
	defer server.Destroy()

	// client #1
	cli1 := NewClient()
	defer cli1.Destroy()
	cli1.Handle(PRODUCER, func(ctx *Request, m string) (string, error) {
		fmt.Println("<<< client #1 handling", m, "from", ctx.Connection().RemoteAddr())
		return strings.ToUpper(m), nil
	})
	<-cli1.Connect(SERVER_1)

	// client #2
	cli2 := NewClient()
	defer cli2.Destroy()
	cli2.Handle(PRODUCER, func(ctx *Request, m string) (string, error) {
		fmt.Println("<<< client #2 handling", m, "from", ctx.Connection().RemoteAddr())
		return strings.ToUpper(m), nil
	})
	cli2.SetGroupId("HA")
	<-cli2.Connect(SERVER_1)

	// client #3
	cli3 := NewClient()
	defer cli3.Destroy()
	cli3.Handle(PRODUCER, func(ctx *Request, m string) (string, error) {
		fmt.Println("<<< client #3 handling", m, "from", ctx.Connection().RemoteAddr())
		return strings.ToUpper(m), nil
	})
	cli3.SetGroupId("HA")
	<-cli3.Connect(SERVER_1)

	// give time to connect
	wait()

	var expected = 0
	// using <- makes the call synchronous
	err := <-server.RequestAll(PRODUCER, MESSAGE, func(ctx Response, r string) {
		fmt.Println(">>> reply:", r)
		if r == "HELLO" {
			expected++
		}
	}, time.Second)

	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	if expected != 2 {
		t.Fatalf(FORMAT_ERROR, 2, expected)
	}
}

func TestLateSubscription(t *testing.T) {
	wait() // give time to the previous test to shutdown

	server := NewServer()
	server.Listen(SERVER_PORT_1)
	defer server.Destroy()

	cli := NewClient()
	defer cli.Destroy()
	<-cli.Connect(SERVER_1)

	// time to connect
	wait()

	// this late server handler must propagate to the client
	serverReceived := 0
	server.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< server received:", m)
		if m == MESSAGE {
			serverReceived++
		}
	})
	wait()

	err := <-cli.Publish(CONSUMER, MESSAGE)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	wait() // publish delivery is asynchronous
	if serverReceived != 1 {
		t.Fatalf(FORMAT_ERROR, 1, serverReceived)
	}

	// this late client handler must propagate to the server
	clientReceived := 0
	cli.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< client received:", m)
		if m == MESSAGE {
			clientReceived++
		}
	})
	wait()

	err = <-server.Publish(CONSUMER, MESSAGE)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	wait() // publish delivery is asynchronous
	if clientReceived != 1 {
		t.Fatalf(FORMAT_ERROR, 1, clientReceived)
	}
}

func TestLateServer(t *testing.T) {
	wait() // give time to the previous test to shutdown

	cli := NewClient()
	defer cli.Destroy()
	cli.Connect(SERVER_1)
	wait()

	// THE SERVER SHOWS UP AFTER THE CLIENT
	server := NewServer()
	defer server.Destroy()
	received := 0
	server.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< server handling:", m)
		if m == MESSAGE {
			received++
		}
	})
	server.Listen(SERVER_PORT_1)
	waitASecond() // enough time to client reconnection

	err := <-cli.Publish(CONSUMER, MESSAGE)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	wait() // publish delivery is asynchronous

	if received != 1 {
		t.Fatalf(FORMAT_ERROR, 1, received)
	}
}

func TestMultiReply(t *testing.T) {
	wait() // give time to the previous test to shutdown

	// all messages arriving to the server are routed to the clients
	server := NewServer()
	defer server.Destroy()
	server.Listen(SERVER_PORT_1)
	server.Route("*", time.Second, nil, nil)

	cli := NewClient()
	defer cli.Destroy()
	// sends multi reply
	cli.Handle(PRODUCER, func(ctx *Request) {
		// json format so that the client decodes it
		ctx.SendReply([]byte("\"Pipe #1\""))
		ctx.SendReply([]byte("\"Pipe #2\""))
		ctx.Terminate()
	})
	<-cli.Connect(SERVER_1)

	cli3 := NewClient()
	defer cli3.Destroy()
	<-cli3.Connect(SERVER_1)

	wait()

	var received = 0
	var endMarker = 0
	err := <-cli3.Request(PRODUCER, nil, func(ctx Response, s string) {
		m := string(ctx.Reply())
		fmt.Printf(">>> reply: raw=%v, decoded=%v\n", m, s)
		if ctx.Last() {
			endMarker++
		} else if strings.HasPrefix(s, "Pipe #") && strings.HasPrefix(m, "\"Pipe #") {
			received++
		}
	})
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	wait()
	if received != 2 {
		t.Fatalf(FORMAT_ERROR, 2, received)
	}
	if endMarker != 1 {
		t.Fatalf(FORMAT_ERROR, 1, endMarker)
	}
}

func TestMultiReplyMix(t *testing.T) {
	wait() // give time to the previous test to shutdown

	// all messages arriving to the server are routed to the clients
	server := NewServer()
	defer server.Destroy()
	server.Listen(SERVER_PORT_1)
	server.Route("*", time.Second, nil, nil)

	cli := NewClient()
	defer cli.Destroy()
	// sends multi reply
	cli.Handle(PRODUCER, func(ctx *Request) {
		// json format so that the client decodes it
		ctx.SendReply([]byte("\"Pipe #1\""))
		ctx.SendReply([]byte("\"Pipe #2\""))
		ctx.Terminate()
	})
	<-cli.Connect(SERVER_1)

	cli2 := NewClient()
	defer cli2.Destroy()
	cli2.Handle(PRODUCER, func(ctx *Request) string {
		return "Pipe #201"
	})
	<-cli2.Connect(SERVER_1)

	cli3 := NewClient()
	defer cli3.Destroy()
	<-cli3.Connect(SERVER_1)

	wait()

	var received = 0
	err := <-cli3.RequestAll(PRODUCER, nil, func(m string) {
		fmt.Println(">>> reply:", m)
		if strings.HasPrefix(m, "Pipe #") {
			received++
		}
	})
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	wait()
	if received != 3 {
		t.Fatalf(FORMAT_ERROR, 3, received)
	}

}

func TestRouteServers(t *testing.T) {
	wait() // give time to the previous test to shutdown

	// routing requests between server 1 and server 2
	server1 := NewServer()
	server1.Listen(SERVER_PORT_1)
	defer server1.Destroy()
	server2 := NewServer()
	server2.Listen(SERVER_PORT_2)
	defer server2.Destroy()
	// all (*) messages arriving to server 1 are routed to server 2
	Route("*", server1, server2, time.Second, nil, nil)

	// client 1 connects to server 1
	cli := NewClient()
	defer cli.Destroy()
	<-cli.Connect(SERVER_1)
	cli2 := NewClient()
	defer cli2.Destroy()
	var received = 0
	cli2.Handle(PRODUCER, func(ctx *Request, m string) (string, error) {
		fmt.Println("<<< processing:", m, "from", ctx.Connection().RemoteAddr())
		return strings.ToUpper(m), nil
	})
	// client 2 connects to server 2
	<-cli2.Connect(SERVER_2)

	err := <-cli.Request(PRODUCER, MESSAGE, func(ctx Response, r string, e error) {
		fmt.Println(">>> reply:", r, "; error:", e, "from", ctx.Connection().RemoteAddr())
		if r == strings.ToUpper(MESSAGE) {
			received++
		}
	})
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	wait()
	if received != 1 {
		t.Fatalf(FORMAT_ERROR, 1, received)
	}
}
