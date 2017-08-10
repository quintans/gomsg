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

const (
	SERVER_PORT_1 = ":7777"
	SERVER_1      = "localhost" + SERVER_PORT_1
	SERVER_PORT_2 = ":7778"
	SERVER_2      = "localhost" + SERVER_PORT_2

	MESSAGE  = "hello"
	PRODUCER = "PRODUCER"
	CONSUMER = "CONSUMER"
)

func wait() {
	time.Sleep(time.Millisecond * 100)
}

func TestPushPull(t *testing.T) {
	server := gomsg.NewServer()
	server.Listen(SERVER_PORT_1)
	defer server.Destroy()

	// client #1
	var received1 = 0
	cli1 := gomsg.NewClient()
	cli1.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< client #1: received", m)
		if m == MESSAGE {
			received1++
		}
	})
	cli1.Connect(SERVER_1)
	defer cli1.Destroy()

	// client #2
	var received2 = 0
	cli2 := gomsg.NewClient()
	cli2.Handle(CONSUMER, func(m string) {
		fmt.Println("<<< client #2: received", m)
		if m == MESSAGE {
			received2++
		}
	})
	cli2.Connect(SERVER_1)
	defer cli2.Destroy()

	// give time to connect
	wait()

	// only one client will receive the message
	e := <-server.Push(CONSUMER, MESSAGE)
	if e != nil {
		fmt.Printf("Error: %s\n", e)
	}

	// a different client from before (round-robin)
	// will receive the message
	e = <-server.Push(CONSUMER, MESSAGE)
	if e != nil {
		t.Fatalf("Error: %s\n", e)
	}

	if received1 != 1 {
		t.Fatalf("Expected '%v', got '%v'\n", 1, received1)
	}

	if received2 != 1 {
		t.Fatalf("Expected '%v', got '%v'\n", 1, received2)
	}
}
