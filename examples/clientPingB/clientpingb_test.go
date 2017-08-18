package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/quintans/gomsg"
	"github.com/quintans/toolkit"
)

const (
	PING = "PING"
)

func wait() {
	time.Sleep(time.Millisecond * 10)
}

func TestPingBytes(t *testing.T) {
	var uuid = toolkit.NewUUID().Bytes()

	var server = gomsg.NewServer()
	server.Handle(PING, func(u []byte) bool {
		return bytes.Compare(uuid, u) == 0
	})
	server.Listen(":7777")
	wait()

	var cli1 = gomsg.NewClient()
	var reply = false
	err := <-cli1.Connect("localhost:7777")
	if err != nil {
		t.Fatal("Failed to connect.", err)
	}
	err = <-cli1.Request(PING, uuid, func(ok bool) {
		reply = ok
	})
	if err != nil {
		t.Fatal("Failed to req	uest.", err)
	}
	if !reply {
		t.Fatal("Expected TRUE got FALSE")
	}
}
