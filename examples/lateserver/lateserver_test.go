package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/quintans/gomsg"
)

func TestLateServer(t *testing.T) {
	cli := gomsg.NewClient()
	cli.Connect("localhost:7777")
	time.Sleep(time.Millisecond * 100)

	var result string
	// THE SERVER SHOWS UP AFTER THE CLIENT
	server := gomsg.NewServer()
	server.Handle("XPTO", func(m string) {
		result = m
		fmt.Println("<=== handling pull:", m)
	})
	server.Listen(":7777")
	time.Sleep(time.Second)

	e := <-cli.Publish("XPTO", "teste")
	if e != nil {
		fmt.Println("===> error:", e)
	}

	/*
		cli.Handle("SUB", func(m string) {
			fmt.Println("<=== handling pub-sub:", m)
		})
		time.Sleep(time.Millisecond * 100)

		<-server.Publish("SUB", "teste")
	*/
	time.Sleep(time.Second)

	if result != "teste" {
		t.Fatal("Expected 'teste', got", result)
	}

}
