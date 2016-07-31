package main

import (
	"fmt"
	"time"

	"github.com/quintans/gomsg"
)

func main() {
	server := gomsg.NewServer()
	server.Listen(":7777")

	ungrouped := 0
	cli := gomsg.NewClient()
	cli.Handle("HELLO", func(m string) {
		fmt.Println("<=== [0] processing:", m)
		ungrouped++
	})
	<-cli.Connect("localhost:7777")

	group1 := 0
	// Group HA subscriber
	cli1 := gomsg.NewClient()
	cli1.SetGroupId("HA")
	cli1.Handle("HELLO", func(m string) {
		fmt.Println("<=== [1] processing:", m)
		group1++
	})
	<-cli1.Connect("localhost:7777")

	// Group HA subscriber
	group2 := 0
	cli2 := gomsg.NewClient()
	cli2.SetGroupId("HA")
	cli2.Handle("HELLO", func(m string) {
		fmt.Println("<=== [2] processing:", m)
		group2++
	})
	<-cli2.Connect("localhost:7777")


	// Only one element of the group HA will process each message, alternately (round robin).
	server.Publish("HELLO", "one")
	wait()
	server.Publish("HELLO", "two")
	wait()
	server.Publish("HELLO", "three")
	wait()
	server.Publish("HELLO", "four")
	wait()

	if ungrouped != 4 {
		fmt.Println("ERROR: RECEIVED", ungrouped, "UNGROUPED EVENTS. EXPECTED 4.")
	}
	if group1 != 2 {
		fmt.Println("ERROR: RECEIVED", group1, "GROUP EVENTS. EXPECTED 2.")
	}
	if group2 != 2 {
		fmt.Println("ERROR: RECEIVED", group2, "GROUP EVENTS. EXPECTED 2.")
	}
	wait()
	server.Destroy()
	wait()
}

func wait() {
	time.Sleep(time.Millisecond * 100)
}
