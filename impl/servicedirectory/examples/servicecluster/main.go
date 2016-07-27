package main

import (
	"fmt"
	"time"

	"github.com/quintans/gomsg"
	"github.com/quintans/gomsg/impl/servicedirectory"
)

const (
	Topic1        = "TOPIC_1"
	Topic2        = "TOPIC_2"
)

var codec = gomsg.JsonCodec{}

func wait() {
	time.Sleep(time.Millisecond * 10)
}

func main() {

	var dir1 = servicedirectory.NewServiceDirectory(codec)
	var err = dir1.Listen(":7001")
	if err != nil {
		fmt.Println("E:", err)
		return
	}
	wait()

	var peer1 = servicedirectory.NewNode()
	err = peer1.Connect(":6001", "127.0.0.1:7001")
	if err != nil {
		fmt.Println("E:", err)
		return
	}
	wait()

	var peer2 = servicedirectory.NewNode()
	err = peer2.Connect(":6002", "127.0.0.1:7001")
	if err != nil {
		fmt.Println("E:", err)
		return
	}
	wait()

	//=============== PUB example =======
	var test = "test-test"
	var result string
	peer2.Handle(Topic1, func(m string) {
		fmt.Println("=====>[2]:", m)
		result = m
	})
	wait()

	fmt.Println("====== PUB example =======")
	peer1.Publish(Topic1, test)
	wait()
	if test != result {
		fmt.Println("E: ===> ERROR: expected", test, "but got", result)
	} else {
		fmt.Println("I: ===> PUB example OK")
	}

	// add node #3
	var peer3 = servicedirectory.NewNode()
	peer3.Handle(Topic1, func(m string) {
		fmt.Println("=====>[3]:", m)
	})
	err = peer3.Connect(":6003", "127.0.0.1:7001")
	if err != nil {
		fmt.Println("E:", err)
		return
	}
	wait()

	// add node #4
	var peer4 = servicedirectory.NewNode()
	peer4.Handle(Topic1, func(m string) {
		fmt.Println("=====>[4]:", m)
	})
	err = peer4.Connect(":6004", "127.0.0.1:7001")
	if err != nil {
		fmt.Println("E:", err)
		return
	}
	wait()

	// must observe rotation of the deliveries
	for i := 3; i < 7; i++ {
		peer1.Push(Topic1, fmt.Sprintf("test #%d", i))
		wait()
	}

	//=============== PUB SUB example =======
	fmt.Println("====== PUB SUB example =======")
	// must observe all deliveries
	peer1.Publish(Topic1, "teste")
	wait()

	//=============== REQ REP example =======
	peer2.Handle(Topic2, func() string {
		return "Two"
	})
	peer3.Handle(Topic2, func() string {
		return "Three"
	})
	peer4.Handle(Topic2, func() string {
		return "Four"
	})
	wait()

	fmt.Println("====== Request One =======")
	// must observe rotation of the replies
	for i := 0; i < 5; i++ {
		peer1.Request(Topic2, nil, func(s string) {
			fmt.Printf("=====>[%v] %s\n", i, s)
		})
		wait()
	}
	wait()

	fmt.Println("====== Request All example =======")
	// must observe all replying
	peer1.RequestAll(Topic2, nil, func(s string) {
		fmt.Println("=====>", s)
	}, time.Second)
	wait()

	time.Sleep(time.Second * 2) // are there any requests pending?

	fmt.Println("====== Drop Peer #2 =======")
	peer2.Destroy() // removed from the cluster

	time.Sleep(time.Second * 5) // provoques peer2 timeout purge
}
