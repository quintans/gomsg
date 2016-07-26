package main

import (
	"fmt"
	"time"

	"github.com/quintans/gomsg"
	"github.com/quintans/gomsg/impl/servicedirectory"
)

const (
	peerTimeout   = time.Second * 10
	serverTimeout = 2 * peerTimeout
	TOPIC_1       = "TOPIC_1"
	TOPIC_2       = "TOPIC_2"
)

var codec = gomsg.JsonCodec{}

func wait() {
	time.Sleep(time.Millisecond * 10)
}

func main() {

	var dir1 = impl.NewServiceDirectory(codec)
	var err = dir1.Listen(":7001")
	if err != nil {
		fmt.Println("E:", err)
		return
	}
	wait()

	var peer1 = impl.NewNode()
	err = peer1.Connect(":6001", "127.0.0.1:7001")
	if err != nil {
		fmt.Println("E:", err)
		return
	}
	wait()

	var peer2 = impl.NewNode()
	err = peer2.Connect(":6002", "127.0.0.1:7001")
	if err != nil {
		fmt.Println("E:", err)
		return
	}
	wait()
	/*
		//=============== PUB example =======
		var test = "test-test"
		var result string
		peer2.Handle(TOPIC_1, func(m string) {
			fmt.Println("=====>[2]:", m)
			result = m
		})
		wait()

		fmt.Println("====== PUB example =======")
		peer1.Publish(TOPIC_1, fmt.Sprint(test))
		time.Sleep(time.Millisecond * 100)
		if test != result {
			fmt.Println("E: ===> ERROR: expected", test, "but got", result)
		} else {
			fmt.Println("I: ===> PUB example OK")
		}

		// add node #3
		var peer3 = impl.NewNode()
		peer3.Handle(TOPIC_1, func(m string) {
			fmt.Println("=====>[3]:", m)
		})
		err = peer3.Connect(":6003", "127.0.0.1:7001")
		if err != nil {
			fmt.Println("E:", err)
			return
		}
		wait()

		// add node #4
		var peer4 = impl.NewNode()
		peer4.Handle(TOPIC_1, func(m string) {
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
			peer1.Push(TOPIC_1, fmt.Sprintf("test #%d", i))
			wait()
		}

		//=============== PUB SUB example =======
		fmt.Println("====== PUB SUB example =======")
		// must observe all deliveries
		peer1.Publish(TOPIC_1, "teste")
		wait()

		//=============== REQ REP example =======
		peer2.Handle(TOPIC_2, func() string {
			return "Two"
		})
		peer3.Handle(TOPIC_2, func() string {
			return "Three"
		})
		peer4.Handle(TOPIC_2, func() string {
			return "Four"
		})
		wait()

		fmt.Println("====== Request One =======")
		// must observe rotation of the replies
		for i := 0; i < 5; i++ {
			peer1.Request(TOPIC_2, nil, func(s string) {
				fmt.Println("=====>", s)
				wait()
			})
		}
		wait()

		fmt.Println("====== Request All example =======")
		// must observe all replying
		peer1.RequestAll(TOPIC_2, nil, func(s string) {
			fmt.Println("=====>", s)
		}, time.Second)
		wait()
	*/

	time.Sleep(time.Second * 5) // are there any requests pending?
	/*
		fmt.Println("====== Drop Peer #2 =======")
		peer2.Destroy() // removed from the cluster
		wait()

		/*
			fmt.Println("====== Drop Peer #1 =======")
			peer1.Destroy() // removed from the cluster
			wait()
	*/

	//time.Sleep(time.Second)
}
