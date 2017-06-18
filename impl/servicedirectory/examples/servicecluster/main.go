package main

import (
	"fmt"
	"os"
	"time"

	"github.com/quintans/gomsg/impl/servicedirectory"
	"github.com/quintans/toolkit/log"
)

const (
	Topic1 = "TOPIC_1"
	Topic2 = "TOPIC_2"
)

func wait() {
	time.Sleep(time.Millisecond * 200)
}

func init() {
	log.Register("/", log.DEBUG).ShowCaller(true)
}

func main() {
	var dir1 = servicedirectory.NewDirectory("DIR#1")
	var err = dir1.Listen(":7001")
	if err != nil {
		fmt.Println("E:", err)
		os.Exit(1)
	}
	/*
		var dir2 = servicedirectory.NewDirectory("DIR#2")
		err = dir2.Listen(":7002")
		if err != nil {
			fmt.Println("E:", err)
			os.Exit(1)
		}
		wait()
	*/

	var dirs = []string{"127.0.0.1:7001"}

	fmt.Println("==== Adding peer #1 ====")
	var peer1 = servicedirectory.NewPeer("Peer#1")
	peer1.SetIdleTimeout(time.Second * 3)
	err = peer1.Connect(":6001", dirs...)
	if err != nil {
		fmt.Println("E:", err)
		os.Exit(1)
	}
	wait()

	fmt.Println("==== Adding peer #2 ====")
	var peer2 = servicedirectory.NewPeer("Peer#2")
	err = peer2.Connect(":6002", dirs...)
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
		os.Exit(1)
	}

	// add node #3
	fmt.Println("==== Adding peer #3 ====")
	var peer3 = servicedirectory.NewPeer("Peer#3")
	peer3.Handle(Topic1, func(m string) {
		fmt.Println("=====>[3]:", m)
	})
	err = peer3.Connect(":6003", dirs...)
	if err != nil {
		fmt.Println("E:", err)
		return
	}
	wait()

	// add node #4
	fmt.Println("==== Adding peer #4 ====")
	var peer4 = servicedirectory.NewPeer("Peer#4")
	peer4.Handle(Topic1, func(m string) {
		fmt.Println("=====>[4]:", m)
	})
	err = peer4.Connect(":6004", dirs...)
	if err != nil {
		fmt.Println("E:", err)
		return
	}
	wait()

	// must observe rotation of the deliveries
	fmt.Println("==== must observe rotation of the deliveries ====")
	for i := 3; i < 7; i++ {
		<-peer1.Push(Topic1, fmt.Sprintf("test #%d", i))
	}

	//=============== PUB SUB example =======
	fmt.Println("====== BEGIN PUB SUB example =======")
	// must observe all deliveries
	<-peer1.Publish(Topic1, "teste")
	fmt.Println("====== END of PUB SUB example =======")

	//=============== REQ REP example =======
	fmt.Println("====== BEGIN REQ REP example =======")
	<-peer2.Handle(Topic2, func() string {
		return "Two"
	})
	<-peer3.Handle(Topic2, func() string {
		return "Three"
	})
	<-peer4.Handle(Topic2, func() string {
		return "Four"
	})

	var size = len(peer1.Endpoints(Topic1))
	if size != 3 {
		fmt.Println("E: ===> ERROR: expected 3 endpoint for peer1(Topic1), got", size)
		os.Exit(1)
	}
	size = len(peer2.Endpoints(Topic1))
	if size != 2 {
		fmt.Println("E: ===> ERROR: expected 2 endpoint for peer2(Topic1), got", size)
		os.Exit(1)
	}
	size = len(peer3.Endpoints(Topic1))
	if size != 2 {
		fmt.Println("E: ===> ERROR: expected 2 endpoint for peer3(Topic1), got", size)
		os.Exit(1)
	}
	size = len(peer4.Endpoints(Topic1))
	if size != 2 {
		fmt.Println("E: ===> ERROR: expected 2 endpoint for peer4(Topic1), got", size)
		os.Exit(1)
	}
	size = len(peer1.Endpoints(Topic2))
	if size != 3 {
		fmt.Println("E: ===> ERROR: expected 3 endpoint for peer1(Topic2), got", size)
		os.Exit(1)
	}
	size = len(peer2.Endpoints(Topic2))
	if size != 2 {
		fmt.Println("E: ===> ERROR: expected 2 endpoint for peer2(Topic2), got", size)
		os.Exit(1)
	}
	size = len(peer3.Endpoints(Topic2))
	if size != 2 {
		fmt.Println("E: ===> ERROR: expected 2 endpoint for peer3(Topic2), got", size)
		os.Exit(1)
	}
	size = len(peer4.Endpoints(Topic2))
	if size != 2 {
		fmt.Println("E: ===> ERROR: expected 2 endpoint for peer4(Topic2), got", size)
		os.Exit(1)
	}

	fmt.Println("====== Request One =======")
	// must observe rotation of the replies
	for i := 0; i < 5; i++ {
		<-peer1.Request(Topic2, nil, func(s string) {
			fmt.Printf("=====>[%v] %s\n", i, s)
		})
	}

	fmt.Println("====== Request All example =======")
	// must observe all replying
	<-peer1.RequestAll(Topic2, nil, func(s string) {
		var str = s
		if s == "" {
			str = "[END]"
		}
		fmt.Println("=====>", str)
	}, time.Second)

	//fmt.Println("====== Drop DIR #2 =======")
	//dir2.Destroy() // removed from the cluster

	fmt.Println("====== Drop Peer #2 =======")
	peer2.Destroy() // removed from the cluster

	fmt.Println("Waiting for timeouts...")
	time.Sleep(time.Second * 3) // provoques peer2 timeout purge
}
