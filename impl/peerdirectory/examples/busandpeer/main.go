package main

import (
	"fmt"
	"time"

	"github.com/quintans/gomsg"
	"github.com/quintans/gomsg/impl/peerdirectory"
)

const (
	peerTimeout   = time.Second * 10
	serverTimeout = 2 * peerTimeout
	PUSH_PULL     = "PUSH_PULL"
	REQ_REP       = "REQ_REP"
)

var codec = gomsg.JsonCodec{}

func main() {

	peerdirectory.NewDirectory("127.0.0.1:7777", codec)
	time.Sleep(time.Millisecond * 100)

	peer1 := impl.NewPeer("127.0.0.1:7777", ":6001", codec)
	peer2 := impl.NewPeer("127.0.0.1:7777", ":6002", codec)
	peer3 := impl.NewPeer("127.0.0.1:7777", ":6003", codec)
	peer4 := impl.NewPeer("127.0.0.1:7777", ":6004", codec)
	time.Sleep(time.Millisecond * 100)

	//=============== PULL PULL example =======
	peer2.Handle(PUSH_PULL, func(m string) {
		fmt.Println("=====>[2]:", m)
	})
	peer3.Handle(PUSH_PULL, func(m string) {
		fmt.Println("=====>[3]:", m)
	})
	peer4.Handle(PUSH_PULL, func(m string) {
		fmt.Println("=====>[4]:", m)
	})
	time.Sleep(time.Millisecond * 100)

	fmt.Println("====== PULL PULL example =======")
	peer1.Push(PUSH_PULL, fmt.Sprint("test-test"))
	// must observe rotation of the replies
	for i := 3; i < 7; i++ {
		peer1.Push(PUSH_PULL, fmt.Sprintf("test #%d", i))
	}

	//=============== PUB SUB example =======
	fmt.Println("====== PUB SUB example =======")
	// must observe all replying
	peer1.Publish(PUSH_PULL, "teste")
	time.Sleep(time.Millisecond * 100)

	//=============== REQ REP example =======
	peer2.Handle(REQ_REP, func() string {
		return "Two"
	})
	peer3.Handle(REQ_REP, func() string {
		return "Three"
	})
	peer4.Handle(REQ_REP, func() string {
		return "Four"
	})
	time.Sleep(time.Millisecond * 100)

	fmt.Println("====== Request One =======")
	// must observe rotation of the replies
	for i := 0; i < 5; i++ {
		peer1.Request(REQ_REP, nil, func(s string) {
			fmt.Println("=====>", s)
		})
	}

	time.Sleep(time.Millisecond * 100)

	fmt.Println("====== Request All example =======")
	// must observe all replying
	peer1.RequestAll(REQ_REP, nil, func(s string) {
		fmt.Println("=====>", s)
	})
	time.Sleep(time.Millisecond * 100)

	peer4.Destroy() // removed from the cluster

	time.Sleep(time.Second)
}
