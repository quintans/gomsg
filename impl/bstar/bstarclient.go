// THIS IS A WORK IN PROGRESS

package bstar

import (
	"fmt"
	"time"

	"github.com/quintans/gomsg"
)

// heavely inspiried in http://zguide.zeromq.org/java:bstar

const (
	REQUEST_TIMEOUT = time.Second
	SETTLE_DELAY    = REQUEST_TIMEOUT * 2 //  Before failing over
)

type BStarClient struct {
	client    *gomsg.Client
	servers   []string
	serverNbr int
}

func NewBStarClient(primary string, backup string) BStarClient {
	this := BStarClient{
		client:  gomsg.NewClient().SetCodec(codec),
		servers: []string{primary, backup},
	}
	this.client.SetTimeout(time.Second)
	this.client.Connect(primary)

	return this
}

// Request sends a request to the Binary Star.
// If there's no reply within our timeout, we close the socket and try again.
// In Binary Star, it's the client vote that decides which
// server is primary; the client must therefore try to connect
// to each server in turn:
func (bsc BStarClient) Request(name string, payload interface{}, handler interface{}, timeout time.Duration) {
	expectReply := true
	// A request is used so that the bstar server can decide not to reply
	// if there is an inconsistent server state
	ch := bsc.client.RequestTimeout(name, payload, handler, timeout)
	for expectReply {
		err := <-ch
		if err == nil {
			expectReply = false
		} else {
			fmt.Println("W: no response from server, failing over. error:", err)
			//  client is confused; close it and open a new one
			bsc.client.Destroy()
			bsc.serverNbr = (bsc.serverNbr + 1) % 2
			<-time.After(SETTLE_DELAY)
			fmt.Printf("I: connecting to server at %s...\n", bsc.servers[bsc.serverNbr])
			bsc.client = gomsg.NewClient().SetCodec(codec)
			bsc.client.Connect(bsc.servers[bsc.serverNbr])

			//  Send request again, on new client
			ch = bsc.client.RequestTimeout(name, payload, handler, timeout)
		}
	}
}
