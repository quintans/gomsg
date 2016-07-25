// THIS IS A WORK IN PROGRESS

package impl

import (
	"fmt"
	"sync"
	"time"

	"github.com/quintans/gomsg"
)

// heavely inspiried in http://zguide.zeromq.org/java:bstar

//  State we can have at any point in time
type State uint8

const (
	STATE_PRIMARY State = iota + 1 //  Primary, waiting for peer to connect
	STATE_BACKUP                   //  Backup, waiting for peer to connect
	STATE_ACTIVE                   //  Active - accepting connections
	STATE_PASSIVE                  //  Passive - not accepting connections
)

//  Events, which start with the states our peer can be in
type Event uint8

const (
	PEER_PRIMARY   Event = iota + 1 //  HA peer is pending primary
	PEER_BACKUP                     //  HA peer is pending backup
	PEER_ACTIVE                     //  HA peer is active
	PEER_PASSIVE                    //  HA peer is passive
	CLIENT_REQUEST                  //  Client makes request
)

var codec = gomsg.JsonCodec{}

type BStar struct {
	mu              sync.RWMutex
	quit            chan bool
	state           State //  Current state
	event           Event //  Current event
	statepub        *gomsg.Client
	statesub        *gomsg.Server
	frontend        *gomsg.Server
	peerExpiry      time.Time //  When peer is considered 'dead'
	frontendHandler func(bstar *BStar, ctx *gomsg.Request)

	stateRemoteAddr string
	stateLocalAddr  string
	frontentAddr    string
}

//  We send state information this often
//  If peer doesn't respond in two heartbeats, it is 'dead'
const HEARTBEAT = time.Second

//  The heart of the Binary Star design is its finite-state machine (FSM).
//  The FSM runs one event at a time. We apply an event to the current state,
//  which checks if the event is accepted, and if so, sets a new state:

func (this *BStar) stateMachine(event Event) bool {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.event = event
	exception := false

	//  These are the PRIMARY and BACKUP states; we're waiting to become
	//  ACTIVE or PASSIVE depending on events we get from our peer:
	if this.state == STATE_PRIMARY {
		if this.event == PEER_BACKUP {
			fmt.Println("I: connected to backup (passive), ready active")
			this.state = STATE_ACTIVE
		} else if this.event == PEER_ACTIVE {
			fmt.Println("I: connected to backup (active), ready passive")
			this.state = STATE_PASSIVE
		}
	} else if this.state == STATE_BACKUP {
		//  Accept client connections
		if this.event == PEER_ACTIVE {
			fmt.Println("I: connected to primary (active), ready passive")
			this.state = STATE_PASSIVE
		} else if this.event == CLIENT_REQUEST {
			//  Reject client connections when acting as backup
			exception = true
		}
	} else if this.state == STATE_ACTIVE {
		//  These are the ACTIVE and PASSIVE states:
		if this.event == PEER_ACTIVE {
			//  Two actives would mean split-brain
			fmt.Println("E: fatal error - dual actives, aborting")
			exception = true
		}
	} else if this.state == STATE_PASSIVE {
		//  Server is passive
		//  CLIENT_REQUEST events can trigger failover if peer looks dead

		if this.event == PEER_PRIMARY {
			//  Peer is restarting - become active, peer will go passive
			fmt.Println("I: primary (passive) is restarting, ready active")
			this.state = STATE_ACTIVE
		} else if this.event == PEER_BACKUP {
			//  Peer is restarting - become active, peer will go passive
			fmt.Println("I: backup (passive) is restarting, ready active")
			this.state = STATE_ACTIVE
		} else if this.event == PEER_PASSIVE {
			//  Two passives would mean cluster would be non-responsive
			fmt.Println("E: fatal error - dual passives, aborting")
			exception = true
		} else if this.event == CLIENT_REQUEST {
			//  Peer becomes active if timeout has passed
			//  It's the client request that triggers the failover
			if time.Now().After(this.peerExpiry) {
				//  If peer is dead, switch to the active state
				fmt.Println("I: failover successful, ready active")
				this.state = STATE_ACTIVE
			} else {
				//  If peer is alive, reject connections
				exception = true
			}
		}
	}
	return exception
}

//  This is our main task. First we bind/connect our sockets with our
//  peer and make sure we will get state messages correctly. We use
//  three sockets; one to publish state, one to subscribe to state, and
//  one for client requests/replies:
func NewBStar(primary bool, frontentAddr string, stateLocalAddr string, stateRemoteAddr string) *BStar {
	this := &BStar{}
	if primary {
		fmt.Println("I: Primary active, waiting for backup (passive)")
		this.state = STATE_PRIMARY
	} else {
		fmt.Println("I: Backup passive, waiting for primary (active)")
		this.state = STATE_BACKUP
	}

	this.quit = make(chan bool, 1)

	this.statepub = gomsg.NewClient().SetCodec(codec)

	this.statesub = gomsg.NewServer()
	this.statesub.SetCodec(codec)
	this.statesub.Handle("STATE", func(ctx *gomsg.Request) {
		state, _ := ctx.Reader().ReadUI8()
		//  Have state from our peer, execute as event
		if this.stateMachine(Event(state)) {
			this.quit <- true //  Error, so exit
		} else {
			this.updatePeerExpiry()
		}

	})

	this.frontend = gomsg.NewServer()
	this.frontend.SetCodec(codec)
	this.frontend.Handle("*", func(ctx *gomsg.Request) {
		//  Have a client request
		ok := !this.stateMachine(CLIENT_REQUEST)
		if ok {
			this.frontendHandler(this, ctx)
		} else {
			// rejects request
			ctx.Terminate()
		}
	})

	this.stateRemoteAddr = stateRemoteAddr
	this.stateLocalAddr = stateLocalAddr
	this.frontentAddr = frontentAddr

	return this
}

// SetClientHandler defines the function that will handle the clients requests
func (bstar *BStar) SetClientHandler(handler func(bstart *BStar, ctx *gomsg.Request)) {
	bstar.frontendHandler = handler
}

func (this *BStar) updatePeerExpiry() {
	this.mu.Lock()
	this.peerExpiry = time.Now().Add(2 * HEARTBEAT)
	this.mu.Unlock()
}

func (this *BStar) Start() {
	this.updatePeerExpiry()

	this.statepub.Connect(this.stateRemoteAddr)
	this.statesub.Listen(this.stateLocalAddr)
	this.frontend.Listen(this.frontentAddr)

	for {
		select {
		case <-this.quit:
			break //  Context has been shut down
		case <-time.After(HEARTBEAT):
			//  If we timed out, send state to peer
			msg := gomsg.NewMsg()
			msg.WriteUI8(uint8(this.state))
			this.statepub.Publish("STATE", msg)
		}
	}

	this.statepub.Destroy()
	this.statesub.Destroy()
	this.frontend.Destroy()
}

func (bstar *BStar) Stop() {
	bstar.quit <- true
}

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
