// THIS IS A WORK IN PROGRESS
//
// This file demonstrates the use o gomsg to build a netwok of services.
// We have peer nodes and directory nodes.
// Peer are nodes that provide and/or consume services.
// Data communication is made directly between peer nodes.
// Directory nodes (one or more) is where the peers register the services that they provide.
// (This network could operate with just one directory node. If this node disapears the network still functions)
// Everytime a peer changes (add/remove) its provided services it informs the directory
// and this in turn notifies all other perrs of this change.

package servicedirectory

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/quintans/gomsg"
)

const (
	// PEERREADY is the topic to inform that the peer is ready,
	// to pass its services and to ask for the already  available services
	C_PEERREADY = "C_PEERREADY" // server -> client
	S_PEERREADY = "S_PEERREADY" // client -> server
	// ADDSERVICE is the topic used to inform the cluster of new service offering
	C_ADDSERVICE = "C_ADDSERVICE"
	S_ADDSERVICE = "S_ADDSERVICE"
	// CANCELSERVICE is the topic used to inform the cluster of cancel a service offering
	C_CANCELSERVICE = "C_CANCELSERVICE"
	S_CANCELSERVICE = "S_CANCELSERVICE"
	// DROPPEER is the topic to inform the peers of a peer disconnect
	C_DROPPEER = "C_DROPPEER"
	S_DROPPEER = "S_DROPPEER"
)

const (
	// PING is the topic to inform the server that a peer is still alive
	PING = "PING"
	// PONG is the reply to a PING request
	PONG = "PONG"
)

// ServiceDirectory tracks all services providers.
type ServiceDirectory struct {
	mu sync.RWMutex
	// for a directory connection the respective peer is providing services in a specific Port
	providers map[net.Conn]*Provider
	server    *gomsg.Server
	// PingInterval is the interval to between peer pings
	PingInterval time.Duration
	// PingFailures is the number of times a ping can fail before the peer is considered offline
	PingFailures int
}

// NewServiceDirectory creates a peer ServiceDirectory where all the clients connect
// to know about service providers
func NewServiceDirectory(codec gomsg.Codec) *ServiceDirectory {
	dir := &ServiceDirectory{
		providers:    make(map[net.Conn]*Provider),
		PingInterval: time.Second,
		PingFailures: 2,
	}
	dir.server = gomsg.NewServer()
	dir.server.SetCodec(codec)
	dir.server.SetTimeout(time.Second * 5)

	dir.server.OnClose = func(c net.Conn) {

		dir.mu.Lock()
		defer dir.mu.Unlock()

		var provider = dir.providers[c]
		if provider != nil {
			fmt.Println("I: < Dir: peer", provider.Endpoint, "exited")
			delete(dir.providers, c)
			dir.server.Publish(C_DROPPEER, provider.Endpoint)
		} else {
			fmt.Println("I: < Dir: closing connection", c.RemoteAddr(), "for inexistente provider")
		}
	}
	// returns a list of endpoints (addresses) by service
	dir.server.Handle(S_ADDSERVICE, func(r *gomsg.Request, service string) error {
		var c = r.Connection()
		dir.mu.Lock()
		var provider = dir.providers[c]
		if provider != nil {
			fmt.Println("I: < Dir: S_ADDSERVICE service", provider.Endpoint, "->", service)
			provider.AddService(service)
		}
		dir.mu.Unlock()

		if provider != nil {
			var w = dir.server.Get(c)
			// notifies all nodes, but caller
			dir.server.SendSkip(w.Wire(), gomsg.REQALL, C_ADDSERVICE, Service{provider.Endpoint, service}, nil, time.Second)
		}

		return nil
	})

	dir.server.Handle(S_CANCELSERVICE, func(r *gomsg.Request, service string) error {
		var c = r.Connection()
		dir.mu.Lock()
		var provider = dir.providers[c]
		if provider != nil {
			fmt.Println("I: < Dir: S_CANCELSERVICE service", provider.Endpoint, "->", service)
			provider.RemoveService(service)
		}
		dir.mu.Unlock()

		if provider != nil {
			var w = dir.server.Get(c)
			// notifies all nodes, but caller
			dir.server.SendSkip(w.Wire(), gomsg.REQALL, C_CANCELSERVICE, Service{provider.Endpoint, service}, nil, time.Second)
		}

		return nil
	})

	dir.server.Handle(S_PEERREADY, func(r *gomsg.Request, provider Provider) []*Provider {
		var c = r.Connection()
		fmt.Println("< Dir: peer", c.RemoteAddr(), " S_PEERREADY:", provider)

		// will be using the IP from where the connection came
		provider.Endpoint = fmt.Sprintf("%s:%s", c.RemoteAddr().(*net.TCPAddr).IP, provider.Endpoint)

		dir.mu.Lock()
		defer dir.mu.Unlock()

		// Creates a shallow copy of all existing providers
		var copy = make([]*Provider, len(dir.providers))
		var i = 0
		for _, v := range dir.providers {
			copy[i] = v
			i++
		}

		// update directory
		dir.providers[c] = &provider

		// notifies all peers, but the caller
		var w = dir.server.Get(c)
		dir.server.SendSkip(w.Wire(), gomsg.REQALL, C_PEERREADY, provider, nil, time.Second)

		return copy
	})

	return dir
}

// Listen starts ServiceDirectory listening for incoming connections
func (dir *ServiceDirectory) Listen(addr string) error {
	var err = dir.server.Listen(addr)
	if err != nil {
		return err
	}

	// keep alive
	var timeout = gomsg.NewTimeout(dir.PingInterval, dir.PingInterval*time.Duration(dir.PingFailures), func(o interface{}) {
		c := o.(net.Conn)

		fmt.Println("I: < Dir: Ping timeout. Purging/killing connection from", c.RemoteAddr())
		dir.server.Kill(c)
	})
	dir.server.OnConnect = func(w *gomsg.Wired) {
		// starts monitoring
		timeout.Delay(w.Conn())
	}
	// reply to pings and delays the timeout
	dir.server.Handle(PING, func(r *gomsg.Request) string {
		timeout.Delay(r.Connection())

		return PONG
	})

	return nil
}

// Provider holds the available services for an endpoint
type Provider struct {
	Endpoint string
	Services []string
}

func NewProvider(endpoint string) *Provider {
	return &Provider{endpoint, make([]string, 0)}
}

// Contains verifies if the Provider has a service
func (p *Provider) Find(service string) int {
	if len(p.Services) > 0 {
		for k, v := range p.Services {
			if v == service {
				return k
			}
		}
	}

	return -1
}

// AddService adds a service to the service list if it does not exists
func (p *Provider) AddService(service string) {
	if p.Find(service) == -1 {
		p.Services = append(p.Services, service)
	}
}

// RemoveService removes a service from the service list and returns if the endpoint has no services
func (p *Provider) RemoveService(service string) bool {
	if k := p.Find(service); k > -1 {
		p.Services = append(p.Services[:k], p.Services[k+1:]...)
		return len(p.Services) == 0
	}
	return false
}

// Service holds an endpoint service
type Service struct {
	Endpoint string
	Name     string
}

// Node is a element of the network service
type Node struct {
	*gomsg.Wires

	local      *gomsg.Server
	dirs       *gomsg.Wires
	remoteDirs []*gomsg.Client // exists only to hold the service server nodes
	mu         sync.RWMutex
	peers      map[string]*gomsg.Client
	muServers  sync.RWMutex
	providers  map[string]*Provider // list of all available service providers, by endpoint
	provides   map[string]bool      // the services that this node provides
	// PingInterval is the interval to between peer pings
	PingInterval time.Duration
	// PingFailures is the number of times a ping can fail before the peer is considered offline
	PingFailures int
}

// NewNode creates a new Node
func NewNode() *Node {
	node := &Node{
		Wires:        gomsg.NewWires(gomsg.JsonCodec{}),
		dirs:         gomsg.NewWires(gomsg.JsonCodec{}),
		remoteDirs:   make([]*gomsg.Client, 0),
		peers:        make(map[string]*gomsg.Client),
		PingInterval: time.Second,
		PingFailures: 2,
		provides:     make(map[string]bool),
		providers:    make(map[string]*Provider),
	}
	node.local = gomsg.NewServer()
	node.AddSendListener(0, func(event gomsg.SendEvent) {
		node.lazyConnect(event.Name)
	})
	return node
}

// SetCodec sets the codec
func (node *Node) SetCodec(codec gomsg.Codec) *Node {
	node.Codec = codec
	node.dirs.Codec = codec
	return node
}

// Connect binds a to a local address to provide services
// and connect to the directory remove addresses
func (node *Node) Connect(bindAddr string, dirAddrs ...string) error {
	var err = node.local.Listen(bindAddr)
	if err != nil {
		return err
	}

	node.remoteDirs = make([]*gomsg.Client, len(dirAddrs))

	for k, dirAddr := range dirAddrs {
		var dir = gomsg.NewClient()
		node.remoteDirs[k] = dir
		dir.Handle(C_PEERREADY, func(provider Provider) {
			fmt.Println("I: > node:", bindAddr, "C_PEERREADY notified of a new peer:", provider)

			node.mu.Lock()
			node.providers[provider.Endpoint] = &provider
			node.mu.Unlock()
		})
		dir.Handle(C_ADDSERVICE, func(service Service) {
			fmt.Println("I: > node:", bindAddr, "C_ADDSERVICE notified of a new service:", service)

			node.mu.Lock()
			for _, v := range node.providers {
				if v.Endpoint == service.Endpoint {
					v.AddService(service.Name)
					break
				}
			}
			node.mu.Unlock()
		})
		dir.Handle(C_CANCELSERVICE, func(service Service) {
			fmt.Println("I: > node:", bindAddr, "C_CANCELSERVICE notified of canceled service:", service)

			node.mu.Lock()
			var empty = false
			for _, v := range node.providers {
				if v.Endpoint == service.Endpoint {
					empty = v.RemoveService(service.Name)
					break
				}
			}
			node.mu.Unlock()

			if empty {
				node.disconnectPeer(service.Endpoint)
			}
		})
		dir.Handle(C_DROPPEER, func(endpoint string) {
			fmt.Println("I: > node:", bindAddr, "C_DROPPEER peer:", endpoint)

			node.mu.Lock()
			delete(node.providers, endpoint)
			node.mu.Unlock()

			node.disconnectPeer(endpoint)
		})

		dir.OnConnect = func(w *gomsg.Wired) {

			// this will ping the service directory,
			// and if there is no reply after some attempts it will reconnect
			go func() {
				var ticker = time.NewTicker(node.PingInterval)
				var retries = node.PingFailures
				for _ = range ticker.C {
					if dir.Active() {
						var pong = ""
						var err = <-dir.RequestTimeout(PING, nil, func(ctx gomsg.Response, reply string) {
							pong = reply
						}, time.Millisecond*10)

						if _, ok := err.(gomsg.TimeoutError); ok || pong != PONG {
							retries--
							fmt.Println("D: > PING failed. retries left:", retries)
							if retries == 0 {
								ticker.Stop()
							}
						} else {
							retries = node.PingFailures
						}
					} else {
						ticker.Stop()
					}
				}
				dir.Reconnect()
			}()

			node.dirs.Put(w.Conn(), w.Wire())

			// only want for the first established connection to get the list of providers
			if node.dirs.Size() > 1 {
				return
			}

			node.mu.RLock()
			var services = make([]string, len(node.provides))
			var i = 0
			for k := range node.provides {
				services[i] = k
				i++
			}
			node.mu.RUnlock()

			var provider = NewProvider(strconv.Itoa(node.local.BindPort()))
			provider.Services = services

			dir.Request(S_PEERREADY, provider, func(ctx gomsg.Response, providers []Provider) {
				node.mu.Lock()
				defer node.mu.Unlock()

				for _, v := range providers {
					node.providers[v.Endpoint] = &v
				}
			})
		}

		dir.OnClose = func(c net.Conn) {
			node.dirs.Kill(c)
			for k, v := range node.remoteDirs {
				if v.Connection() == c {
					node.remoteDirs = append(node.remoteDirs[:k], node.remoteDirs[k+1:]...)
				}
			}
		}

		dir.Connect(dirAddr)
	}

	return nil
}

func (node *Node) connectPeer(peerAddr string) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.peers[peerAddr] == nil {
		cli := gomsg.NewClient().SetCodec(node.Codec)

		cli.OnConnect = func(w *gomsg.Wired) {
			node.Put(w.Conn(), w.Wire())
		}
		cli.OnClose = func(c net.Conn) {
			node.Kill(c)

			node.mu.Lock()
			defer node.mu.Unlock()
			// remove from connected peers
			delete(node.peers, peerAddr)
		}

		// doesn't try to reconnect
		cli.SetReconnectInterval(0)

		// wait for the connection to establish
		var err = <-cli.Connect(peerAddr)
		if err != nil {
			return err
		}
		node.peers[peerAddr] = cli
	}
	return nil
}

func (node *Node) disconnectPeer(peerAddr string) {
	node.mu.RLock()
	var cli = node.peers[peerAddr]
	node.mu.RUnlock()

	if cli != nil {
		fmt.Println("I: > disconnecting peer:", peerAddr)

		// will eventually call OnClose
		// where it will be removed from peers list
		cli.Destroy()
	}
}

// Handle handles incoming messages for a topic
func (node *Node) Handle(name string, fun interface{}) *Node {
	node.local.Handle(name, fun)

	node.mu.Lock()
	node.provides[name] = true
	node.mu.Unlock()

	// notify service directory cluster of new handle
	node.dirs.RequestAll(S_ADDSERVICE, name, nil, time.Second)

	return node
}

// Cancel cancels handling of incoming messages for a topic
func (node *Node) Cancel(name string) {
	node.local.Cancel(name)
	// notify service directory cluster of cancel handle
	node.dirs.RequestAll(S_CANCELSERVICE, name, nil, time.Second)

	node.mu.Lock()
	delete(node.provides, name)
	node.mu.Unlock()
}

// Destroy kills this node, releasing resources
func (node *Node) Destroy() {
	if node.local != nil {
		node.local.Destroy()
	}
	if node.remoteDirs != nil {
		for _, v := range node.remoteDirs {
			v.Destroy()
		}
	}

	node.dirs.Destroy()
	node.Wires.Destroy()

	node.remoteDirs = nil
	node.local = nil
	node.dirs = nil
	node.Wires = nil
}

// lazyConnect connects to relevant unconnected peers
func (node *Node) lazyConnect(topic string) {
	node.mu.RLock()
	// find unconnected peers for this topic
	var unconnected = make([]string, 0)
	for _, provider := range node.providers {
		if provider.Find(topic) != -1 {
			var cli = node.peers[provider.Endpoint]
			if cli == nil {
				fmt.Println("D: > found unconnect endpoint", provider.Endpoint, "for", topic)
				unconnected = append(unconnected, provider.Endpoint)
			}
		}
	}
	node.mu.RUnlock()

	for _, endpoint := range unconnected {
		node.connectPeer(endpoint)
	}
}
