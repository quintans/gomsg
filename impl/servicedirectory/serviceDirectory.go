package impl

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
	PEERREADY = "PEERREADY"
	// ADDSERVICE is the topic used to inform the cluster of new service offering
	ADDSERVICE = "ADDSERVICE"
	// CANCELSERVICE is the topic used to inform the cluster of cancel a service offering
	CANCELSERVICE = "CANCELSERVICE"
	// DROPPEER is the topic to inform the peers of a peer disconnect
	DROPPEER = "DROPPEER"
)

const (
	// PING is the topic to inform the server that a peer is still alive
	PING = "PING"
	// PONG is the reply to a PING request
	PONG = "PONG"
)

// ServiceDirectory tracks all services providers.
type ServiceDirectory struct {
	mu        sync.RWMutex
	providers Providers
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
		providers:    make(Providers),
		PingInterval: time.Second,
		PingFailures: 2,
	}
	dir.server = gomsg.NewServer()
	dir.server.SetCodec(codec)

	dir.server.OnClose = func(c net.Conn) {
		// p will be called if a peer stops pinging
		fmt.Println("I: < Dir: peer", c.RemoteAddr(), "exited")

		dir.mu.Lock()
		defer dir.mu.Unlock()

		var endpoint = c.RemoteAddr().String()
		delete(dir.providers, endpoint)
		dir.server.Publish(DROPPEER, endpoint)
	}
	// returns a list of endpoints (addresses) by service
	dir.server.Handle(ADDSERVICE, func(r *gomsg.Request, service Service) {
		dir.mu.RLock()
		defer dir.mu.RUnlock()

		var c = r.Connection()
		service.Endpoint = fmt.Sprintf("%s:%s", c.RemoteAddr().(*net.TCPAddr).IP, service.Endpoint)

		fmt.Println("I: < Dir: ADDSERVICE service", service.Endpoint, "->", service.Name)
		dir.providers.AddService(service.Endpoint, service.Name)

		var w = dir.server.Get(c)
		// notifies all nodes, but caller
		dir.server.SendSkip(w.Wire(), gomsg.REQALL, ADDSERVICE, service, nil, time.Second)
	})
	dir.server.Handle(PEERREADY, func(r *gomsg.Request, providers Providers) Providers {
		var c = r.Connection()
		fmt.Println("< Dir: peer", c.RemoteAddr(), " PEER PEERREADY:", providers)

		dir.mu.Lock()
		defer dir.mu.Unlock()

		// Creates a list of all providers
		var copy = make(Providers)
		for k, v := range dir.providers {
			copy[k] = v
		}

		var endpoint string
		var list []string
		for k, v := range providers {
			endpoint = fmt.Sprintf("%s:%s", c.RemoteAddr().(*net.TCPAddr).IP, k)
			list = v
			// only has one entry
			break
		}

		// will be using the IP from where the connection came
		providers = make(map[string][]string)
		// update directory
		dir.providers[endpoint] = list
		providers[endpoint] = list

		// notifies all peers, but caller
		var w = dir.server.Get(c)
		dir.server.SendSkip(w.Wire(), gomsg.REQALL, PEERREADY, providers, nil, time.Second)

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
		fmt.Println("I: < killing connection from", c.RemoteAddr())
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

// Providers holds the available services for an endpoint
// in the form of [endpoint][]service
type Providers map[string][]string

// Contains verifies if the Provider has a service
func (p Providers) Contains(endpoint string, service string) bool {
	var services = p[endpoint]
	return contains(services, service)
}

func contains(services []string, service string) bool {
	if len(services) > 0 {
		for _, v := range services {
			if v == service {
				return true
			}
		}
	}

	return false
}

// AddService adds a service to the service list if it does not exists
func (p Providers) AddService(endpoint string, service string) {
	var services = p[endpoint]
	if !contains(services, service) {
		p[endpoint] = append(services, service)
	}
}

// RemoveService removes a service from the service list and returns if the endpoint was removed
func (p Providers) RemoveService(endpoint string, service string) bool {
	var services, ok = p[endpoint]
	if ok {
		var s []string
		for k, v := range services {
			if v == service {
				s = append(services[:k], services[k+1:]...)
				p[endpoint] = s
				break
			}
		}
		if len(s) == 0 {
			return true
		}
		return false
	}
	return true
}

// Service holds an endpoint service
type Service struct {
	Endpoint string
	Name     string
}

// Node is a element of the network service
type Node struct {
	wires *gomsg.Wires

	local      *gomsg.Server
	dirs       *gomsg.Wires
	remoteDirs []*gomsg.Client // exists only to hold the service server nodes
	mu         sync.RWMutex
	peers      map[string]*gomsg.Client
	muServers  sync.RWMutex
	providers  Providers       // list of all available service providers
	provides   map[string]bool // the services that this node provides
	// PingInterval is the interval to between peer pings
	PingInterval time.Duration
	// PingFailures is the number of times a ping can fail before the peer is considered offline
	PingFailures int
}

// NewNode creates a new Node
func NewNode() *Node {
	p := &Node{
		wires:        gomsg.NewWires(gomsg.JsonCodec{}),
		dirs:         gomsg.NewWires(gomsg.JsonCodec{}),
		remoteDirs:   make([]*gomsg.Client, 0),
		peers:        make(map[string]*gomsg.Client),
		PingInterval: time.Second,
		PingFailures: 2,
		provides:     make(map[string]bool),
		providers:    make(Providers),
	}
	p.local = gomsg.NewServer()
	// TODO make *Wires
	return p
}

// SetCodec sets the codec
func (node *Node) SetCodec(codec gomsg.Codec) *Node {
	node.wires.Codec = codec
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
		dir.Handle(PEERREADY, func(providers Providers) {
			fmt.Println("I: > node:", bindAddr, "PEERREADY notified of a new peer:", providers)

			node.mu.Lock()
			for k, v := range providers {
				node.providers[k] = v
			}
			node.mu.Unlock()
		})
		dir.Handle(ADDSERVICE, func(service Service) {
			fmt.Println("I: > node:", bindAddr, "ADDSERVICE notified of a new service:", service)

			node.mu.Lock()
			node.providers.AddService(service.Endpoint, service.Name)
			node.mu.Unlock()
		})
		dir.Handle(CANCELSERVICE, func(service Service) {
			fmt.Println("I: > node:", bindAddr, "CANCELSERVICE notified of canceled service:", service)

			node.mu.Lock()
			var empty = node.providers.RemoveService(service.Endpoint, service.Name)
			node.mu.Unlock()

			if empty {
				node.disconnectPeer(service.Endpoint)
			}
		})
		dir.Handle(DROPPEER, func(peerAddr string) {
			fmt.Println("I: > node:", bindAddr, "DROPPEER peer:", peerAddr)

			node.mu.Lock()
			delete(node.providers, peerAddr)
			node.mu.Unlock()

			node.disconnectPeer(peerAddr)
		})

		dir.OnConnect = func(w *gomsg.Wired) {
			node.dirs.Put(w.Conn(), w.Wire())

			node.mu.RLock()
			var services = make([]string, len(node.provides))
			var i = 0
			for k := range node.provides {
				services[i] = k
				i++
			}
			node.mu.RUnlock()
			var providers = make(Providers)
			providers[strconv.Itoa(node.local.BindPort())] = services

			dir.Request(PEERREADY, providers, func(ctx gomsg.Response, providers Providers) {
				node.mu.Lock()
				defer node.mu.Unlock()

				node.providers = providers
			})

			// this will ping the service directory,
			// and if there is no reply after some attempts it will disconnect
			go func(conn net.Conn) {
				var ticker = time.NewTicker(node.PingInterval)
				var retries = node.PingFailures
				for _ = range ticker.C {
					if dir.Active() {
						<-dir.RequestTimeout(PING, nil, func(ctx gomsg.Response, reply string) {
							if reply != PONG {
								if retries == 0 {
									ticker.Stop()
								}
								retries--
							} else {
								retries = node.PingFailures
							}
						}, time.Millisecond*200)
					} else {
						ticker.Stop()
					}
					dir.Reconnect()
				}
			}(w.Conn())

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
		fmt.Println("I: > new peer at", peerAddr)
		cli := gomsg.NewClient().SetCodec(node.wires.Codec)

		cli.OnConnect = func(w *gomsg.Wired) {
			node.wires.Put(w.Conn(), w.Wire())
		}
		cli.OnClose = func(c net.Conn) {
			node.wires.Kill(c)

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
	node.dirs.RequestAll(ADDSERVICE, Service{strconv.Itoa(node.local.BindPort()), name}, nil, time.Second)

	return node
}

// Cancel cancels handling of incoming messages for a topic
func (node *Node) Cancel(name string) {
	node.local.Cancel(name)
	// notify service directory cluster of cancel handle
	node.dirs.RequestAll(CANCELSERVICE, name, nil, time.Second)

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
	node.Destroy()

	node.remoteDirs = nil
	node.local = nil
	node.dirs = nil
	node.wires = nil
}

// lazyConnect connects to relevant unconnected peers
func (node *Node) lazyConnect(topic string) {
	node.mu.RLock()
	// find unconnected peers for this topic
	var unconnected = make([]string, 0)
	for endpoint, s := range node.providers {
		for _, service := range s {
			if service == topic {
				var cli = node.peers[endpoint]
				if cli == nil {
					unconnected = append(unconnected, endpoint)
				}
			}
		}
	}
	node.mu.RUnlock()

	for _, endpoint := range unconnected {
		node.connectPeer(endpoint)
	}
}

// Publish sends a message to all peer subscribed to this topic.
func (node *Node) Publish(topic string, payload interface{}) <-chan error {
	node.lazyConnect(topic)

	return node.wires.Publish(topic, payload)
}

// Push sends a message to one peer subscribed to this topic.
// It will perform a lazy connect
func (node *Node) Push(topic string, payload interface{}) <-chan error {
	node.lazyConnect(topic)

	return node.wires.Push(topic, payload)
}
