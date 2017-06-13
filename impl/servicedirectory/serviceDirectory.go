// THIS IS A WORK IN PROGRESS
//
// This code demonstrates the use o gomsg to build a netwok of services.
// We have directory nodes and peer nodes.
// Directory nodes (one or more) is where the peers register the services that they provide.
// Peer nodes provide and/or consume services.
// All peers are connected to the directory node(s). Peer nodes inform the directory nodes and are informed by
// by them when a service(s) change.
// Service communication is done directly between peer nodes.
// A peer connects to a service provider (peer) lazily.
// (This network could operate with just one directory node. If this node disapears the network still functions)
// Everytime a peer changes (add/remove) its provided services it informs the directory
// and this in turn notifies all other peers of this change.

package servicedirectory

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/quintans/gomsg"
	"github.com/quintans/toolkit/log"
)

const (
	// PEERREADY is the topic to inform that the peer is ready,
	// to pass its services and to ask for the already  available services
	C_PEERREADY = "C_PEERREADY"   // server -> client
	S_PEERREADY = "DIR/PEERREADY" // client -> server
	// ADDSERVICE is the topic used to inform the cluster of new service offering
	C_ADDSERVICE = "C_ADDSERVICE"
	S_ADDSERVICE = "DIR/ADDSERVICE"
	// CANCELSERVICE is the topic used to inform the cluster of cancel a service offering
	C_CANCELSERVICE = "C_CANCELSERVICE"
	S_CANCELSERVICE = "DIR/CANCELSERVICE"
	// DROPPEER is the topic to inform the peers of a peer disconnect
	C_DROPPEER = "C_DROPPEER"
	S_DROPPEER = "DIR/DROPPEER"
)

const (
	// PING is the topic to inform the server that a peer is still alive
	PING = "PING"
	// PONG is the reply to a PING request
	PONG = "PONG"
)

var logger = log.LoggerFor("github.com/quintans/gomsg/servicedirectory")

// Directory tracks all services providers.
type Directory struct {
	name string
	mu   sync.RWMutex
	// for a directory connection the respective peer is providing services in a specific Port
	providers map[net.Conn]*Provider
	server    *gomsg.Server
	// PingInterval is the interval to between peer pings
	PingInterval time.Duration
	// PingFailures is the number of times a ping can fail before the peer is considered offline
	PingFailures int
}

// NewDirectory creates a peer Directory where all the clients connect
// to know about service providers
func NewDirectory(name string) *Directory {
	dir := &Directory{
		name:         name,
		providers:    make(map[net.Conn]*Provider),
		PingInterval: time.Second,
		PingFailures: 2,
	}
	dir.server = gomsg.NewServer()
	dir.server.SetTimeout(time.Second * 5)

	dir.server.OnClose = func(c net.Conn) {

		dir.mu.Lock()
		defer dir.mu.Unlock()

		var provider = dir.providers[c]
		if provider != nil {
			logger.Infof("[Dir:OnClose] peer %s exited", provider.Endpoint)
			delete(dir.providers, c)
			dir.server.Publish(C_DROPPEER, provider.Endpoint)
		} else {
			logger.Debugf("[Dir:OnClose] closing connection %s for inexistente provider", c.RemoteAddr())
		}
	}
	// returns a list of endpoints (addresses) by service
	dir.server.Handle(S_ADDSERVICE, func(r *gomsg.Request, service string) error {
		var c = r.Connection()
		dir.mu.Lock()
		var provider = dir.providers[c]
		if provider != nil {
			logger.Infof("[Dir:Handle] S_ADDSERVICE new service %s at %s", service, provider.Endpoint)
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
			logger.Infof("[Dir:Handle] S_CANCELSERVICE ignore service %s from %s", service, provider.Endpoint)
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
		// will be using the IP from where the connection came
		provider.Endpoint = fmt.Sprintf("%s:%s", c.RemoteAddr().(*net.TCPAddr).IP, provider.Endpoint)
		logger.Infof("[Dir:Handle] S_PEERREADY peer ready: %+v", provider)

		dir.mu.Lock()
		defer dir.mu.Unlock()

		// Creates a shallow copy of all existing providers
		var tmp = make([]*Provider, len(dir.providers))
		var i = 0
		for _, v := range dir.providers {
			tmp[i] = v
			i++
		}

		// update directory
		dir.providers[c] = &provider

		// notifies all peers, but the caller
		var w = dir.server.Get(c)
		dir.server.SendSkip(w.Wire(), gomsg.REQALL, C_PEERREADY, provider, nil, time.Second)

		return tmp
	})

	dir.SetCodec(gomsg.JsonCodec{})
	return dir
}

// SetCodec sets the codec
func (dir *Directory) SetCodec(codec gomsg.Codec) *Directory {
	dir.server.SetCodec(codec)
	return dir
}

// Name returns the name of this Directory
func (dir *Directory) Name() string {
	return dir.name
}

// Destroy kills this node, releasing resources
func (dir *Directory) Destroy() {
	dir.server.Destroy()
	dir.server = nil

	dir.mu.Lock()
	dir.providers = nil
	dir.mu.Unlock()
}

// Listen starts Directory listening for incoming connections
func (dir *Directory) Listen(addr string) error {
	var err = dir.server.Listen(addr)
	if err != nil {
		return err
	}

	logger.Infof("[Dir:Listen] Ping timeout of %s", dir.PingInterval*time.Duration(dir.PingFailures))
	// keep alive
	var timeout = gomsg.NewTimeout(dir.PingInterval, dir.PingInterval*time.Duration(dir.PingFailures), func(o interface{}) {
		if dir != nil && dir.server != nil {
			c := o.(net.Conn)

			logger.Debugf("[Dir:Listen] Ping timeout. Purging/killing connection from %s", c.RemoteAddr())
			dir.server.Kill(c)
		}
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
	Name     string
	Endpoint string
	Services []string
}

func NewProvider(name string, endpoint string) *Provider {
	return &Provider{name, endpoint, make([]string, 0)}
}

// Find verifies if the Provider has a service
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
type Peer struct {
	*gomsg.Wires

	name       string
	local      *gomsg.Server
	dirs       *gomsg.Wires
	remoteDirs []*gomsg.Client // exists only to hold the service server nodes
	mu         sync.RWMutex
	peers      map[string]*gomsg.Client
	muServers  sync.RWMutex
	providers  map[string]map[string]bool // list of all available endpoints for a service
	services   map[string]bool            // the services that this node provides
	// PingInterval is the interval to between peer pings
	PingInterval time.Duration
	// PingFailures is the number of times a ping can fail before the peer is considered offline
	PingFailures int
}

// NewNode creates a new Node
func NewPeer(name string) *Peer {
	node := &Peer{
		name:         name,
		Wires:        gomsg.NewWires(gomsg.JsonCodec{}),
		dirs:         gomsg.NewWires(gomsg.JsonCodec{}),
		remoteDirs:   make([]*gomsg.Client, 0),
		peers:        make(map[string]*gomsg.Client),
		PingInterval: time.Second,
		PingFailures: 2,
		services:     make(map[string]bool),
		providers:    make(map[string]map[string]bool),
	}
	node.local = gomsg.NewServer()
	// consecutive calls under 500 ms to "DIR/*"
	// will be consumed by the same directory node
	node.dirs.Stick("DIR/*", time.Millisecond*500)
	node.AddSendListener(0, func(event gomsg.SendEvent) {
		node.lazyConnect(event.Name)
	})
	node.SetCodec(gomsg.JsonCodec{})
	return node
}

// SetCodec sets the codec
func (node *Peer) SetCodec(codec gomsg.Codec) *Peer {
	node.Codec = codec
	node.dirs.Codec = codec
	return node
}

func (node *Peer) addService(service string, endpoint string) {
	var endpoints map[string]bool
	if endpoints = node.providers[service]; endpoints == nil {
		endpoints = make(map[string]bool)
		node.providers[service] = endpoints
	}
	endpoints[endpoint] = true
}

// Connect binds a to a local address to provide services
// and connect to the directory remove addresses
func (node *Peer) Connect(bindAddr string, dirAddrs ...string) error {
	var err = node.local.Listen(bindAddr)
	if err != nil {
		return err
	}

	node.remoteDirs = make([]*gomsg.Client, len(dirAddrs))

	for k, dirAddr := range dirAddrs {
		var dir = gomsg.NewClient()
		node.remoteDirs[k] = dir
		dir.Handle(C_PEERREADY, func(provider Provider) {
			logger.Infof("[Peer:Handle] C_PEERREADY peer %s notified of a new peer: %+v", bindAddr, provider)

			node.mu.Lock()

			// group providers according to service
			for _, service := range provider.Services {
				node.addService(service, provider.Endpoint)
			}

			node.mu.Unlock()
		})
		dir.Handle(C_ADDSERVICE, func(service Service) {
			logger.Infof("[Peer:Handle] C_ADDSERVICE peer %s notified of a new service: %+v", bindAddr, service)
			node.mu.Lock()
			node.addService(service.Name, service.Endpoint)
			node.mu.Unlock()
		})
		dir.Handle(C_CANCELSERVICE, func(service Service) {
			logger.Infof("[Peer:Handle] C_CANCELSERVICE peer %s notified of a canceled service: %+v", bindAddr, service)

			node.mu.Lock()
			if endpoints := node.providers[service.Name]; endpoints != nil {
				delete(endpoints, service.Endpoint)
			}
			// does the endpoint exist in another service
			var notFound = true
			for _, endpoints := range node.providers {
				if endpoints[service.Endpoint] {
					notFound = false
					break
				}
			}

			node.mu.Unlock()

			if notFound {
				node.disconnectPeer(service.Endpoint)
			}
		})
		dir.Handle(C_DROPPEER, func(endpoint string) {
			logger.Infof("[Peer:Handle] C_DROPPEER peer %s notified of a droped peer: %s", bindAddr, endpoint)

			node.mu.Lock()
			for _, endpoints := range node.providers {
				delete(endpoints, endpoint)
			}
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
						}, time.Millisecond*50)

						if _, ok := err.(gomsg.TimeoutError); ok || pong != PONG {
							retries--
							logger.Debugf("[Peer:Ticker] PING failed. retries left: %d", retries)
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
			var services = make([]string, len(node.services))
			var i = 0
			for k := range node.services {
				services[i] = k
				i++
			}
			node.mu.RUnlock()

			var provider = NewProvider(node.name, strconv.Itoa(node.local.BindPort()))
			provider.Services = services

			dir.Request(S_PEERREADY, provider, func(ctx gomsg.Response, providers []Provider) {
				node.mu.Lock()
				defer node.mu.Unlock()

				for _, v := range providers {
					for _, s := range v.Services {
						node.addService(s, v.Endpoint)
					}
				}
			})
		}

		dir.OnClose = func(c net.Conn) {
			node.dirs.Kill(c)
			var a = node.remoteDirs
			for k, v := range a {
				if v.Connection() == c {
					copy(a[k:], a[k+1:])
					// since the slice has a non-primitive, we have to zero it
					a[len(a)-1] = nil // zero it
					node.remoteDirs = a[:len(a)-1]
					break
				}
			}
		}

		dir.Connect(dirAddr)
	}

	return nil
}

func (node *Peer) connectPeer(peerAddr string) error {
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

func (node *Peer) disconnectPeer(peerAddr string) {
	node.mu.RLock()
	var cli = node.peers[peerAddr]
	node.mu.RUnlock()

	if cli != nil {
		logger.Debugf("[Peer:disconnectPeer] disconnecting peer: %s", peerAddr)

		// will eventually call OnClose
		// where it will be removed from peers list
		cli.Destroy()
	}
}

// Name returns the name of this node
func (node *Peer) Name() string {
	return node.name
}

// Handle handles incoming messages for a topic
func (node *Peer) Handle(name string, fun interface{}) *Peer {
	node.local.Handle(name, fun)

	node.mu.Lock()
	node.services[name] = true
	node.mu.Unlock()

	// notify service directory cluster of new handle
	node.dirs.RequestAll(S_ADDSERVICE, name, nil, time.Second)

	return node
}

// Cancel cancels handling of incoming messages for a topic
func (node *Peer) Cancel(name string) {
	node.local.Cancel(name)
	// notify service directory cluster of cancel handle
	node.dirs.RequestAll(S_CANCELSERVICE, name, nil, time.Second)

	node.mu.Lock()
	delete(node.services, name)
	node.mu.Unlock()
}

// Destroy kills this node, releasing resources
func (node *Peer) Destroy() {
	if node.local != nil {
		node.local.Destroy()
	}
	if node.remoteDirs != nil {
		// calling destroy will eventually call OnClose
		// where we are shrinking node.remoteDirs.
		var a = append([]*gomsg.Client(nil), node.remoteDirs...)
		for _, v := range a {
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
func (node *Peer) lazyConnect(topic string) {
	// TODO optimize this. shouldn't traverse all end points to see if there are unconnected peers
	node.mu.RLock()
	// find unconnected peers for this topic
	var unconnected = make([]string, 0)
	if endpoints := node.providers[topic]; endpoints != nil {
		for endpoint := range endpoints {
			if cli := node.peers[endpoint]; cli == nil {
				unconnected = append(unconnected, endpoint)
			}
		}
	}
	node.mu.RUnlock()

	for _, endpoint := range unconnected {
		logger.Debugf("[Peer:lazyConnect] connecting peer: %s", endpoint)
		node.connectPeer(endpoint)
	}
}

// Endpoints returns the list of endpoints for a topic/service
func (node *Peer) Endpoints(topic string) []string {
	var tmp = make([]string, 0)

	node.mu.RLock()
	if endpoints := node.providers[topic]; endpoints != nil {
		for endpoint := range endpoints {
			tmp = append(tmp, endpoint)
		}
	}
	node.mu.RUnlock()

	return tmp
}
