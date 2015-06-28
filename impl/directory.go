package impl

import (
	"fmt"
	"net"
	"sync"

	"github.com/quintans/gomsg"
)

type Directory struct {
	mu     sync.RWMutex
	peers  map[net.Conn]string
	server *gomsg.Server
}

// creates a directory service where all the clients connect to know about their peers
func NewDirectory(addr string, codec gomsg.Codec) *Directory {
	dir := &Directory{
		peers: make(map[net.Conn]string),
	}
	dir.server = gomsg.NewServer().SetCodec(codec)
	dir.server.OnClose = func(c net.Conn) {
		// this will be called if a peer stops pinging
		fmt.Println("< Dir: peer", c.RemoteAddr(), "exited")

		dir.mu.Lock()
		defer dir.mu.Unlock()

		delete(dir.peers, c)
		dir.server.Publish("DROP", dir.peers[c])
	}
	// returns all the peer addresses
	dir.server.Handle("DIR", func(ctx *gomsg.Request) []string {
		fmt.Println("< Dir: DIRECTORY LIST")
		dir.mu.RLock()
		defer dir.mu.RUnlock()

		addrs := make([]string, len(dir.peers))
		i := 0
		for _, v := range dir.peers {
			addrs[i] = v
			i++
		}
		return addrs
	})
	dir.server.Handle("READY", func(ctx *gomsg.Request, port int) {
		c := ctx.Connection()
		addr := fmt.Sprint(c.RemoteAddr().(*net.TCPAddr).IP, ":", port)
		fmt.Println("< Dir: peer", addr, "PEER READY")

		dir.mu.Lock()
		defer dir.mu.Unlock()

		dir.peers[c] = addr

		dir.server.Publish("NEW", addr)
	})
	dir.server.Listen(addr)

	return dir
}

type Peered struct {
	addr string
	peer *gomsg.Client
}

type Peers struct {
	mu    sync.RWMutex
	peers []*Peered
}

func NewPeers() *Peers {
	this := new(Peers)
	this.peers = make([]*Peered, 0)
	return this
}

func (this *Peers) Clear() {
	this.mu.Lock()
	defer this.mu.Unlock()

	for _, v := range this.peers {
		v.peer.Destroy()
	}

	this.peers = make([]*Peered, 0)
}

func (this *Peers) Put(addr string, peer *gomsg.Client) {
	this.mu.Lock()
	defer this.mu.Unlock()

	for _, v := range this.peers {
		if v.addr == addr {
			v.peer = peer
			return
		}
	}
	this.peers = append(this.peers, &Peered{addr, peer})
}

func (this *Peers) Remove(addr string) {
	this.mu.Lock()
	defer this.mu.Unlock()

	for k, v := range this.peers {
		if v.addr == addr {
			v.peer.Destroy()
			this.peers = append(this.peers[:k], this.peers[k+1:]...)
		}
	}
}

func (this *Peers) List() []*gomsg.Client {
	return this.list(false)
}

func (this *Peers) Rotate() []*gomsg.Client {
	return this.list(true)
}

func (this *Peers) list(rotate bool) []*gomsg.Client {
	this.mu.Lock()
	defer this.mu.Unlock()

	size := len(this.peers)
	r := make([]*gomsg.Client, size)
	for i, v := range this.peers {
		r[i] = v.peer
	}

	// rotate
	if rotate && size > 1 {
		this.peers = append(this.peers[1:], this.peers[:1]...)
	}

	return r
}

type Peer struct {
	codec gomsg.Codec
	self  string
	peers *Peers
	local *gomsg.Server
	dir   *gomsg.Client
}

func NewPeer(dirAddr string, bindAddr string, codec gomsg.Codec) *Peer {
	this := &Peer{
		codec: codec,
		peers: NewPeers(),
	}
	this.local = gomsg.NewServer().SetCodec(codec)
	this.local.Listen(bindAddr)

	this.dir = gomsg.NewClient().SetCodec(codec)
	this.dir.OnConnect = func(c net.Conn) {
		this.dir.Handle("NEW", func(peerAddr string) {
			if peerAddr != this.self {
				fmt.Println("====>", bindAddr, ": new peer at", peerAddr)
				cli := gomsg.NewClient().SetCodec(codec)
				cli.Connect(peerAddr)
				this.peers.Put(peerAddr, cli)
			}
		})
		this.dir.Handle("DROP", func(peerAddr string) {
			this.peers.Remove(peerAddr)
		})

		port := this.local.Port()
		<-this.dir.Request("DIR", nil, func(ctx gomsg.Response, addrs []string) {
			this.self = fmt.Sprint(ctx.Connection().LocalAddr().(*net.TCPAddr).IP, ":", port)
			for _, v := range addrs {
				cli := gomsg.NewClient().SetCodec(this.codec)
				cli.Connect(v)
				this.peers.Put(v, cli)
			}
		})
		this.dir.Push("READY", port)
	}
	this.dir.Connect(dirAddr)

	return this
}

func (this *Peer) Publish(name string, payload interface{}) {
	// every client knows its remote topics
	for _, cli := range this.peers.List() {
		cli.Publish(name, payload)
	}
}

func (this *Peer) Push(name string, payload interface{}) error {
	// every client knows its remote topics
	for _, cli := range this.peers.Rotate() {
		err := <-cli.Push(name, payload)
		if err == nil {
			return nil
		}
		fmt.Println("====> peers", cli.Connection().LocalAddr(), err)
	}
	return gomsg.UNKNOWNTOPIC
}

func (this *Peer) Request(name string, payload interface{}, handler interface{}) <-chan error {
	errch := make(chan error, 1)
	err := gomsg.UNKNOWNTOPIC

	// every client knows its remote topics
	for _, cli := range this.peers.Rotate() {
		err := <-cli.Request(name, payload, handler)
		if err == nil {
			break
		}
	}
	errch <- err
	return errch
}

func (this *Peer) RequestAll(name string, payload interface{}, handler interface{}) <-chan error {
	var wg sync.WaitGroup
	err := gomsg.UNKNOWNTOPIC
	for _, cli := range this.peers.List() {
		wg.Add(1)
		ch := cli.Request(name, payload, handler)
		go func() {
			defer wg.Done()
			// waits completion
			e := <-ch
			if e == nil {
				err = nil
			}
		}()
	}
	errch := make(chan error, 1)
	go func() {
		// Wait for all requests to complete.
		wg.Wait()
		errch <- err
	}()
	return errch
}

func (this *Peer) Handle(name string, fun interface{}) *Peer {
	this.local.Handle(name, fun)
	return this
}

func (this *Peer) Cancel(name string) {
	this.local.Cancel(name)
}

func (this *Peer) Destroy() {
	this.dir.Destroy()
	this.local.Destroy()

	// find the client for this address
	this.peers.Clear()

	this.peers = nil
	this.local = nil
	this.dir = nil
}
