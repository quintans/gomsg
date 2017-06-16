// THIS IS A WORK IN PROGRESS
// based on http://zguide.zeromq.org/php:chapter8#Detecting-Disappearances

package brokerless

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/quintans/gomsg"
	"github.com/quintans/toolkit"
	"github.com/quintans/toolkit/log"
)

var logger = log.LoggerFor("github.com/quintans/gomsg/brokerless")

const (
	UdpAddr           = "224.0.0.1:9999"
	MaxDatagramSize   = 1024
	TAG               = "PEER"
	UuidSize          = 16
	PING              = "PING"
	BeaconInterval    = time.Second
	BeaconMaxInterval = BeaconInterval * 2
	// BeaconCountdown is the number of consecutive pings inside the ping window
	// to reactivate the UDP health check
	BeaconCountdown = 3
)

type node struct {
	uuid            string
	client          *gomsg.Client
	debouncer       *toolkit.Debouncer
	beaconLastTime  time.Time
	beaconCountdown int
}

type Peer struct {
	uuid    []byte
	tcpAddr string
	timeout time.Duration
	mu      sync.RWMutex
	// peers ones will be used to listen
	peers map[string]*node
	// the server will be used to send
	server   *gomsg.Server
	udpConn  *net.UDPConn
	handlers map[string]interface{}
}

func NewPeer(uuid []byte) *Peer {
	peer := &Peer{
		uuid:     uuid,
		peers:    make(map[string]*node),
		handlers: make(map[string]interface{}),
		timeout:  time.Second,
	}

	return peer
}

func (peer *Peer) Timeout(timeout time.Duration) {
	peer.timeout = timeout
}

func (peer *Peer) Connect(tcpAddr string) {
	logger.Infof("Binding peer %X at %s", peer.uuid, tcpAddr)
	peer.server = gomsg.NewServer()

	// special case where we receive a targeted request
	// when a peer tries to check if I exist
	// because it did not received the beacon in time
	peer.server.Handle(PING, func() {})

	peer.tcpAddr = tcpAddr
	peer.server.Listen(tcpAddr)
	peer.startBeacon(UdpAddr)
	peer.serveMulticastUDP(UdpAddr, peer.beaconHandler)
}

func (peer *Peer) checkPeer(uuid string, addr string) {
	peer.mu.Lock()
	defer peer.mu.Unlock()

	if n := peer.peers[uuid]; n != nil {
		if addr != n.client.Address() {
			logger.Infof("%X - Registering OLD peer %s at %s", peer.uuid, uuid, addr)
			// client reconnected with another address
			n.client.Destroy()
			n.debouncer.Kill()
			delete(peer.peers, uuid)
			peer.connectPeer(uuid, addr)
		} else {
			peer.checkBeacon(n)
		}
	} else {
		logger.Infof("%X - Registering NEW peer %s at %s", peer.uuid, uuid, addr)
		peer.connectPeer(uuid, addr)
	}
}

func (peer *Peer) checkBeacon(n *node) {
	if n.beaconCountdown == 0 {
		// this debouncer is only for UDP beacon when beaconCountdown == 0
		n.debouncer.Delay(nil)
	} else {
		println("check " + n.uuid)
		var now = time.Now()
		if now.Sub(n.beaconLastTime) < BeaconMaxInterval {
			n.beaconCountdown--
		} else {
			n.beaconCountdown = BeaconCountdown
		}
		if n.beaconCountdown == 0 {
			// the client responded, switching to UDP
			logger.Infof("%X - Peer %s at %s responded. Switching to UDP listening", peer.uuid, n.uuid, n.client.Address())
			// kill the TCP health check
			n.debouncer.Kill()
			peer.healthCheckByUDP(n)
		}
		n.beaconLastTime = now
	}
}

func (peer *Peer) connectPeer(uuid string, addr string) error {
	var cli = gomsg.NewClient()
	var e = <-cli.Connect(addr)
	if e != nil {
		logger.Errorf("%X - unable to connect to %s at %s", peer.uuid, uuid, addr)
		return e
	}
	var n = &node{
		uuid:   uuid,
		client: cli,
	}
	peer.healthCheckByUDP(n)
	peer.peers[uuid] = n

	// apply all handlers
	for k, v := range peer.handlers {
		cli.Handle(k, v)
	}
	return nil
}

func (peer *Peer) dropPeer(n *node) {
	peer.mu.Lock()
	defer peer.mu.Unlock()

	logger.Infof("%X - Purging unresponsive peer %s at %s", peer.uuid, n.uuid, n.client.Address())
	n.client.Destroy()
	n.debouncer = nil
	delete(peer.peers, n.uuid)
}

// healthCheckByIP is the client that checks actively the remote peer
func (peer *Peer) healthCheckByTCP(n *node) {
	var ticker = toolkit.NewTicker(BeaconInterval, func(t time.Time) {
		<-n.client.RequestTimeout(PING, nil, func() {
			n.debouncer.Delay(nil)
		}, BeaconInterval)
	})
	n.debouncer = toolkit.NewDebounce(BeaconMaxInterval, func(o interface{}) {
		peer.dropPeer(n)
	})
	n.debouncer.OnExit = func() {
		ticker.Stop()
	}
}

func (peer *Peer) healthCheckByUDP(n *node) {
	n.debouncer = toolkit.NewDebounce(BeaconMaxInterval, func(o interface{}) {
		// the client did not responded, switching to TCP
		logger.Infof("%X - Silent peer %s at %s. Switching to TCP ping", peer.uuid, n.uuid, n.client.Address())
		n.beaconCountdown = BeaconCountdown
		peer.healthCheckByTCP(n)
	})
}

func (peer *Peer) beaconHandler(src *net.UDPAddr, n int, b []byte) {
	// starts with tag
	if bytes.HasPrefix(b, []byte(TAG)) {
		var r = bytes.NewReader(b)
		r.Seek(int64(len(TAG)), io.SeekStart)
		var uuid = make([]byte, UuidSize)
		r.Read(uuid)
		// ignore self
		if bytes.Compare(uuid, peer.uuid) != 0 {
			var buf16 = make([]byte, 2)
			r.Read(buf16)
			var port = int(binary.LittleEndian.Uint16(buf16))
			peer.checkPeer(fmt.Sprintf("%X", uuid), src.IP.String()+":"+strconv.Itoa(port))
		}
	}
}

func (peer *Peer) Destroy() {
	peer.server.Destroy()
	var conn = peer.udpConn
	peer.udpConn = nil
	if conn != nil {
		conn.Close()
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	for _, v := range peer.peers {
		v.debouncer.Kill()
		v.client.Destroy()
	}
	peer.peers = make(map[string]*node)
}

func (peer *Peer) startBeacon(a string) error {
	addr, err := net.ResolveUDPAddr("udp", a)
	if err != nil {
		return nil
	}
	c, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil
	}
	var buf16 = make([]byte, 2)
	var port = uint16(peer.server.BindPort())
	binary.LittleEndian.PutUint16(buf16, port)

	var buf bytes.Buffer
	buf.WriteString(TAG)
	buf.Write(peer.uuid)
	buf.Write(buf16)
	go func(data []byte) {
		for peer.server.Listener() != nil {
			c.Write(data)
			time.Sleep(BeaconInterval)
		}
	}(buf.Bytes())
	return nil
}

func (peer *Peer) serveMulticastUDP(a string, hnd func(*net.UDPAddr, int, []byte)) error {
	addr, err := net.ResolveUDPAddr("udp", a)
	if err != nil {
		return err
	}
	l, err := net.ListenMulticastUDP("udp", nil, addr)
	if err != nil {
		return err
	}
	l.SetReadBuffer(MaxDatagramSize)
	peer.udpConn = l
	go func() {
		for {
			var payload = make([]byte, MaxDatagramSize)
			n, src, err := l.ReadFromUDP(payload)
			if peer.udpConn == nil {
				return
			} else if err != nil {
				logger.Errorf("%X - ReadFromUDP failed: %s", peer.uuid, err)
				return
			}
			hnd(src, n, payload)
		}
	}()
	return nil
}

func (peer *Peer) Handle(name string, hnd interface{}) {
	peer.mu.Lock()
	defer peer.mu.Unlock()

	peer.handlers[name] = hnd
	for _, v := range peer.peers {
		v.client.Handle(name, hnd)
	}
}

func (peer *Peer) Cancel(name string) {
	peer.mu.Lock()
	defer peer.mu.Unlock()

	for _, v := range peer.peers {
		v.client.Cancel(name)
	}
}

func (peer *Peer) Publish(name string, payload interface{}) <-chan error {
	return peer.server.Publish(name, payload)
}

func (peer *Peer) Push(name string, payload interface{}) <-chan error {
	return peer.server.Push(name, payload)
}

func (peer *Peer) Request(name string, payload interface{}, handler interface{}) <-chan error {
	return peer.server.RequestTimeout(name, payload, handler, peer.timeout)
}

func (peer *Peer) RequestAll(name string, payload interface{}, handler interface{}) <-chan error {
	return peer.server.RequestAll(name, payload, handler, peer.timeout)
}
