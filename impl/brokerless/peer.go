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

// defaults
const (
	UuidSize              = 16
	PING                  = "PING"
	beaconMaxDatagramSize = 1024
	beaconAddr            = "224.0.0.1:9999"
	beaconName            = "PEER"
	beaconInterval        = time.Second
	beaconMaxInterval     = beaconInterval * 2
	// BeaconCountdown is the number of consecutive pings inside the ping window
	// to reactivate the UDP health check
	beaconCountdown = 3
)

type node struct {
	uuid            string
	client          *gomsg.Client
	debouncer       *toolkit.Debouncer
	beaconLastTime  time.Time
	beaconCountdown int
}

type Config struct {
	Uuid                  []byte
	BeaconAddr            string
	BeaconName            string
	BeaconMaxDatagramSize int
	BeaconInterval        time.Duration
	BeaconMaxInterval     time.Duration
	// BeaconCountdown is the number of consecutive pings inside the ping window
	// to reactivate the UDP health check
	BeaconCountdown int
}

type Peer struct {
	sync.RWMutex

	cfg            Config
	tcpAddr        string
	requestTimeout time.Duration
	// peers ones will be used to listen
	peers map[string]*node
	// the server will be used to send
	server       *gomsg.Server
	udpConn      *net.UDPConn
	handlers     map[string]interface{}
	beaconTicker *toolkit.Ticker
}

func NewPeer(cfg Config) *Peer {
	if cfg.Uuid == nil {
		panic("Uuid must be defined")
	}
	// apply defaults
	if cfg.BeaconAddr == "" {
		cfg.BeaconAddr = beaconAddr
	}
	if cfg.BeaconName == "" {
		cfg.BeaconName = beaconName
	}
	if cfg.BeaconMaxDatagramSize == 0 {
		cfg.BeaconMaxDatagramSize = beaconMaxDatagramSize
	}
	if cfg.BeaconInterval == 0 {
		cfg.BeaconInterval = beaconInterval
	}
	if cfg.BeaconMaxInterval == 0 {
		cfg.BeaconMaxInterval = beaconMaxInterval
	}
	if cfg.BeaconCountdown == 0 {
		cfg.BeaconCountdown = beaconCountdown
	}

	peer := &Peer{
		cfg:            cfg,
		peers:          make(map[string]*node),
		handlers:       make(map[string]interface{}),
		requestTimeout: time.Second,
	}

	return peer
}

func (peer *Peer) SetRequestTimeout(timeout time.Duration) {
	peer.requestTimeout = timeout
}

func (peer *Peer) Connect(tcpAddr string) {
	logger.Infof("Binding peer %X at %s", peer.cfg.Uuid, tcpAddr)
	peer.server = gomsg.NewServer()

	// special case where we receive a targeted request
	// when a peer tries to check if I exist
	// because it did not received the beacon in time
	peer.server.Handle(PING, func() {})

	peer.tcpAddr = tcpAddr
	peer.serveUDP(peer.cfg.BeaconAddr, peer.beaconHandler)
	peer.server.OnBind = func(l net.Listener) {
		fmt.Println("==========> Binded")
		peer.startBeacon(peer.cfg.BeaconAddr)
	}

	peer.server.Listen(tcpAddr)
}

func (peer *Peer) checkPeer(uuid string, addr string) {
	peer.Lock()
	defer peer.Unlock()

	if n := peer.peers[uuid]; n != nil {
		if addr != n.client.Address() {
			logger.Infof("%X - Registering OLD peer %s at %s", peer.cfg.Uuid, uuid, addr)
			// client reconnected with another address
			n.client.Destroy()
			n.debouncer.Kill()
			delete(peer.peers, uuid)
			peer.connectPeer(uuid, addr)
		} else {
			peer.checkBeacon(n)
		}
	} else {
		logger.Infof("%X - Registering NEW peer %s at %s", peer.cfg.Uuid, uuid, addr)
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
		if now.Sub(n.beaconLastTime) < peer.cfg.BeaconMaxInterval {
			n.beaconCountdown--
		} else {
			n.beaconCountdown = peer.cfg.BeaconCountdown
		}
		if n.beaconCountdown == 0 {
			// the client responded, switching to UDP
			logger.Infof("%X - Peer %s at %s responded. Switching to UDP listening", peer.cfg.Uuid, n.uuid, n.client.Address())
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
		logger.Errorf("%X - unable to connect to %s at %s", peer.cfg.Uuid, uuid, addr)
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
	peer.Lock()
	defer peer.Unlock()

	logger.Infof("%X - Purging unresponsive peer %s at %s", peer.cfg.Uuid, n.uuid, n.client.Address())
	n.client.Destroy()
	n.debouncer = nil
	delete(peer.peers, n.uuid)
}

// healthCheckByIP is the client that checks actively the remote peer
func (peer *Peer) healthCheckByTCP(n *node) {
	var ticker = toolkit.NewTicker(peer.cfg.BeaconInterval, func(t time.Time) {
		<-n.client.RequestTimeout(PING, nil, func() {
			n.debouncer.Delay(nil)
		}, peer.cfg.BeaconInterval)
	})
	n.debouncer = toolkit.NewDebounce(peer.cfg.BeaconMaxInterval, func(o interface{}) {
		peer.dropPeer(n)
	})
	n.debouncer.OnExit = func() {
		ticker.Stop()
	}
}

func (peer *Peer) healthCheckByUDP(n *node) {
	n.debouncer = toolkit.NewDebounce(peer.cfg.BeaconMaxInterval, func(o interface{}) {
		// the client did not responded, switching to TCP
		logger.Infof("%X - Silent peer %s at %s. Switching to TCP ping", peer.cfg.Uuid, n.uuid, n.client.Address())
		n.beaconCountdown = peer.cfg.BeaconCountdown
		peer.healthCheckByTCP(n)
	})
}

func (peer *Peer) beaconHandler(src *net.UDPAddr, n int, b []byte) {
	// starts with tag
	if bytes.HasPrefix(b, []byte(peer.cfg.BeaconName)) {
		var r = bytes.NewReader(b)
		r.Seek(int64(len(peer.cfg.BeaconName)), io.SeekStart)
		var uuid = make([]byte, UuidSize)
		r.Read(uuid)
		// ignore self
		if bytes.Compare(uuid, peer.cfg.Uuid) != 0 {
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
	peer.Lock()
	defer peer.Unlock()
	for _, v := range peer.peers {
		v.debouncer.Kill()
		v.client.Destroy()
	}
	peer.peers = make(map[string]*node)
	if peer.beaconTicker != nil {
		peer.beaconTicker.Stop()
	}
	peer.beaconTicker = nil
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
	buf.WriteString(peer.cfg.BeaconName)
	buf.Write(peer.cfg.Uuid)
	buf.Write(buf16)

	var data = buf.Bytes()
	if peer.beaconTicker != nil {
		peer.beaconTicker.Stop()
	}
	peer.beaconTicker = toolkit.NewDelayedTicker(0, peer.cfg.BeaconInterval, func(t time.Time) {
		c.Write(data)
	})

	return nil
}

func (peer *Peer) serveUDP(a string, hnd func(*net.UDPAddr, int, []byte)) error {
	addr, err := net.ResolveUDPAddr("udp", a)
	if err != nil {
		return err
	}

	l, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	l.SetReadBuffer(peer.cfg.BeaconMaxDatagramSize)
	peer.udpConn = l
	go func() {
		for {
			var payload = make([]byte, peer.cfg.BeaconMaxDatagramSize)
			n, src, err := l.ReadFromUDP(payload)
			if peer.udpConn == nil {
				return
			} else if err != nil {
				logger.Errorf("%X - ReadFromUDP failed: %s", peer.cfg.Uuid, err)
				return
			}
			hnd(src, n, payload)
		}
	}()
	return nil
}

func (peer *Peer) Handle(name string, hnd interface{}) {
	peer.Lock()
	defer peer.Unlock()

	peer.handlers[name] = hnd
	for _, v := range peer.peers {
		v.client.Handle(name, hnd)
	}
}

func (peer *Peer) Cancel(name string) {
	peer.Lock()
	defer peer.Unlock()

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
	return peer.server.RequestTimeout(name, payload, handler, peer.requestTimeout)
}

func (peer *Peer) RequestAll(name string, payload interface{}, handler interface{}) <-chan error {
	return peer.server.RequestAll(name, payload, handler, peer.requestTimeout)
}
