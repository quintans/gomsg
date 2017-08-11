package gomsg

import (
	"sync"
	"sync/atomic"
	"time"
)

var _ LoadBalancer = RoundRobinLB{}

type Quarentine struct {
	until time.Time
}

type RoundRobinLB struct {
	sync.RWMutex
	Stickies

	quarantine time.Duration
	counter    uint64
}

func NewRoundRobinLB() RoundRobinLB {
	return RoundRobinLB{
		Stickies:   make(map[string]*Sticky),
		quarantine: time.Second * 5,
	}
}

func (lb RoundRobinLB) SetQuarantine(quarantine time.Duration) {
	lb.quarantine = quarantine
}

// Add adds wire to load balancer
func (lb RoundRobinLB) Add(w *Wire) {
}

// Remove removes wire from load balancer
func (lb RoundRobinLB) Remove(w *Wire) {
	w.load = nil

	lb.Lock()
	defer lb.Unlock()
	lb.Unstick(w)
}

func (lb RoundRobinLB) Prepare(w *Wire, msg Envelope) {
	atomic.AddUint64(&lb.counter, 1)
}

func (lb RoundRobinLB) Done(w *Wire, msg Envelope, err error) {
	if err != nil {
		var load *Quarentine
		if w.load == nil {
			load = new(Quarentine)
			w.load = load
		} else {
			load = w.load.(*Quarentine)
		}
		load.until = time.Now().Add(lb.quarantine)

		lb.Lock()
		defer lb.Unlock()
		lb.Unstick(w)
	}
}

// Balance
func (lb RoundRobinLB) Next(name string, wires []*Wire) *Wire {
	lb.Lock()
	defer lb.Unlock()

	var wire, sticker = lb.IsSticky(name, wires)
	if wire != nil {
		return wire
	}

	var pos = lb.counter % uint64(len(wires))
	var w = wires[int(pos)]
	if sticker != nil {
		sticker.lastWire = w
	}
	return w
}
