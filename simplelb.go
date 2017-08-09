package gomsg

import (
	"sync"
	"sync/atomic"
	"time"
)

var _ LoadBalancer = SimpleLB{}

type Load struct {
	value           uint64
	failures        int
	quarantineUntil time.Time
}

type SimpleLB struct {
	sync.RWMutex
	Stickies

	maxFailures int
	quarantine  time.Duration
}

func NewSimpleLB() SimpleLB {
	return SimpleLB{
		Stickies:    make(map[string]*Sticky),
		quarantine:  time.Second * 5,
		maxFailures: 2,
	}
}

func (lb SimpleLB) MaxFailures(maxFailures int) {
	lb.maxFailures = maxFailures
}

func (lb SimpleLB) SetQuarantine(quarantine time.Duration) {
	lb.quarantine = quarantine
}

// Add adds wire to load balancer
func (lb SimpleLB) Add(w *Wire) {
	w.load = new(Load)
}

// Remove removes wire from load balancer
func (lb SimpleLB) Remove(w *Wire) {
	w.load = nil

	lb.Lock()
	defer lb.Unlock()
	lb.Unstick(w)
}

func (lb SimpleLB) Prepare(w *Wire, msg Envelope) {
	var load = w.load.(*Load)
	atomic.AddUint64(&load.value, 1)
}

func (lb SimpleLB) Success(w *Wire, msg Envelope) {
	var load = w.load.(*Load)
	load.failures = 0
}

func (lb SimpleLB) Failure(w *Wire, msg Envelope, err error) {
	var load = w.load.(*Load)
	load.failures++
	if load.failures >= lb.maxFailures {
		load.quarantineUntil = time.Now().Add(lb.quarantine)
	}

	lb.Lock()
	defer lb.Unlock()
	lb.Unstick(w)
}

// Balance
func (lb SimpleLB) Next(name string, wires []*Wire) *Wire {
	lb.Lock()
	defer lb.Unlock()

	var wire, sticker = lb.IsSticky(name, wires)
	if wire != nil {
		return wire
	}

	// find the wire with the lowest load
	var min = ^uint64(0) // max for uint64
	var minw *Wire
	var now = time.Now()
	for _, w := range wires {
		var load = w.load.(*Load)
		if load.value < min && now.After(load.quarantineUntil) {
			min = load.value
			minw = w
		}
	}

	if sticker != nil {
		sticker.lastWire = minw
	}
	return minw
}
