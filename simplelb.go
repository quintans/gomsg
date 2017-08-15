package gomsg

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// this implementation includes the circuit breaker pattern

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
		quarantine:  time.Second * 10,
		maxFailures: 3,
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
	w.Load = new(Load)
}

// Remove removes wire from load balancer
func (lb SimpleLB) Remove(w *Wire) {
	w.Load = nil

	lb.Lock()
	defer lb.Unlock()
	lb.Unstick(w)
}

func (lb SimpleLB) Done(w *Wire, msg Envelope, err error) {
	var load = w.Load.(*Load)
	if err == nil {
		load.failures = 0
		// with kind is REQALL or PUB, the PickAll method is used.
		// That method does not increase the load.
		// Since in REQALL and PUB we might only use one connection (groups),
		// we incease the load on delivery
		if test(msg.Kind, REQALL, PUB) {
			addLoad(w)
		}
	} else {
		load.failures++
		if load.failures >= lb.maxFailures {
			load.quarantineUntil = time.Now().Add(lb.quarantine)
		}

		lb.Lock()
		defer lb.Unlock()
		lb.Unstick(w)
	}
}

func addLoad(wire *Wire) {
	var load = wire.Load.(*Load)
	atomic.AddUint64(&load.value, 1)
	fmt.Printf("===>Setting LOAD: %s -> load=%d\n", wire.Conn().RemoteAddr(), load.value)
}

func (lb SimpleLB) PickOne(msg Envelope, wires []*Wire) (*Wire, error) {
	// get valid wires
	var valid, err = lb.PickAll(msg, wires)
	if err != nil {
		return nil, err
	}

	var now = time.Now()
	lb.Lock()
	var wire, sticker = lb.IsSticky(msg.Name, valid)
	lb.Unlock()
	if wire != nil {
		addLoad(wire)
		return wire, nil
	}

	// find the wire with the lowest load
	var min = ^uint64(0) // max for uint64
	var minw *Wire

	for _, w := range valid {
		var load = w.Load.(*Load)
		if now.After(load.quarantineUntil) && load.value < min {
			min = load.value
			minw = w
		}
	}

	if sticker != nil {
		sticker.lastWire = minw
	}
	// prepare
	addLoad(minw)
	return minw, nil
}

func (lb SimpleLB) PickAll(msg Envelope, wires []*Wire) ([]*Wire, error) {
	// NOTE: for messages of type PUB and REQALL the sticker does not make sense

	var now = time.Now()
	var ws = make([]*Wire, 0, len(wires))
	for _, w := range wires {
		var load = w.Load.(*Load)
		if now.After(load.quarantineUntil) {
			ws = append(ws, w)
		}
	}
	if len(ws) == 0 {
		return nil, ServiceUnavailableError(fmt.Errorf(UNAVAILABLESERVICE, msg.Name))
	}

	// sort by load
	sort.Slice(ws, func(i, j int) bool {
		var loadi = ws[i].Load.(*Load)
		var loadj = ws[j].Load.(*Load)
		return loadi.value < loadj.value
	})

	return ws, nil
}
