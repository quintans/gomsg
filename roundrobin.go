package gomsg

import (
	"fmt"
	"sort"
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
	w.Load = nil

	lb.Lock()
	defer lb.Unlock()
	lb.Unstick(w)
}

func (lb RoundRobinLB) Done(w *Wire, msg Envelope, err error) {
	if err != nil {
		var load *Quarentine
		if w.Load == nil {
			load = new(Quarentine)
			w.Load = load
		} else {
			load = w.Load.(*Quarentine)
		}
		load.until = time.Now().Add(lb.quarantine)

		lb.Lock()
		defer lb.Unlock()
		lb.Unstick(w)
	}
}

func (lb RoundRobinLB) PickOne(msg Envelope, wires []*Wire) (*Wire, error) {
	// get valid wires
	var valid, err = lb.PickAll(msg, wires)
	if err != nil {
		return nil, err
	}

	lb.Lock()
	var wire, sticker = lb.IsSticky(msg.Name, valid)
	lb.Unlock()
	if wire != nil {
		// prepare
		atomic.AddUint64(&lb.counter, 1)
		return wire, nil
	}

	var pos = lb.counter % uint64(len(valid))
	var w = valid[int(pos)]
	if sticker != nil {
		sticker.lastWire = w
	}

	// prepare
	atomic.AddUint64(&lb.counter, 1)
	return w, nil
}

func (lb RoundRobinLB) PickAll(msg Envelope, wires []*Wire) ([]*Wire, error) {
	// NOTE: for messages of type PUB and REQALL the sticker does not make sense

	var now = time.Now()
	var ws = make([]*Wire, 0, len(wires))
	for _, w := range wires {
		var load = w.Load.(*Quarentine)
		if now.After(load.until) {
			ws = append(ws, w)
		}
	}

	var size = len(ws)
	if size == 0 {
		return nil, ServiceUnavailableError(fmt.Errorf(UNAVAILABLESERVICE, msg.Name))
	}

	// sort by load
	var counter = atomic.AddUint64(&lb.counter, 1)
	var cursor = int(counter % uint64(size))
	sort.Slice(ws, func(i, j int) bool {
		var posi = transform(cursor, size, i)
		var posj = transform(cursor, size, j)
		return posi < posj
	})

	return ws, nil
}

// round robin transformation
func transform(cursor, size, x int) int {
	x -= cursor
	if x < 0 {
		x = size - cursor
	}
	return x
}
