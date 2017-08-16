package gomsg

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

var _ LBPolicy = &HysteresisPolicy{}

type HysteresisPolicy struct {
	MaxFailures     int
	failures        int
	MinSuccesses    int
	successes       int
	Quarantine      time.Duration
	quarantineUntil time.Time
	load            uint64
}

func (this *HysteresisPolicy) AddLoad(name string) uint64 {
	this.load++
	return this.load
}

func (this *HysteresisPolicy) Load(name string) uint64 {
	return this.load
}

func (this *HysteresisPolicy) Failed(name string) {
	if this.failures >= this.MaxFailures {
		// OPEN
		this.successes = 0
		this.quarantineUntil = time.Now().Add(this.Quarantine)
		return
	}

	this.successes = 0
	this.failures++
}

func (this *HysteresisPolicy) Succeeded(string) {
	if this.successes >= this.MinSuccesses {
		// CLOSE
		this.failures = 0
		return
	}
	if this.failures < this.MaxFailures {
		// not OPEN
		this.failures = 0
	}
	this.successes++
}

func (this *HysteresisPolicy) InQuarantine(string) bool {
	return time.Now().Before(this.quarantineUntil)
}

var _ LoadBalancer = SimpleLB{}

type SimpleLB struct {
	sync.RWMutex
	Stickies
	policyFactory func() LBPolicy
}

func NewSimpleLB() SimpleLB {
	return SimpleLB{
		Stickies: make(map[string]*Sticky),
		policyFactory: func() LBPolicy {
			return &HysteresisPolicy{
				Quarantine:  time.Second * 10,
				MaxFailures: 3,
			}
		},
	}
}

func (lb SimpleLB) SetPolicyFactory(factory func() LBPolicy) {
	lb.policyFactory = factory
}

// Add adds wire to load balancer
func (lb SimpleLB) Add(w *Wire) {
	w.Policy = lb.policyFactory()
}

// Remove removes wire from load balancer
func (lb SimpleLB) Remove(w *Wire) {
	w.Policy = nil

	lb.Lock()
	defer lb.Unlock()
	lb.Unstick(w)
}

func (lb SimpleLB) Done(w *Wire, msg Envelope, err error) {
	if err == nil {
		w.Policy.Succeeded(msg.Name)
		// when kind is REQALL or PUB, the PickAll method is used.
		// That method does not increase the load.
		// Since in REQALL and PUB we might only use one connection (groups),
		// we incease the load on delivery
		if test(msg.Kind, REQALL, PUB) {
			w.Policy.AddLoad(msg.Name)
		}
	} else {
		w.Policy.Failed(msg.Name)

		lb.Lock()
		defer lb.Unlock()
		lb.Unstick(w)
	}
}

func (lb SimpleLB) PickOne(msg Envelope, wires []*Wire) (*Wire, error) {
	// get valid wires
	var valid, err = lb.PickAll(msg, wires)
	if err != nil {
		return nil, err
	}

	lb.Lock()
	var wire, sticker = lb.IsSticky(msg.Name, valid)
	lb.Unlock()
	if wire != nil {
		wire.Policy.AddLoad(msg.Name)
		return wire, nil
	}

	// since valid is sorted, the first one has the lowest load
	var minw = valid[0]

	if sticker != nil {
		sticker.lastWire = minw
	}
	// prepare
	minw.Policy.AddLoad(msg.Name)
	return minw, nil
}

func (lb SimpleLB) PickAll(msg Envelope, wires []*Wire) ([]*Wire, error) {
	// NOTE: for messages of type PUB and REQALL the sticker does not make sense

	var ws = make([]*Wire, 0, len(wires))
	for _, w := range wires {
		if !w.Policy.InQuarantine(msg.Name) {
			ws = append(ws, w)
		}
	}
	if len(ws) == 0 {
		return nil, ServiceUnavailableError(fmt.Errorf(UNAVAILABLESERVICE, msg.Name))
	}

	// sort by load
	sort.Slice(ws, func(i, j int) bool {
		var loadi = ws[i].Policy.Load(msg.Name)
		var loadj = ws[j].Policy.Load(msg.Name)
		return loadi < loadj
	})

	return ws, nil
}
