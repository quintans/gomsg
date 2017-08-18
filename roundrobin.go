package gomsg

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var _ LBPolicy = &SimpleLBPolicy{}

type SimpleLBPolicy struct {
	Quarantine time.Duration
	until      time.Time
}

func (this *SimpleLBPolicy) IncLoad(name string) uint64 {
	return 0
}

func (this *SimpleLBPolicy) Load(name string) uint64 {
	return 0
}

func (this *SimpleLBPolicy) Failed(name string) {
	this.until = time.Now().Add(this.Quarantine)
}

func (this *SimpleLBPolicy) Succeeded(string) {
}

func (this *SimpleLBPolicy) InQuarantine(string) bool {
	return time.Now().Before(this.until)
}

var _ LoadBalancer = RoundRobinLB{}

type RoundRobinLB struct {
	sync.RWMutex
	Stickies

	policyFactory func() LBPolicy
	counter       uint64
}

func NewRoundRobinLB() RoundRobinLB {
	return RoundRobinLB{
		Stickies: make(map[string]*Sticky),
		policyFactory: func() LBPolicy {
			return &SimpleLBPolicy{
				Quarantine: time.Second * 5,
			}
		},
	}
}

func (lb RoundRobinLB) SetPolicyFactory(factory func() LBPolicy) {
	lb.policyFactory = factory
}

// Add adds wire to load balancer
func (lb RoundRobinLB) Add(w *Wire) {
	w.Policy = lb.policyFactory()
}

// Remove removes wire from load balancer
func (lb RoundRobinLB) Remove(w *Wire) {
	w.Policy = nil

	lb.Lock()
	defer lb.Unlock()
	lb.Unstick(w)
}

func (lb RoundRobinLB) AllDone(msg Envelope, err error) error {
	return err
}

func (lb RoundRobinLB) Done(w *Wire, msg Envelope, err error) {
	if err != nil {
		w.Policy.Failed(msg.Name)

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

	// since valid is sorted, the first one has the lowest load
	var w = valid[0]
	if sticker != nil {
		sticker.lastWire = w
	}

	// prepare
	atomic.AddUint64(&lb.counter, 1)
	return w, nil
}

func (lb RoundRobinLB) PickAll(msg Envelope, wires []*Wire) ([]*Wire, error) {
	// NOTE: for messages of type PUB and REQALL the sticker does not make sense

	// make array with the max capacity
	var ws = make([]*Wire, 0, len(wires))
	for _, w := range wires {
		if !w.Policy.InQuarantine(msg.Name) {
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
