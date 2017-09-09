package gomsg

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

const maxUint64 = ^uint64(0)

var _ LBPolicy = &HysteresisPolicy{}

type loader struct {
	id   uint64
	load uint64
	time time.Time
}

func (l loader) Compare(c Comparer) int {
	if other, ok := c.(loader); ok {
		if l.load < other.load {
			return -1
		} else if l.load > other.load {
			return 1
		} else {
			return 0
		}
	}
	return 10
}

// HysteresisPolicy is the policy to control the connections load.
// For each topic name there will be a load
// that is the sum time that the connection is borrowed.
// Each load is initiated with a load, usually the minimum load for all wires for that topic.
// The strategy used in this policy is of a Circuit Breaker.
// After the circuit is open, a quarantine time is observed. After that quarantine,
// the topic is tested letting ONE connection pass. Until that connection is returned,
// no more connections are borrowed.
// After the connection is returned is when we test if we should close the circuit.
type HysteresisPolicy struct {
	sync.RWMutex
	data         map[string]*HysteresisPolicyData
	MaxFailures  int
	MinSuccesses int
	Quarantine   time.Duration
}

type HysteresisPolicyData struct {
	continuousFailures  int
	continuousSuccesses int
	quarantineUntil     time.Time
	open                bool
	test                bool
	load                uint64
	quarantine          bool

	borrowed    map[uint64]loader
	borrowedIdx uint64
}

func NewHysteresisPolicy() *HysteresisPolicy {
	return &HysteresisPolicy{
		Quarantine:  time.Second * 10,
		MaxFailures: 3,
		data:        make(map[string]*HysteresisPolicyData),
	}
}

func (this *HysteresisPolicy) SetQuarantine(quarantine time.Duration) *HysteresisPolicy {
	this.Quarantine = quarantine
	return this
}

func (this *HysteresisPolicy) SetMaxFailures(maxFailures int) *HysteresisPolicy {
	this.MaxFailures = maxFailures
	return this
}

func (this *HysteresisPolicy) SetMinSuccesses(minSuccesses int) *HysteresisPolicy {
	this.MinSuccesses = minSuccesses
	return this
}

func (this *HysteresisPolicy) init(name string, initial Comparer) Comparer {
	var borrowed = make(map[uint64]loader)

	var borrow = loader{
		time: time.Now(),
	}
	if initial != nil {
		borrow.load = initial.(loader).load
	}
	borrowed[0] = borrow

	var data = &HysteresisPolicyData{
		load:     borrow.load,
		borrowed: borrowed,
	}

	this.data[name] = data

	return borrow
}

func (this *HysteresisPolicy) Borrow(name string, initialize func(name string) Comparer) Comparer {
	this.Lock()
	defer this.Unlock()

	var borrow loader
	var data = this.data[name]
	if data == nil {
		var load = initialize(name)
		return this.init(name, load)
	} else {
		// if the circuit is open,
		// we just let one test connection to be borrowed
		if data.open {
			if data.test {
				// testing connection is already borrowed
				return nil
			} else {
				// lock testing connection
				data.test = true
			}
		}

		data.borrowedIdx++
		borrow = loader{
			load: data.load,
			id:   data.borrowedIdx,
			time: time.Now(),
		}
		data.borrowed[data.borrowedIdx] = borrow
	}

	return borrow
}

func (this *HysteresisPolicy) Return(name string, loaded Comparer, err error) {
	this.Lock()
	defer this.Unlock()

	var load = loaded.(loader)
	var data = this.data[name]
	if data != nil {
		// reset the testing flag
		data.test = false
		// will sum time diff between when it was borrowed and now.
		data.load += uint64(time.Now().Sub(load.time))
		if err == nil {
			this.succeeded(data)
		} else {
			if this.failed(data) {
				// reset load when entering in quarantine
				data.load = 0
			}
		}
		delete(data.borrowed, load.id)
	}
}

func (this *HysteresisPolicy) failed(data *HysteresisPolicyData) bool {
	data.continuousSuccesses = 0
	if data.open {
		data.quarantineUntil = time.Now().Add(this.Quarantine)
		return true
	} else {
		data.continuousFailures++
		if this.MaxFailures > -1 && data.continuousFailures > this.MaxFailures {
			// OPEN
			data.open = true
			data.quarantineUntil = time.Now().Add(this.Quarantine)
			return true
		}
	}

	return false
}

func (this *HysteresisPolicy) succeeded(data *HysteresisPolicyData) {
	data.continuousFailures = 0
	if data.open {
		data.continuousSuccesses++
		if data.continuousSuccesses > this.MinSuccesses {
			// CLOSE
			data.open = false
		}
	}
}

func (this *HysteresisPolicy) Quarantined(name string) bool {
	this.RLock()
	defer this.RUnlock()

	var data = this.data[name]
	if data == nil {
		// if it has no data is new
		return false
	} else {
		return data.test || time.Now().Before(data.quarantineUntil)

	}

}

func (this *HysteresisPolicy) Load(name string) Comparer {
	this.RLock()
	defer this.RUnlock()

	var data = this.data[name]
	if data == nil {
		// if it has no data is new
		return loader{}
	} else {
		// current load + the borrowed loads
		var now = time.Now()
		var borrowed uint64
		for _, v := range data.borrowed {
			borrowed += uint64(now.Sub(v.time))
		}
		return loader{load: data.load + borrowed}
	}
}

var _ LoadBalancer = &SimpleLB{}

type SimpleLB struct {
	sync.RWMutex
	Stickies
	policyFactory func() LBPolicy
	wires         map[*Wire]bool
}

func NewSimpleLB() *SimpleLB {
	return &SimpleLB{
		Stickies: make(map[string]*Sticky),
		wires:    make(map[*Wire]bool),
		policyFactory: func() LBPolicy {
			return NewHysteresisPolicy()
		},
	}
}

type Wired struct {
	wire   *Wire
	loader Comparer
}

func (this *Wired) Wire() *Wire {
	return this.wire
}

func (lb *SimpleLB) SetPolicyFactory(factory func() LBPolicy) {
	lb.policyFactory = factory
}

// Add adds wire to load balancer
func (lb *SimpleLB) Add(w *Wire) {
	lb.Lock()
	w.Policy = lb.policyFactory()
	lb.Unlock()
}

// Remove removes wire from load balancer
func (lb *SimpleLB) Remove(w *Wire) {
	lb.Lock()
	//w.Policy = nil
	delete(lb.wires, w)
	lb.Unstick(w)
	lb.Unlock()
}

func (lb *SimpleLB) AllDone(msg Envelope, err error) error {
	return err
}

func (lb *SimpleLB) Use(wire *Wire, msg Envelope) Wirer {
	var loaded = wire.Policy.Borrow(msg.Name, func(name string) Comparer {
		// initialization
		return lb.findMinLoad(msg.Name)
	})
	if loaded == nil {
		return nil
	} else {
		return &Wired{wire, loaded}
	}
}

func (lb *SimpleLB) Done(wirer Wirer, msg Envelope, err error) {
	var w = wirer.(*Wired)
	w.wire.Policy.Return(msg.Name, w.loader, err)
	if err != nil {
		lb.Lock()
		lb.Unstick(w.wire)
		lb.Unlock()
	}
}

func (lb *SimpleLB) PickAll(msg Envelope, wires []*Wire) ([]*Wire, error) {

	// NOTE: for messages of type PUB and REQALL the sticker does not make sense
	type loadedWire struct {
		load Comparer
		wire *Wire
	}

	// retrive all loads
	var loads = make([]loadedWire, 0, len(wires))
	for _, w := range wires {
		if !w.Policy.Quarantined(msg.Name) {
			var data = w.Policy.Load(msg.Name)
			loads = append(loads, loadedWire{data, w})
		}
	}

	if len(loads) == 0 {
		return nil, ServiceUnavailableError(fmt.Errorf(UNAVAILABLESERVICE, msg.Name))
	}

	// sort by load
	sort.Slice(loads, func(i, j int) bool {
		return loads[i].load.Compare(loads[j].load) < 0
	})

	// slice with just the wires
	var ws = make([]*Wire, len(loads))
	for k, l := range loads {
		ws[k] = l.wire
	}

	return ws, nil
}

func (lb *SimpleLB) PickOne(msg Envelope, wires []*Wire) (*Wire, error) {
	// get valid wires
	var valid, err = lb.PickAll(msg, wires)
	if err != nil {
		return nil, err
	}

	lb.Lock()
	var wire, sticker = lb.IsSticky(msg.Name, valid)
	lb.Unlock()
	if wire != nil {
		return wire, nil
	}

	// since valid is sorted, the first one has the lowest load
	var minw = valid[0]

	if sticker != nil {
		sticker.lastWire = minw
	}
	return minw, nil
}

func (lb *SimpleLB) findMinLoad(name string) Comparer {
	lb.Lock()
	defer lb.Unlock()

	// look for the smallest load
	var minload Comparer
	for k := range lb.wires {
		if k.Policy != nil {
			var load = k.Policy.Load(name)
			if minload == nil || load.Compare(minload) < 0 {
				minload = load
			}
		}
	}
	return minload
}
