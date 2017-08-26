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

func (l loader) IsZero() bool {
	return l.load == 0
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

type HysteresisPolicy struct {
	sync.RWMutex
	data         map[string]*HysteresisPolicyData
	MaxFailures  int
	MinSuccesses int
	Quarantine   time.Duration
}

type HysteresisPolicyData struct {
	failures        int
	successes       int
	quarantineUntil time.Time
	load            uint64
	quarantine      bool

	borrowed    map[uint64]loader
	borrowedIdx uint64
}

func (this *HysteresisPolicy) Init(name string, initial Comparer) Loader {
	this.Lock()
	defer this.Unlock()

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

func (this *HysteresisPolicy) Borrow(name string) Loader {
	this.Lock()
	defer this.Unlock()

	var borrow loader
	var data = this.data[name]
	if data == nil {
		return loader{}
	} else {
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

func (this *HysteresisPolicy) Return(name string, loaded Loader, err error) {
	this.Lock()
	defer this.Unlock()

	var load = loaded.(loader)
	var data = this.data[name]
	if data != nil {
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
	if data.failures >= this.MaxFailures {
		// OPEN
		data.successes = 0
		data.quarantineUntil = time.Now().Add(this.Quarantine)
		return true
	}

	data.successes = 0
	data.failures++
	return false
}

func (this *HysteresisPolicy) succeeded(data *HysteresisPolicyData) {
	if data.successes >= this.MinSuccesses {
		// CLOSE
		data.failures = 0
		return
	}
	if data.failures < this.MaxFailures {
		// not OPEN
		data.failures = 0
	}
	data.successes++
}

func (this *HysteresisPolicy) Quarantined(name string) bool {
	this.RLock()
	defer this.RUnlock()

	var data = this.data[name]
	if data == nil {
		// if it has no data is new
		return false
	} else {
		return time.Now().Before(data.quarantineUntil)

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

var _ LoadBalancer = SimpleLB{}

type SimpleLB struct {
	sync.RWMutex
	Stickies
	policyFactory func() LBPolicy
	wires         map[*Wire]bool
}

func NewSimpleLB() SimpleLB {
	return SimpleLB{
		Stickies: make(map[string]*Sticky),
		wires:    make(map[*Wire]bool),
		policyFactory: func() LBPolicy {
			return &HysteresisPolicy{
				Quarantine:  time.Second * 10,
				MaxFailures: 3,
				data:        make(map[string]*HysteresisPolicyData),
			}
		},
	}
}

type Wired struct {
	wire   *Wire
	loader Loader
}

func (this *Wired) Wire() *Wire {
	return this.wire
}

func (lb SimpleLB) SetPolicyFactory(factory func() LBPolicy) {
	lb.policyFactory = factory
}

// Add adds wire to load balancer
func (lb SimpleLB) Add(w *Wire) {
	lb.Lock()
	w.Policy = lb.policyFactory()
	lb.Unlock()
}

// Remove removes wire from load balancer
func (lb SimpleLB) Remove(w *Wire) {
	lb.Lock()
	//w.Policy = nil
	delete(lb.wires, w)
	lb.Unstick(w)
	lb.Unlock()
}

func (lb SimpleLB) AllDone(msg Envelope, err error) error {
	return err
}

func (lb SimpleLB) Use(wire *Wire, msg Envelope) Wirer {
	var loaded = wire.Policy.Borrow(msg.Name)
	if loaded.IsZero() {
		// initialization
		var load = lb.findMinLoad(msg.Name)
		loaded = wire.Policy.Init(msg.Name, load)
	}
	return &Wired{wire, loaded}
}

func (lb SimpleLB) Done(wirer Wirer, msg Envelope, err error) {
	var w = wirer.(*Wired)
	w.wire.Policy.Return(msg.Name, w.loader, err)
	if err != nil {
		lb.Lock()
		lb.Unstick(w.wire)
		lb.Unlock()
	}
}

func (lb SimpleLB) PickAll(msg Envelope, wires []*Wire) ([]*Wire, error) {

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
		return wire, nil
	}

	// since valid is sorted, the first one has the lowest load
	var minw = valid[0]

	if sticker != nil {
		sticker.lastWire = minw
	}
	return minw, nil
}

func (lb SimpleLB) findMinLoad(name string) Comparer {
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
