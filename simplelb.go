package gomsg

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

var _ LBPolicy = &HysteresisPolicy{}

type HysteresisPolicy struct {
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
}

func (this *HysteresisPolicy) getData(name string) *HysteresisPolicyData {
	var data = this.data[name]
	if data == nil {
		data = &HysteresisPolicyData{}
		this.data[name] = data
	}
	return data
}

func (this *HysteresisPolicy) IncLoad(name string) uint64 {
	var data = this.getData(name)
	data.load++

	return data.load
}

func (this *HysteresisPolicy) Load(name string) uint64 {
	return this.getData(name).load
}

func (this *HysteresisPolicy) Failed(name string) {
	var data = this.getData(name)
	if data.failures >= this.MaxFailures {
		// OPEN
		data.successes = 0
		data.quarantineUntil = time.Now().Add(this.Quarantine)
		return
	}

	data.successes = 0
	data.failures++
}

func (this *HysteresisPolicy) Succeeded(name string) {
	var data = this.getData(name)
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

func (this *HysteresisPolicy) InQuarantine(name string) bool {
	var data = this.getData(name)
	return time.Now().Before(data.quarantineUntil)
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
				data:        make(map[string]*HysteresisPolicyData),
			}
		},
	}
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
	lb.Unstick(w)
	lb.Unlock()
}

func (lb SimpleLB) AllDone(msg Envelope, err error) error {
	return err
}

func (lb SimpleLB) Done(w *Wire, msg Envelope, err error) {
	if err == nil {
		w.Policy.Succeeded(msg.Name)
		// when kind is REQALL or PUB, the PickAll method is used.
		// That method does not increase the load.
		// Since in REQALL and PUB we might only use one connection (groups),
		// we incease the load on delivery
		if test(msg.Kind, REQALL, PUB) {
			w.Policy.IncLoad(msg.Name)
		}
	} else {
		w.Policy.Failed(msg.Name)

		lb.Lock()
		lb.Unstick(w)
		lb.Unlock()
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
		wire.Policy.IncLoad(msg.Name)
		return wire, nil
	}

	// since valid is sorted, the first one has the lowest load
	var minw = valid[0]

	if sticker != nil {
		sticker.lastWire = minw
	}
	// prepare
	minw.Policy.IncLoad(msg.Name)
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
