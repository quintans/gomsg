package gomsg

import (
	"crypto/rand"
	"errors"
	"net"
	"sync"
	"time"
)

func IP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String(), nil
		}
	}
	return "", errors.New("are you connected to the network?")
}

type Looper struct {
	last   int
	offset int
	cursor int
	max    int
}

func NewLooper(start int, max int) *Looper {
	if start >= max {
		start = 0
	}

	return &Looper{
		last:   start,
		offset: start,
		cursor: 0,
		max:    max,
	}
}

func (this *Looper) Next() int {
	current := this.cursor
	if this.HasNext() {
		if this.cursor+this.offset >= this.max {
			this.offset = this.offset - this.max
		}
		this.cursor++
	}

	this.last = current + this.offset
	return this.last
}

func (this *Looper) Last() int {
	return this.last
}

func (this *Looper) HasNext() bool {
	return this.cursor < this.max
}

// Timeout is a timer over a generic element, that will call a function when a specified timeout occurs.
// It is possible to delay the timeout.
type Timeout struct {
	mu       sync.RWMutex
	what     map[interface{}]time.Time
	ticker   *time.Ticker
	duration time.Duration
	handle   uint32
}

// NewTimeout create a timeout
func NewTimeout(tick time.Duration, duration time.Duration, expired func(o interface{})) *Timeout {
	this := new(Timeout)
	this.what = make(map[interface{}]time.Time)
	this.duration = duration
	this.ticker = time.NewTicker(tick)
	go func() {
		for _ = range this.ticker.C {
			this.purge(expired)
		}
	}()
	return this
}

func (timeout *Timeout) purge(expired func(o interface{})) {
	timeout.mu.Lock()
	defer timeout.mu.Unlock()

	now := time.Now()
	for k, v := range timeout.what {
		if v.Before(now) {
			expired(k)
			delete(timeout.what, k)
		}
	}
}

// Delay delays the timeout occurence.
// If this is the first time it is called, only from now the timeout will occur.
func (timeout *Timeout) Delay(o interface{}) {
	timeout.mu.Lock()
	defer timeout.mu.Unlock()

	timeout.what[o] = time.Now().Add(timeout.duration)
}

type KeyValueItem struct {
	Key   interface{}
	Value interface{}
}

type KeyValue struct {
	Items []*KeyValueItem
}

func NewKeyValue() *KeyValue {
	return &KeyValue{make([]*KeyValueItem, 0)}
}

func (kv *KeyValue) Put(key interface{}, value interface{}) interface{} {

	// if already defined, replace value and return old one
	for _, v := range kv.Items {
		if v.Key == key {
			var old = v.Value
			v.Value = value
			return old
		}
	}
	var item = &KeyValueItem{key, value}
	kv.Items = append(kv.Items, item)
	return nil
}

func (kv *KeyValue) Get(key interface{}) interface{} {
	return kv.Find(func(item *KeyValueItem) bool {
		return item.Key == key
	})
}

func (kv *KeyValue) Find(fn func(item *KeyValueItem) bool) *KeyValueItem {
	for _, v := range kv.Items {
		if fn(v) {
			return v
		}
	}
	return nil
}

func (kv *KeyValue) Delete(key interface{}) interface{} {
	for k, v := range kv.Items {
		if v.Key == key {
			// since the slice has a non-primitive, we have to zero it
			copy(kv.Items[k:], kv.Items[k+1:])
			kv.Items[len(kv.Items)-1] = nil // zero it
			kv.Items = kv.Items[:len(kv.Items)-1]
			return v.Value
		}
	}
	return nil
}

func NewUUID() []byte {
	var b = make([]byte, 16)
	rand.Read(b)
	return b
}
