package gomsg

import (
	"errors"
	"net"
	"time"
)

func ExternalIP() (string, error) {
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

type PollItem struct {
	Index int
	Error interface{}
}

// from an array of channels returning the first to complete
func Poll(chans []chan interface{}, timeout time.Duration) PollItem {
	out := make(chan PollItem, len(chans)+1)
	go func() {
		time.Sleep(timeout)
		out <- PollItem{-1, nil}
	}()
	for k, v := range chans {
		go func(i int, errch chan interface{}) {
			select {
			case <-time.After(timeout):
			case e := <-errch:
				out <- PollItem{i, e}
			}
		}(k, v)
	}
	// only the first matters
	return <-out
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
