package gomsg

import (
	"strings"
	"time"
)

const FILTER_TOKEN = "*"

type Sticky struct {
	duration time.Duration
	timeout  time.Time
	lastWire *Wire
}

type Stickies map[string]*Sticky

// Stick forces the messages to go to the same wire (in a multi wire cenario)
// if the time between messages is smaller than the duration argument.
func (st Stickies) Stick(name string, duration time.Duration) {
	if duration == time.Duration(0) {
		delete(st, name)
	} else {
		st[name] = &Sticky{duration: duration}
	}
}

func (st Stickies) findSticky(name string) *Sticky {
	// find key filter that intercepts name
	for s, v := range st {
		if strings.HasSuffix(s, FILTER_TOKEN) {
			if strings.HasPrefix(name, s[:len(s)-1]) {
				return v
			}
		} else if name == s {
			return v
		}
	}
	return nil
}

func (st Stickies) IsSticky(name string, wires []*Wire) (*Wire, *Sticky) {
	if len(wires) == 0 {
		return nil, nil
	}

	// find sticky filter that intercepts name
	var stick = st.findSticky(name)
	if stick == nil {
		return nil, nil
	}

	var now = time.Now()
	var timeout = stick.timeout == time.Time{} || now.After(stick.timeout)
	// for the next call
	stick.timeout = now.Add(stick.duration)

	if timeout {
		return nil, stick
	}

	// find last used wire
	for _, v := range wires {
		if v == stick.lastWire {
			var load = v.Load.(*Load)
			if now.After(load.quarantineUntil) {
				return v, stick
			} else {
				break
			}
		}
	}
	// if we are here, the lastWire is invalid
	stick.lastWire = nil

	return nil, stick
}

func (st Stickies) Unstick(w *Wire) {
	// reset all lastWires that are the same as the one being removed
	for _, v := range st {
		if w == v.lastWire {
			v.timeout = time.Time{}
			v.lastWire = nil
		}
	}
}
