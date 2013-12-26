package gobus

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"
)

const (
	REDIAL_FIRST_INTERVAL = 1
	REDIAL_MAX_INTERVAL   = 15
)

type Codec interface {
	Encode(interface{}) ([]byte, error)
	Decode([]byte, interface{}) error
}

type Client struct {
	mu           sync.Mutex
	wire         *Wire
	Protocol     string
	Endpoint     string
	codec        Codec
	dialer       chan bool
	filter       *FilterHandler
	registry     map[string]string
	OnDisconnect func()
	OnConnect    func()
}

func NewClient(protocol string, endpoint string, codec Codec) *Client {
	this := new(Client)
	this.Protocol = protocol
	this.Endpoint = endpoint
	if codec == nil {
		this.codec = JsonCodec{}
	} else {
		this.codec = codec
	}
	this.dialer = make(chan bool)
	this.filter = new(FilterHandler)
	this.registry = make(map[string]string)

	go this.handleConn()

	this.dialer <- true

	return this
}

func (this *Client) GetConnection() net.Conn {
	return this.wire.Connection
}

func (this *Client) Stop() {
	this.dialer <- false
}

func (this *Client) handleConn() {
	pause := REDIAL_FIRST_INTERVAL
	dialing := false
	stop := false
	for {
		select {
		case dial := <-this.dialer:
			if dial {
				// if already invalidated, return
				if stop {
					return
				}
				conn, err := net.Dial(this.Protocol, this.Endpoint)
				if err != nil {
					// try again later
					dialing = true
					go this.redial(pause)
					if pause < REDIAL_MAX_INTERVAL {
						pause = 2 * pause
					}
				} else {
					dialing = false
					this.wire = NewWire(conn, this.filter)
					// unscheduled disconnect
					// will start a timer to periodically check if the connection has become alive
					this.wire.OnDisconnect = func() {
						this.mu.Lock()
						defer this.mu.Unlock()
						if this.OnDisconnect != nil {
							this.OnDisconnect()
						}
						this.wire = nil
						if !stop {
							// try again later
							dialing = true
							go this.redial(REDIAL_FIRST_INTERVAL)
							pause = 2 * REDIAL_FIRST_INTERVAL
						}
					}
					this.sendTopics(nil)
					if this.OnConnect != nil {
						this.OnConnect()
					}

				}
			} else {
				stop = true
				if this.wire != nil {
					this.wire.Stop()
				}
				this.wire = nil
				// if there is not a dialing event waiting to trigger
				if !dialing {
					return
				}
			}
		}
	}
}

func (this *Client) redial(pause int) {
	logger.Debugf("redial in %v seconds", pause)
	time.After(time.Duration(pause) * time.Second)
	this.dialer <- true
}

func (this *Client) Publish(topic string, message interface{}, failure func(error)) error {
	return this.send(PUB_CMD+topic, message, failure)
}

func (this *Client) sendTopics(failure func(error)) error {
	subs := make([]string, 0)
	for k, _ := range this.registry {
		subs = append(subs, k)
	}
	return this.send(CMD_REGS, subs, failure)
}

// publish a message to a topic. The failure handler, is for local errors
func (this *Client) send(endpoint string, message interface{}, failure func(error)) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.wire != nil {
		var data []byte
		var err error
		if message != nil {
			data, err = this.codec.Encode(message)
			if err != nil {
				return err
			}
		}
		err = this.wire.Send(endpoint, data, nil, nil, failure)
		if err != nil {
			return err
		}
	} else {
		return DeadServiceError
	}
	return nil
}

var (
	errorType   = reflect.TypeOf((*error)(nil)).Elem()    // interface type
	contextType = reflect.TypeOf((*IContext)(nil)).Elem() // interface type
)

// subscribes a topic
func (this *Client) Subscribe(topic string, handler interface{}) error {
	return this.register(topic, handler, false, nil)
}

// replies to requests
func (this *Client) ReplyAllways(name string, handler interface{}) error {
	return this.register(name, handler, true, func() bool { return true })
}

// replies to requests
func (this *Client) Reply(name string, handler interface{}, accept func() bool) error {
	return this.register(name, handler, true, accept)
}

func (this *Client) register(topic string, handler interface{}, returnable bool, accept func() bool) error {
	namesz := len(topic)
	if namesz > int(UINT8_SIZE) {
		return errors.New(fmt.Sprintf("Name topic size is bigger than %v", UINT8_SIZE))
	}

	filter, err := MakeFilter(returnable, handler, this.codec)
	if err != nil {
		return err
	}
	this.filter.Push(topic, filter)

	if accept != nil {
		this.filter.Push(ASK_CMD+topic, func(ctx IContext) error {
			r := ctx.GetResponse()
			r.Kind = REPLY
			ok := accept()
			data, err := this.codec.Encode(ok)
			if err != nil {
				return err
			}
			r.Body.Write(data)
			return nil
		})
	}

	this.mu.Lock()
	defer this.mu.Unlock()
	// cache topic. this will be asked upon server restart
	this.registry[topic] = topic
	if this.wire != nil {
		// send to the server the topic subscription
		return this.wire.Send(REG_CMD+topic, nil, nil, nil, func(fault error) {
			logger.Errorf("%s", fault.Error())
		})
	}

	return nil
}

func (this *Client) CallOne(name string, message interface{}, success interface{}, failure func(error)) error {
	if this.wire == nil {
		return DeadServiceError
	}
	return this.wire.Call(REQONE_CMD+name, message, success, failure, this.codec, false)
}

func (this *Client) CallMany(name string, message interface{}, success interface{}, failure func(error)) error {
	if this.wire == nil {
		return DeadServiceError
	}
	return this.wire.Call(REQALL_CMD+name, message, success, failure, this.codec, true)
}
