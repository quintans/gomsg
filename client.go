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
	registry     map[string]bool
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
	this.registry = make(map[string]bool)

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

// Message will be delivered to ALL subscribers
func (this *Client) Publish(topic string, message interface{}, failure func(error)) error {
	return this.send(ACT_PUBALL, topic, message, failure)
}

// Message will be delivered to only ONE subscriber
func (this *Client) Queue(topic string, message interface{}, failure func(error)) error {
	return this.send(ACT_PUBONE, topic, message, failure)
}

func (this *Client) sendTopics(failure func(error)) error {
	subs := make([]string, 0)
	for k, _ := range this.registry {
		subs = append(subs, k)
	}
	return this.send(ACT_REGS, "", subs, failure)
}

// publish a message to a topic. The failure handler, is for local errors
func (this *Client) send(action Action, endpoint string, message interface{}, failure func(error)) error {
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
		err = this.wire.Send(action, endpoint, data, nil, nil, failure)
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
	if this.registry[topic] {
		return errors.New(fmt.Sprintf("Name topic '%s' size is already registered", topic))
	}

	namesz := len(topic)
	if namesz > int(UINT8_SIZE) {
		return errors.New(fmt.Sprintf("Name topic size is bigger than %v", UINT8_SIZE))
	}

	filter, err := MakeFilter(returnable, handler, this.codec)
	if err != nil {
		return err
	}

	if accept != nil {
		this.filter.Push(topic, func(ctx IContext) error {
			if ctx.GetRequest().Action == ACT_ASK {
				r := ctx.GetResponse()
				r.Kind = REPLY
				ok := accept()
				data, err := this.codec.Encode(ok)
				if err != nil {
					return err
				}
				r.Body.Write(data)
				return nil
			} else {
				return filter(ctx)
			}
		})
	}

	this.mu.Lock()
	defer this.mu.Unlock()
	// cache topic. this will be asked upon server restart
	this.registry[topic] = true
	if this.wire != nil {
		// send to the server the topic subscription
		return this.wire.Send(ACT_REG, topic, nil, nil, nil, func(fault error) {
			if fault != nil {
				logger.Errorf("%s", fault.Error())
			}
		})
	}

	return nil
}

// calls a endpoint at the other end point. If there is no success handler, then a reply is not expected.
// if the multiple flag is set, then the success handler, if not nil, must have a slice as a parameter
func (this *Wire) Call(name string, message interface{}, success interface{}, failure func(error), codec Codec, multiReply bool) error {
	var successHandler func(Payload)
	var failureHandler func(Payload)

	if success != nil {
		var returnValue reflect.Value
		var returnType reflect.Type
		function := reflect.ValueOf(success)
		typ := function.Type()
		size := typ.NumIn()
		if size > 1 {
			return errors.New("success function must have at the most one parameter.")
		} else if size == 1 {
			returnType = typ.In(0)
		}

		if multiReply {
			if size != 1 || returnType.Kind() != reflect.Slice {
				return errors.New("When a multiple reply, the success function must have one parameter and it must be a slice.")
			} else {
				// instanciates the slice
				returnValue = reflect.New(returnType).Elem()
				// sets the return type to the inner type of the array
				returnType = returnType.Elem()
			}
		}

		successHandler = func(payload Payload) {
			var p reflect.Value
			if returnType != nil {
				p = reflect.New(returnType)
				codec.Decode(payload.Data, p.Interface())
				if multiReply {
					returnValue = reflect.Append(returnValue, p.Elem())
				} else {
					returnValue = p.Elem()
				}
			}

			// REPLY kind is the final reply
			if payload.Kind == REPLY {
				params := make([]reflect.Value, 0)
				if returnType != nil {
					params = append(params, returnValue)
				}
				function.Call(params)
			}
		}
	}

	if failure != nil {
		var faults Faults
		var err error
		failureHandler = func(payload Payload) {
			fault := Fault{}
			codec.Decode(payload.Data, &fault)
			if multiReply {
				faults = append(faults, fault)
				err = faults
			} else {
				err = fault
			}
			if payload.Kind == ERROR {
				failure(err)
			}
		}
	}

	var data []byte
	var err error
	if message != nil {
		data, err = codec.Encode(message)
		if err != nil {
			return err
		}
	}

	var action Action
	if multiReply {
		action = ACT_REQALL
	} else {
		action = ACT_REQONE
	}

	err = this.Send(
		action,
		name,
		data,
		successHandler,
		failureHandler,
		failure,
	)

	return err
}

func (this *Client) CallOne(name string, message interface{}, success interface{}, failure func(error)) error {
	if this.wire == nil {
		return DeadServiceError
	}
	return this.wire.Call(name, message, success, failure, this.codec, false)
}

func (this *Client) CallMany(name string, message interface{}, success interface{}, failure func(error)) error {
	if this.wire == nil {
		return DeadServiceError
	}
	return this.wire.Call(name, message, success, failure, this.codec, true)
}
