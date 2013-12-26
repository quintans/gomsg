package gobus

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"
)

const UINT8_SIZE uint8 = 255
const UINT16_SIZE uint16 = 65535
const UINT32_SIZE uint32 = 4294967295

type GobCodec struct {
}

func (this GobCodec) Encode(data interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (this GobCodec) Decode(payload []byte, p interface{}) error {
	var buf bytes.Buffer
	buf.Write(payload)
	dec := gob.NewDecoder(&buf)
	err := dec.Decode(p)
	if err != nil {
		return err
	}
	return nil
}

type JsonCodec struct {
}

func (this JsonCodec) Encode(data interface{}) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (this JsonCodec) Decode(payload []byte, p interface{}) error {
	err := json.Unmarshal(payload, p)
	if err != nil {
		return err
	}
	return nil
}

type Faults []Fault

func (this Faults) Error() string {
	var buffer bytes.Buffer
	for _, fault := range this {
		buffer.WriteString(fault.Error() + "\n")
	}
	return buffer.String()
}

type Fault struct {
	Code    string
	Message string
}

func (this Fault) Error() string {
	return fmt.Sprintf("%s: %s", this.Code, this.Message)
}

func NewFaultError(err error) Fault {
	return Fault{
		Code:    "ERROR",
		Message: err.Error(),
	}
}

func NewFault(code string, msg string) Fault {
	return Fault{
		Code:    code,
		Message: msg,
	}
}

var DeadServiceError = errors.New("The service is dead!")
var RequestTimeoutError = errors.New("Timeout ocurred for the response")

type Wire struct {
	mu           sync.Mutex
	Server       IServer
	emit         chan *Communication
	receive      chan []byte
	incoming     chan *Communication
	outgoing     chan *Communication
	Connection   net.Conn
	callbacks    map[uint32]*callback
	timeoutcb    chan uint32
	callerId     uint32
	ReadFailure  func(error)
	OnDisconnect func()
	kill         chan bool
}

var BUFFER_IN = make([]byte, int(UINT16_SIZE))
var BUFFER_OUT = make([]byte, int(UINT16_SIZE))

func (this *Wire) read(connection net.Conn) (err error) {
	defer func() {
		if err != nil {
			if this.ReadFailure != nil {
				this.ReadFailure(err)
			}
			this.Stop()
		}
	}()

	reader := NewCustomReader(bufio.NewReader(connection))

	for {
		msg := &Communication{}
		// reads Communication kind: request, reply, none
		var k uint8
		k, err = reader.ReadUI8()
		if err != nil {
			return
		}
		msg.Kind = MsgKind(k)
		// reads Communication id
		msg.Id, err = reader.ReadUI32()
		if err != nil {
			return
		}
		//logger.Debugf("===> incoming id %v", msg.Id)
		// reads Communication timestamp, in milliseconds
		var msecs uint64
		msecs, err = reader.ReadUI64()
		if err != nil {
			return
		}
		secs := msecs / 1e3
		nsecs := (msecs % 1e3) * 1e6
		msg.Timestamp = time.Unix(int64(secs), int64(nsecs))

		// reads Communication name
		msg.Name, err = reader.ReadString()
		if err != nil {
			return
		}
		// reads Communication header
		msg.Header, err = reader.ReadBytes()
		if err != nil {
			return
		}
		// reads Communication data
		msg.Data, err = reader.ReadBytes()
		if err != nil {
			return
		}

		this.incoming <- msg
	}
}

func (this *Wire) write(connection net.Conn) {
	w := bufio.NewWriter(connection)
	writer := NewCustomWriter(w)
	for outgoing := range this.emit {
		// kind
		err := writer.WriteUI8(uint8(outgoing.Kind))
		if fail(this, err, outgoing) {
			return
		}
		// Communication id
		err = writer.WriteUI32(outgoing.Id)
		if fail(this, err, outgoing) {
			return
		}
		// Communication timestamp, in milliseconds
		msecs := outgoing.Timestamp.UnixNano() / 1e6
		err = writer.WriteUI64(uint64(msecs))
		if fail(this, err, outgoing) {
			return
		}

		// Communication name size
		err = writer.WriteString(outgoing.Name)
		if fail(this, err, outgoing) {
			return
		}
		// Communication header
		err = writer.WriteBytes(outgoing.Header)
		if fail(this, err, outgoing) {
			return
		}
		// Communication data
		err = writer.WriteBytes(outgoing.Data)
		if fail(this, err, outgoing) {
			return
		}
		err = w.Flush()
		if fail(this, err, outgoing) {
			return
		}
	}
}

func fail(wire *Wire, err error, comm *Communication) bool {
	if err != nil {
		logger.Errorf("%s", err.Error())
		if comm.callback != nil && comm.callback.localFailure != nil {
			comm.callback.localFailure(err)
		}
		wire.Stop()
		return true
	}
	return false
}

func (this *Wire) handleIOs() {
	//tick := time.Tick(10 * time.Minute)
	for {
		select {
		case id := <-this.timeoutcb:
			cb, ok := this.callbacks[id]
			if ok {
				logger.Debugf("timeout for request id %v", id)
				delete(this.callbacks, id)
				if cb.localFailure != nil {
					cb.localFailure(RequestTimeoutError)
				}
			}

		case msg := <-this.incoming:
			switch msg.Kind {
			case REPLY, REPLY_PARTIAL, ERROR, ERROR_PARTIAL:
				logger.Debugf("%s for Id %v: %s", msg.Kind, msg.Id, msg.Data)

				// if *_PARTIAL it not be removed from the callbacks
				if callback, ok := this.callbacks[msg.Id]; ok {
					if msg.Kind == REPLY || msg.Kind == ERROR {
						callback.timer.Stop()
						delete(this.callbacks, msg.Id)
					}

					if msg.Kind == ERROR || msg.Kind == ERROR_PARTIAL {
						callback.remoteFailure(Payload{msg.Kind, msg.Data})
					} else {
						callback.success(Payload{msg.Kind, msg.Data})
					}
				}

			case REQUEST, NONE:
				logger.Debugf("%s for '%s' (Id=%v): %s", msg.Kind, msg.Name, msg.Id, msg.Data)
				go func(msg *Communication) {
					head := new(bytes.Buffer)
					body := new(bytes.Buffer)
					resp := &Response{
						Head: head,
						Body: body,
					}
					resp.Flush = func() {
						this.sendSync(&Communication{
							Kind:      resp.Kind, // can be reply or error
							Id:        msg.Id,    // in a reply the same id as the request is used
							Timestamp: time.Now(),
							Header:    head.Bytes(),
							Data:      body.Bytes(),
						})
						head.Reset()
						body.Reset()
					}
					// applies endpoint rules
					if this.Server != nil {
						msg.Emiter = this
						err := this.Server.ServeStream(resp, msg)
						if err != nil {
							logger.Errorf("(Wire): %s", err.Error())
						}
					}

					if msg.Kind == REQUEST {
						resp.Flush()
					}
				}(msg)
			}

		case outgoing := <-this.outgoing:
			this.sendSync(outgoing)

		case <-this.kill:
			if this.OnDisconnect != nil {
				this.OnDisconnect()
			}
			// close all channels
			close(this.emit) // terminates writer go routine
			// clean up
			this.callbacks = nil
			conn := this.Connection
			this.Connection = nil
			conn.Close() // terminates reader go routine
			return       // terminates this go routine
		}
	}
}

// called from handleIOs()
func (this *Wire) sendSync(comm *Communication) {
	// if it is a request, it is necessary
	// an id for an eventual reply, if comm.callback != nil
	if comm.Kind == REQUEST && comm.callback != nil {
		this.callerId++
		comm.Id = this.callerId
		id := this.callerId
		this.callbacks[id] = comm.callback
		logger.Debugf("setting timeout for request id %v. topic: %s", id, comm.Name)
		comm.callback.timer = time.AfterFunc(15*time.Second, func() {
			this.timeoutcb <- id
		})

		// circular. zero is reserved for requests, therefore is not used
		if this.callerId == UINT32_SIZE {
			this.callerId = 0
		}
	}

	this.emit <- comm
}

func NewWire(conn net.Conn, server IServer) *Wire {
	this := new(Wire)
	this.Server = server
	this.emit = make(chan *Communication)
	this.receive = make(chan []byte)
	this.incoming = make(chan *Communication)
	this.outgoing = make(chan *Communication)
	this.timeoutcb = make(chan uint32)
	this.kill = make(chan bool)
	this.callbacks = make(map[uint32]*callback)
	this.Connection = conn

	go this.handleIOs()
	go this.read(conn)
	go this.write(conn)

	return this
}

func (this *Wire) Stop() {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.kill <- true
}

// sends data to the connection and the response is returned to the passed function
func (this *Wire) Send(name string, data []byte, success func(Payload), remoteFailure func(Payload), localFailure func(error)) error {
	// check size of data
	size := len(data)
	if size > int(UINT16_SIZE) {
		return errors.New(fmt.Sprintf("Data size cannot be bigger than %v", UINT16_SIZE))
	}
	namesz := len(name)
	if namesz > int(UINT8_SIZE) {
		return errors.New(fmt.Sprintf("Name size cannot be bigger than %v", UINT8_SIZE))
	}

	this.mu.Lock()
	defer this.mu.Unlock()

	if this.Connection == nil {
		return DeadServiceError
	} else {
		k := NONE
		// registers the call and the reply timeout
		var call *callback
		if success != nil || remoteFailure != nil {
			call = &callback{
				success:       success,
				remoteFailure: remoteFailure,
				localFailure:  localFailure,
			}
			k = REQUEST
		}

		comm := &Communication{
			Kind:      k,
			Timestamp: time.Now(),
			Name:      name,
			Data:      data,
			callback:  call,
		}

		this.outgoing <- comm
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

			// reply is the final reply
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
	err = this.Send(
		name,
		data,
		successHandler,
		failureHandler,
		failure,
	)

	return err
}
