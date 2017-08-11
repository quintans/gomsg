package gomsg

// the protocol is:
// PUB, PUSH, REQ, REQALL - |type(1)|sequence(4)|topic(4+n)|payload(4+n)|
// ACK, NACK, REP, ERR, REP_PARTIAL, ERR_PARTIAL - |type(1)|sequence(4)|payload(4+n)|
//
// handler success functions can be func(IContext, any)(any, error). Every argument and return type is optional
// there are three kinds of client outgoing comunications:
// * PUB - a message is sent to all destinations registered to this message and there is no reply
// * PUSH - a message is sent, in turn, to all the destinations returning on the first success. The destinations rotate.
//          There is an acknowledgement from the
// * REQ - Same as PUSH but it returns a payload
// * REQALL - The message is sent to all subscribed endpoints. Only finishes when all endpoints have replyied or a timeout has occurred.
//
// * REP - response in REQ
// * ERR - Error in a REQ
// * REP_PARTIAL - reply in a REQ_ALL
// * ERR_PARTIAL - error in a  REQ_ALL
// * ACK - acknowledges the delivery of a message by PUSH or the End Of Replies
// * NACK - failed to acknowledge the delivery of a message by PUSH (sent by the endpoint)
//
// Sending messages has no buffering. If a destination is not present to consume the message
// when it is sent, then the message will be lost.
// When a client connection is established, the client "wire" sends its topic to the other end (server)
// and receives the remote topics. After this it starts receiving/sending messages.
// Whenever the local endpoint has a new topic it sends that information to the remote endpoint.
//
// Context has ALL the information of the delivered message. It can be used as the only parameter of the handler function. ex: func (ctx IRequest)
// A reply can be sent by using ctx.SetReply([]byte), ctx.SetFault(), ctx.Writer().WriteXXX()
//
// If we send a message, if the type is different of mybus.Msg, then the message will be serialized by using the defined decoder.
// When receiving the message, if the function handler, has a parameter different than IRequest/IResponse it will deserialize the payload using the defined decoder.

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quintans/toolkit"
	"github.com/quintans/toolkit/faults"
)

const (
	_   EKind = iota
	SUB       // indicates that wants to receive messages for a topic
	UNSUB
	REQ
	REQALL
	PUSH
	PUB
	REP         // terminates the REQALL
	ERR         // terminates the REQALL
	REP_PARTIAL // partial reply from a request all
	ERR_PARTIAL // partial error from a request all
	ACK         // Successful delivery of a PUSH message or it is a End Of Replies
	NACK
)

var kind_labels = [...]string{
	"SUB",
	"UNSUB",
	"REQ",
	"REQALL",
	"PUSH",
	"PUB",
	"REP",
	"ERR",
	"REP_PARTIAL",
	"ERR_PARTIAL",
	"ACK",
	"NACK",
}

type EKind uint8

const noGROUP = ""

func (this EKind) String() string {
	idx := int(this)
	if idx > 0 && idx <= len(kind_labels) {
		return kind_labels[idx-1]
	}

	return fmt.Sprint("unknown:", idx)
}

var (
	errorType   = reflect.TypeOf((*error)(nil)).Elem()    // interface type
	replyType   = reflect.TypeOf((*Response)(nil)).Elem() // interface type
	requestType = reflect.TypeOf((**Request)(nil)).Elem() // interface type
)

// errors
var (
	NOCODEC    = errors.New("No codec defined")
	EOR        = errors.New("End Of Multiple Reply")
	NACKERROR  = errors.New("Not Acknowledge Error")
	CLOSEDWIRE = errors.New("Closed Wire")

	UNKNOWNTOPIC = "No registered subscriber for %s."
	TIMEOUT      = "Timeout (%s) while waiting for reply of call #%d %s(%s)=%s"
)

// Specific error types are define so that we can use type assertion, if needed
type UnknownTopic error
type TimeoutError error

func timeouttime(timeout time.Duration) time.Time {
	if timeout == time.Duration(0) {
		return time.Time{}
	} else {
		return time.Now().Add(timeout)
	}
}

// Context is used to pass all the data to replying function if we so wish.
// payload contains the data for incoming messages and reply contains the reply data
// err is used in REPlies in case something went wrong
type Context struct {
	wire     *Wire
	conn     net.Conn
	Kind     EKind
	Name     string
	sequence uint32
}

// Connection getter
func (this *Context) Connection() net.Conn {
	return this.conn
}

type Response struct {
	*Context

	reply []byte
	fault error
}

//var _ IResponse = Response{}

func NewResponse(wire *Wire, c net.Conn, kind EKind, seq uint32, payload []byte) Response {
	r := Response{
		Context: &Context{
			wire:     wire,
			conn:     c,
			Kind:     kind,
			sequence: seq,
		},
	}

	if kind == ERR || kind == ERR_PARTIAL {
		r.fault = errors.New(string(payload))
	}
	r.reply = payload

	return r
}

func (this Response) Reader() *InputStream {
	return NewInputStream(bytes.NewReader(this.reply))
}

func (this Response) Reply() []byte {
	return this.reply
}

func (this Response) Fault() error {
	return this.fault
}

// is the last reply?
func (this Response) Last() bool {
	return this.Kind != REP_PARTIAL && this.Kind != ERR_PARTIAL
}

// is this the end mark?
func (this Response) EndMark() bool {
	return this.Kind == ACK
}

type Middleware func(*Request)

type Request struct {
	Response

	payload   []byte
	writer    *bytes.Buffer
	terminate bool

	// If an handler doesn't define a return type (error, whatever) and define an input of
	// type gomsg.Request the reply will not be sent until we call gomsg.Request.ReplyAs().
	// This is used to route messages (gomsg.Route) from server to server
	deferReply bool

	middleware    []Middleware
	middlewarePos int
}

//var _ IRequest = &Request{}

func NewRequest(wire *Wire, c net.Conn, msg Envelope) *Request {
	ctx := &Request{
		Response: Response{
			Context: &Context{
				wire:     wire,
				conn:     c,
				Kind:     msg.kind,
				Name:     msg.name,
				sequence: msg.sequence,
			},
		},
		payload: msg.payload,
	}

	return ctx
}

// Next calls the next handler
func (this *Request) Next() {
	if this.middlewarePos < len(this.middleware)-1 {
		this.middlewarePos++
		this.middleware[this.middlewarePos](this)
		this.middlewarePos--
	}
}

func (this *Request) reset() {
	this.fault = nil
	this.reply = nil
	this.writer = nil
	this.terminate = false
}

func (this *Request) Payload() []byte {
	return this.payload
}

func (this *Request) Writer() *OutputStream {
	this.writer = new(bytes.Buffer)
	return NewOutputStream(this.writer)
}

// sets a single reply error (ERR)
func (this *Request) SetFault(err error) {
	this.reset()
	this.fault = err
}

// sets a single reply (REQ)
func (this *Request) SetReply(payload []byte) {
	this.reset()
	this.reply = payload
}

func (this *Request) Reply() []byte {
	if this.writer != nil {
		this.reply = this.writer.Bytes()
		this.writer = nil
	}
	return this.reply
}

// DeferReply indicates that the reply won't be sent immediatly.
// The reply will eventually be sent by calling with gomsg.Request.ReplyAs().
func (this *Request) DeferReply() {
	this.deferReply = true
}

// Terminate terminates a series of replies by sending an ACK message to the caller.
// It can also be used to reject a request, since this terminates the request without sending a reply payload.
func (this *Request) Terminate() {
	this.reset()
	this.terminate = true
}

func (this *Request) Terminated() bool {
	return this.terminate
}

// sets a multi reply (REQ_PARTIAL)
func (this *Request) SendReply(reply []byte) {
	this.ReplyAs(REP_PARTIAL, reply)
}

// sets a multi reply error (ERR_PARTIAL)
func (this *Request) SendFault(err error) {
	var reply []byte
	w := this.wire
	if w.codec != nil {
		reply, _ = w.codec.Encode(err.Error())
	} else {
		reply = []byte(err.Error())
	}
	this.ReplyAs(ERR_PARTIAL, reply)
}

func (this *Request) ReplyAs(kind EKind, reply []byte) {
	this.wire.reply(kind, this.sequence, reply)
}

//var _ IContext = &Context{}

type Msg struct {
	*OutputStream
	buffer *bytes.Buffer
}

func NewMsg() *Msg {
	var buffer = new(bytes.Buffer)

	return &Msg{
		NewOutputStream(buffer),
		buffer,
	}
}

type Envelope struct {
	kind     EKind
	sequence uint32
	name     string
	payload  []byte
	handler  func(ctx Response)
	timeout  time.Duration
	errch    chan error
}

func (this Envelope) String() string {
	return fmt.Sprintf("{kind:%s, sequence:%d, name:%s, payload:%s, handler:%p, timeout:%s, errch:%p",
		this.kind, this.sequence, this.name, this.payload, this.handler, this.timeout, this.errch)
}

type EnvelopeConn struct {
	message Envelope
	conn    net.Conn
}

type Wire struct {
	conn      net.Conn
	chin      chan Envelope
	handlerch chan EnvelopeConn

	codec    Codec
	sequence uint32
	timeout  time.Duration

	mucb      sync.RWMutex
	callbacks map[uint32]chan *Response

	disconnected func(net.Conn, error)
	findHandlers func(name string) []handler

	mutop         sync.RWMutex
	remoteGroupID string // it is used for High Availability
	localGroupID  string // it is used for High Availability
	remoteUuid    []byte
	remoteTopics  map[string]bool
	OnSend        SendListener
	OnNewTopic    TopicListener
	OnDropTopic   TopicListener

	// load can be anything defined by the loadbalancer
	load interface{}

	rateLimiter toolkit.Rate
	bufferSize  int
}

func NewWire(codec Codec) *Wire {
	wire := &Wire{
		callbacks:    make(map[uint32]chan *Response),
		timeout:      time.Second * 20,
		codec:        codec,
		remoteTopics: make(map[string]bool),
		bufferSize:   1000,
	}

	return wire
}

func (this *Wire) SetBufferSize(size int) {
	this.bufferSize = size
}

func (this *Wire) SetRateLimiter(limiter toolkit.Rate) {
	this.rateLimiter = limiter
}

// Conn gets the connection
func (this *Wire) Conn() net.Conn {
	return this.conn
}

func (this *Wire) SetConn(c net.Conn) {
	this.mucb.Lock()
	defer this.mucb.Unlock()

	this.stop()
	if c != nil {
		this.conn = c
		this.chin = make(chan Envelope, this.bufferSize)
		this.handlerch = make(chan EnvelopeConn, this.bufferSize)
		go this.writer(c, this.chin)
		go this.reader(c)
		go this.runHandler(this.handlerch)
	}
}

func (this *Wire) stop() {
	if this.conn != nil {
		this.conn.Close() // will also stop the reader()
	}
	this.conn = nil
	if this.chin != nil {
		this.mucb.Lock()
		defer this.mucb.Unlock()

		close(this.chin)
		close(this.handlerch)

		this.chin = nil
		this.handlerch = nil

		this.callbacks = make(map[uint32]chan *Response)
		this.remoteTopics = make(map[string]bool)
	}
}

func (this *Wire) Destroy() {
	this.stop()
	this.OnNewTopic = nil
	this.OnDropTopic = nil
}

func (this *Wire) addRemoteTopic(name string) {
	if this.OnNewTopic != nil {
		this.OnNewTopic(TopicEvent{this.remoteUuid, this.conn.RemoteAddr().String(), name})
	}
	this.mutop.Lock()
	defer this.mutop.Unlock()
	this.remoteTopics[name] = true
}

func (this *Wire) deleteRemoteTopic(name string) {
	this.mutop.Lock()
	delete(this.remoteTopics, name)
	this.mutop.Unlock()

	if this.OnDropTopic != nil {
		this.OnDropTopic(TopicEvent{this.remoteUuid, this.conn.RemoteAddr().String(), name})
	}
}

func (this *Wire) hasRemoteTopic(name string) bool {
	this.mutop.Lock()
	defer this.mutop.Unlock()

	for k := range this.remoteTopics {
		if strings.HasSuffix(k, FILTER_TOKEN) {
			if strings.HasPrefix(name, k[:len(k)-1]) {
				return true
			}
		} else if name == k {
			return true
		}
	}
	return false
}

func (this *Wire) asynchWaitForCallback(msg Envelope) chan *Response {
	this.mucb.Lock()
	defer this.mucb.Unlock()

	// frame channel
	ch := make(chan *Response, 10)
	this.callbacks[msg.sequence] = ch
	// error channel
	go func() {
		for {
			select {
			case <-time.After(msg.timeout):
				this.delCallback(msg.sequence)
				msg.errch <- TimeoutError(faults.New(TIMEOUT, msg.timeout, msg.sequence, msg.kind, msg.name, msg.payload))
				logger.Infof("Timeout for: %s", msg)
				return

			case r := <-ch:
				// if it is nil, it was canceled
				if r == nil {
					return
				}

				if msg.handler != nil {
					msg.handler(*r)
					// only breaks the loop if it is the last one
					// allowing to receive multiple replies
					if r.Last() {
						msg.errch <- nil
						return
					}
				} else {
					if r.Kind == ACK {
						msg.errch <- nil
					} else if r.Kind == NACK {
						msg.errch <- NACKERROR
					}
					return
				}
			}
		}
	}()
	return ch
}

func (this *Wire) getCallback(seq uint32, last bool) chan *Response {
	this.mucb.Lock()
	defer this.mucb.Unlock()

	cb := this.callbacks[seq]
	if last {
		delete(this.callbacks, seq)
	}
	return cb
}

func (this *Wire) delCallback(seq uint32) {
	this.mucb.Lock()
	defer this.mucb.Unlock()

	delete(this.callbacks, seq)
}

func (this *Wire) SetTimeout(timeout time.Duration) {
	this.timeout = timeout
}

func (this *Wire) Send(msg Envelope) <-chan error {
	msg.errch = make(chan error, 1)
	// check first if the peer has the topic before sending.
	// This is done because remote topics are only available after a connection
	if test(msg.kind, PUB, PUSH, REQ, REQALL) && !this.hasRemoteTopic(msg.name) {
		msg.errch <- UnknownTopic(fmt.Errorf(UNKNOWNTOPIC, msg.name))
		return msg.errch
	}

	if this.OnSend != nil {
		this.OnSend(SendEvent{
			Kind:    msg.kind,
			Name:    msg.name,
			Payload: msg.payload,
			Handler: msg.handler,
		})
	}

	return this.justsend(msg)
}

func (this *Wire) justsend(msg Envelope) <-chan error {
	if msg.errch == nil {
		msg.errch = make(chan error, 1)
	}

	// SUB, UNSUB
	msg.sequence = atomic.AddUint32(&this.sequence, 1)
	//msg.sequence = randomSeq()
	this.enqueue(msg)

	return msg.errch
}

func (this *Wire) enqueue(msg Envelope) {
	if this.rateLimiter != nil {
		this.rateLimiter.TakeN(1)
	}
	this.mucb.Lock()
	defer this.mucb.Unlock()

	if this.chin != nil {
		this.chin <- msg
	} else {
		logger.Errorf("Closed connection. Unable to send %s", msg)
		msg.errch <- CLOSEDWIRE
	}
}

func (this *Wire) reply(kind EKind, seq uint32, payload []byte) {
	msg := this.NewReplyEnvelope(kind, seq, payload)
	this.enqueue(msg)
	err := <-msg.errch
	if err != nil {
		this.Destroy()
	}
}

/*
func randomSeq() uint32 {
	b := make([]byte, 4)
	rand.Read(b)
	seq := binary.LittleEndian.Uint32(b)
	return seq
}
*/

func (this *Wire) NewReplyEnvelope(kind EKind, seq uint32, payload []byte) Envelope {
	return Envelope{
		kind:     kind,
		sequence: seq,
		payload:  payload,
		timeout:  time.Second,
		errch:    make(chan error, 1),
	}
}

func (this *Wire) writer(c net.Conn, chin chan Envelope) {
	for msg := range chin {
		logger.Debugf("Writing %s to %s", msg, c.RemoteAddr())
		// writing to the wire can be faster (theoretically) than lauching the asynchWaitForCallback
		// so we first prepare the reception (if any)
		var ch chan *Response
		if test(msg.kind, SUB, UNSUB, REQ, REQALL, PUSH) {
			// wait the reply asynchronously
			ch = this.asynchWaitForCallback(msg)
		}
		var err = this.write(c, msg)
		if err != nil {
			logger.Errorf("Error while writing to %s", c.RemoteAddr())
			if ch != nil {
				ch <- nil // stop waiting for calback
			}
			msg.errch <- err
		} else if ch == nil {
			msg.errch <- nil
		}
	}
	// exit
	this.disconnected(c, nil)
}

func (this *Wire) write(c net.Conn, msg Envelope) (err error) {
	defer func() {
		if err != nil {
			this.disconnected(c, err)
		}
	}()

	c.SetWriteDeadline(timeouttime(msg.timeout))

	buf := bufio.NewWriter(c)
	w := NewOutputStream(buf)

	// kind
	err = w.WriteUI8(uint8(msg.kind))
	if err != nil {
		return
	}
	// channel sequence
	err = w.WriteUI32(msg.sequence)
	if err != nil {
		return
	}

	if test(msg.kind, PUB, PUSH, SUB, UNSUB, REQ, REQALL) {
		// topic
		err = w.WriteString(msg.name)
		if err != nil {
			return
		}
	}
	if test(msg.kind, REQ, REQALL, PUB, PUSH, REP, REP_PARTIAL, ERR, ERR_PARTIAL) {
		// payload
		err = w.WriteBytes(msg.payload)
		if err != nil {
			return
		}
	}

	return buf.Flush()
}

func read(r *InputStream, bname bool, bpayload bool) (name string, payload []byte, err error) {
	if bname {
		name, err = r.ReadString()
		if err != nil {
			return
		}
	}
	if bpayload {
		payload, err = r.ReadBytes()
		if err != nil {
			return
		}
	}
	return
}

func (this *Wire) reader(c net.Conn) {
	r := NewInputStream(c)
	var err error

loop:
	for {
		// we wait forever for the first read
		c.SetReadDeadline(time.Time{})

		// kind
		var k uint8
		k, err = r.ReadUI8()
		if err != nil {
			break
		}
		var kind = EKind(k)

		// after the first read we apply the timeout
		c.SetWriteDeadline(timeouttime(this.timeout))

		// sequence
		var seq uint32
		seq, err = r.ReadUI32()
		if err != nil {
			break
		}

		var name string
		var data []byte

		if test(kind, SUB, UNSUB, REQ, REQALL, PUSH, PUB) {
			if test(kind, SUB, UNSUB) {
				// un/register topic from peer
				name, _, err = read(r, true, false)
				if err != nil {
					break
				}
				if kind == SUB {
					this.addRemoteTopic(name)
				} else {
					this.deleteRemoteTopic(name)
				}
				msg := this.NewReplyEnvelope(ACK, seq, nil)
				this.enqueue(msg)
				err = <-msg.errch
				if err != nil {
					break
				}

			} else {
				name, data, err = read(r, true, true)
				if err != nil {
					break
				}

				this.handlerch <- EnvelopeConn{
					message: Envelope{
						kind:     kind,
						sequence: seq,
						name:     name,
						payload:  data,
					},
					conn: c,
				}
			}
		} else {
			last := true
			switch kind {
			case REP_PARTIAL, REP:
				_, data, err = read(r, false, true)
				if err != nil {
					break loop
				}
				last = kind == REP

			case ERR, ERR_PARTIAL:
				_, data, err = read(r, false, true)
				if err != nil {
					break loop
				}
				// reply was an error
				var s string
				if this.codec != nil {
					var e = this.codec.Decode(data, &s)
					if e != nil {
						logger.Errorf("Unable to decode %s; cause=%s", data, e)
						s = fmt.Sprintf("Unable to decode %s; cause=%s", data, e)
					}
					data = []byte(s)
				}
				last = kind == ERR
			}

			cb := this.getCallback(seq, last)
			if cb != nil {
				var r = NewResponse(this, c, kind, seq, data)
				cb <- &r
			} else {
				logger.Errorf("No callback found in %s for kind=%s, sequence=%d.", this.conn.LocalAddr(), kind, seq)
			}
		}
	}
	/*
		if err == io.EOF {
			fmt.Println("< client", c.RemoteAddr(), "closed connection")
		} else if err != nil {
			fmt.Println("< error:", err)
		}
	*/

	this.disconnected(c, err)
}

// Executes the function that handles the request. By default the reply of this handler is allways final (REQ or ERR).
// If we wish  to send multiple replies, we must cancel the response and then send the several replies.
func (this *Wire) runHandler(handlerch chan EnvelopeConn) {
	for msgconn := range handlerch {
		msg := msgconn.message
		c := msgconn.conn

		if msg.kind == REQ || msg.kind == REQALL {
			// Serve
			var reply []byte
			var err error
			var deferReply bool
			var terminate bool
			var handlers = this.findHandlers(msg.name)
			if len(handlers) > 0 {
				var req = NewRequest(this, c, msg)
				for _, hnd := range handlers {
					req.middleware = hnd.calls
					req.middlewarePos = 0
					hnd.calls[0](req)
					// if there is any handler that terminates,
					// there will be no reply
					if req.terminate {
						terminate = true
					}
					// if there is at least one handler with gomsg.Request
					// there will be no explicit reply,
					if req.deferReply {
						deferReply = true
						reply = nil
					}
					if req.Fault() != nil {
						err = req.Fault() // single fault
						reply = nil
						break
					} else if !deferReply {
						reply = req.Reply() // single reply
					}
					// reset
					req.terminate = false
					req.deferReply = false
				}
			} else {
				err = UnknownTopic(faults.New(UNKNOWNTOPIC, msg.name))
			}

			// only sends a reply if there was some kind of return action
			if terminate {
				this.reply(ACK, msg.sequence, nil)
			} else if !deferReply {
				if err != nil {
					msg.kind = ERR
					if this.codec != nil {
						reply, _ = this.codec.Encode(err.Error())
					} else {
						reply = []byte(err.Error())
					}
				} else {
					msg.kind = REP
				}
				this.reply(msg.kind, msg.sequence, reply)
			}
		} else { // PUSH & PUB
			var handlers = this.findHandlers(msg.name)

			if msg.kind == PUSH {
				if len(handlers) > 0 {
					msg.kind = ACK
				} else {
					msg.kind = NACK
				}
				this.reply(msg.kind, msg.sequence, nil)
			}

			if len(handlers) > 0 {
				var req = NewRequest(this, c, msg)
				for _, handler := range handlers {
					req.middleware = handler.calls
					req.middlewarePos = 0
					handler.calls[0](req)
				}
			}
		}
	}
}

func validateSigErr(params []reflect.Type, contextType reflect.Type) (bool, bool, reflect.Type, bool) {
	order := 0
	var hasContext, hasError bool
	var payloadType reflect.Type
	for _, v := range params {
		if v == contextType {
			if order >= 1 {
				return false, false, nil, false
			}
			hasContext = true
			order = 1
		} else if v == errorType {
			if order >= 3 {
				return false, false, nil, false
			}
			hasError = true
			order = 3
		} else {
			if order >= 2 {
				return false, false, nil, false
			}
			payloadType = v
			order = 2
		}
	}
	return true, hasContext, payloadType, hasError
}

func CreateResponseHandler(codec Codec, fun interface{}) func(Response) {
	if f, ok := fun.(func(Response)); ok {
		return f
	}

	// validate input
	var payloadType reflect.Type
	var hasContext, hasError, ok bool
	function := reflect.ValueOf(fun)
	typ := function.Type()

	// validate argument types
	size := typ.NumIn()
	if size > 3 {
		panic("Invalid function. Function can only have at the most 3 parameters.")
	} else {
		params := make([]reflect.Type, 0, size)
		for i := 0; i < size; i++ {
			params = append(params, typ.In(i))
		}
		ok, hasContext, payloadType, hasError = validateSigErr(params, replyType)
		if !ok {
			panic("Invalid function. All parameter function are optional but they must folow the signature order: func([gomsg.Request], [custom type], [error]).")
		}
	}

	return func(ctx Response) {
		var p reflect.Value
		var params = make([]reflect.Value, 0)
		if hasContext {
			params = append(params, reflect.ValueOf(ctx))
		}
		if payloadType != nil {
			if codec != nil && ctx.reply != nil {
				p = reflect.New(payloadType)
				var e = codec.Decode(ctx.reply, p.Interface())
				if e != nil {
					logger.Errorf("Unable to decode %s; cause=%s", ctx.reply, e)
					ctx.fault = e
				}
				params = append(params, p.Elem())

			} else {
				// when the codec is nil the data is sent as is
				if ctx.reply == nil {
					params = append(params, reflect.Zero(payloadType))
				} else {
					params = append(params, reflect.ValueOf(ctx.reply))
				}
			}
		}
		if hasError {
			var err = ctx.Fault()
			params = append(params, reflect.ValueOf(&err).Elem())
		}

		function.Call(params)
	}

}

func CreateRequestHandler(codec Codec, fun interface{}) func(*Request) {
	if f, ok := fun.(func(*Request)); ok {
		return f
	}

	// validate input
	var payloadType reflect.Type
	function := reflect.ValueOf(fun)
	typ := function.Type()
	// validate argument types
	size := typ.NumIn()
	if size > 2 {
		panic("Invalid function. Function can only have at most two parameters.")
	} else if size > 1 {
		t := typ.In(0)
		if t != requestType {
			panic("Invalid function. In a two paramater function the first must be the gomsg.Request.")
		}
	}

	var hasContext bool
	if size == 2 {
		payloadType = typ.In(1)
		hasContext = true
	} else if size == 1 {
		t := typ.In(0)
		if t != requestType {
			payloadType = t
		} else {
			hasContext = true
		}
	}
	// validate return
	size = typ.NumOut()
	if size > 2 {
		panic("functions can only have at the most two return values.")
	} else if size > 1 && errorType != typ.Out(1) {
		panic("In a two return values functions the second value must be an error.")
	}

	return func(req *Request) {
		var p reflect.Value
		var params = make([]reflect.Value, 0)
		if hasContext {
			params = append(params, reflect.ValueOf(req))
		}
		if payloadType != nil {
			if codec != nil && req.payload != nil {
				p = reflect.New(payloadType)
				var e = codec.Decode(req.payload, p.Interface())
				if e != nil {
					logger.Errorf("Unable to decode %s; cause=%s", req.payload, e)
					req.SetFault(e)
					return
				}
				params = append(params, p.Elem())
			} else {
				// when the codec is nil the data is sent as is
				if req.payload == nil {
					params = append(params, reflect.Zero(payloadType))
				} else {
					params = append(params, reflect.ValueOf(req.payload))
				}
			}
		}

		results := function.Call(params)

		// check for error
		var result []byte
		var err error
		if len(results) > 0 {
			for k, v := range results {
				if v.Type() == errorType {
					if !v.IsNil() {
						err = v.Interface().(error)
					}
					break
				} else {
					// stores the result to return at the end of the check
					data := results[k].Interface()
					if codec != nil {
						result, err = codec.Encode(data)
						if err != nil {
							break
						}
					} else {
						// when the codec is nil the data is sent as is
						result = data.([]byte)
					}
				}
			}

			if err != nil {
				req.SetFault(err)
			} else {
				req.SetReply(result)
			}
		}
	}
}

type handler struct {
	rule  string
	calls []Middleware
}

type ClientServer struct {
	uuid     []byte
	muhnd    sync.RWMutex
	handlers []handler

	muconn    sync.RWMutex
	OnConnect func(w *Wire)
	OnClose   func(c net.Conn)
	Name      string
}

func NewClientServer() ClientServer {
	var uuid = NewUUID()
	return ClientServer{
		handlers: make([]handler, 0),
		uuid:     uuid,
		Name:     fmt.Sprintf("%X", uuid),
	}
}

func (this *ClientServer) removeHandler(name string) {
	this.muhnd.Lock()
	defer this.muhnd.Unlock()

	var a = this.handlers
	for i, v := range a {
		if v.rule == name {
			copy(a[i:], a[i+1:])
			a[len(a)-1] = handler{} // avoid memory leak
			this.handlers = a[:len(a)-1]
			return
		}
	}

	this.muhnd.Unlock()
}

func (this *ClientServer) addHandler(name string, hnds []Middleware) {
	this.muhnd.Lock()
	defer this.muhnd.Unlock()

	for k, v := range this.handlers {
		if v.rule == name {
			this.handlers[k].calls = hnds
			return
		}
	}

	this.handlers = append(this.handlers, handler{name, hnds})

}

// If the type of the payload is *mybus.Msg
// it will ignore encoding and use the internal bytes as the payload.
func createEnvelope(kind EKind, name string, payload interface{}, success interface{}, timeout time.Duration, codec Codec) (Envelope, error) {
	var handler func(ctx Response)
	if success != nil {
		handler = CreateResponseHandler(codec, success)
	}

	msg := Envelope{
		kind:    kind,
		name:    name,
		handler: handler,
		timeout: timeout,
	}

	if payload != nil {
		switch m := payload.(type) {
		case []byte:
			msg.payload = m
		case *Msg:
			msg.payload = m.buffer.Bytes()
		default:
			var err error
			msg.payload, err = codec.Encode(payload)
			if err != nil {
				return Envelope{}, err
			}
		}
	}

	return msg, nil
}

type Client struct {
	ClientServer

	addr                 string
	wire                 *Wire
	reconnectInterval    time.Duration
	reconnectMaxInterval time.Duration
	sendListeners        map[uint64]SendListener
	newTopicListeners    map[uint64]TopicListener
	dropTopicListeners   map[uint64]TopicListener
	listenersIdx         uint64
	defaultTimeout       time.Duration
}

// NewClient creates a Client
func NewClient() *Client {
	this := &Client{
		reconnectInterval:    time.Millisecond * 10,
		reconnectMaxInterval: time.Second,
		wire:                 NewWire(JsonCodec{}),
		sendListeners:        make(map[uint64]SendListener),
		newTopicListeners:    make(map[uint64]TopicListener),
		dropTopicListeners:   make(map[uint64]TopicListener),
		defaultTimeout:       time.Second * 5,
	}
	this.ClientServer = NewClientServer()

	this.wire.findHandlers = func(name string) []handler {
		this.muhnd.RLock()
		defer this.muhnd.RUnlock()

		return findHandlers(name, this.handlers)
	}

	this.wire.disconnected = func(c net.Conn, e error) {
		this.muconn.Lock()
		defer this.muconn.Unlock()

		// Since this can be called from several places at the same time (reader goroutine, Reconnect(), ),
		// we must check if it is still the same connection
		if this.wire.conn == c {
			this.wire.Destroy()
			if this.OnClose != nil {
				this.OnClose(c)
			}
			this.wire.conn = nil
			if this.reconnectInterval > 0 {
				go this.dial(this.reconnectInterval, make(chan error, 1))
			}
		}
	}

	return this
}

func (this *Client) SetDefaultTimeout(timeout time.Duration) {
	this.defaultTimeout = timeout
}

// AddSendListener adds a listener on send messages (Publis/Push/RequestAll/Request)
func (this *Client) AddSendListener(listener SendListener) uint64 {
	if this.wire.OnSend == nil {
		this.wire.OnSend = func(event SendEvent) {
			this.fireSendListeners(event)
		}
	}
	var idx = atomic.AddUint64(&this.listenersIdx, 1)
	this.sendListeners[idx] = listener
	return idx
}

// RemoveSendListener removes a previously added listener on send messages
func (this *Client) RemoveSendListener(idx uint64) {
	delete(this.sendListeners, idx)
}

func (this *Client) fireSendListeners(event SendEvent) {
	for _, listener := range this.sendListeners {
		listener(event)
	}
}

func (this *Client) AddNewTopicListener(listener TopicListener) uint64 {
	if this.wire.OnNewTopic == nil {
		this.wire.OnNewTopic = func(event TopicEvent) {
			this.fireNewTopicListeners(event)
		}
	}
	var idx = atomic.AddUint64(&this.listenersIdx, 1)
	this.newTopicListeners[idx] = listener
	return idx
}

// RemoveSendListener removes a previously added listener on send messages
func (this *Client) RemoveNewTopicListener(idx uint64) {
	delete(this.newTopicListeners, idx)
}

func (this *Client) fireNewTopicListeners(event TopicEvent) {
	for _, listener := range this.newTopicListeners {
		listener(event)
	}
}

func (this *Client) AddDropTopicListener(listener TopicListener) uint64 {
	if this.wire.OnDropTopic == nil {
		this.wire.OnDropTopic = func(event TopicEvent) {
			this.fireDropTopicListener(event)
		}
	}
	var idx = atomic.AddUint64(&this.listenersIdx, 1)
	this.dropTopicListeners[idx] = listener
	return idx
}

// RemoveSendListener removes a previously added listener on send messages
func (this *Client) RemoveDropTopicListener(idx uint64) {
	delete(this.dropTopicListeners, idx)
}

func (this *Client) fireDropTopicListener(event TopicEvent) {
	for _, listener := range this.dropTopicListeners {
		listener(event)
	}
}

func (this *Client) SetBufferSize(size int) {
	this.wire.bufferSize = size
}

func (this *Client) SetRateLimiter(limiter toolkit.Rate) {
	this.wire.SetRateLimiter(limiter)
}

func (this *Client) SetReconnectInterval(reconnectInterval time.Duration) *Client {
	this.reconnectInterval = reconnectInterval
	return this
}

func (this *Client) SetReconnectMaxInterval(reconnectMaxInterval time.Duration) *Client {
	this.reconnectMaxInterval = reconnectMaxInterval
	return this
}

func (this *Client) SetCodec(codec Codec) *Client {
	this.wire.codec = codec
	return this
}

// Make it belong to a group.
// Only one element at a time (round-robin) handles the messages.
func (this *Client) SetGroupId(groupId string) *Client {
	this.wire.localGroupID = groupId
	return this
}

func (this *Client) handshake(c net.Conn) error {
	this.muhnd.RLock()
	var size = len(this.handlers) + 1
	var payload = make([]string, size)
	payload[0] = this.wire.localGroupID
	i := 1
	for _, v := range this.handlers {
		payload[i] = v.rule
		i++
	}
	this.muhnd.RUnlock()

	err := serializeHanshake(c, this.wire.codec, time.Second, this.uuid, payload)
	if err != nil {
		return err
	}

	// get reply
	uuid, group, remoteTopics, err := deserializeHandshake(c, this.wire.codec, time.Second)
	if err != nil {
		return err
	}
	this.wire.mutop.Lock()
	this.wire.remoteUuid = uuid
	this.wire.remoteTopics = remoteTopics
	var addr = c.RemoteAddr().String()
	for k := range remoteTopics {
		this.fireNewTopicListeners(TopicEvent{this.wire.remoteUuid, addr, k})
	}
	this.wire.remoteGroupID = group
	this.wire.mutop.Unlock()

	return nil
}

func serializeHanshake(c net.Conn, codec Codec, timeout time.Duration, uuid []byte, payload []string) error {
	var data []byte
	var err error
	if codec != nil {
		data, err = codec.Encode(payload)
		if err != nil {
			return faults.Wrap(err)
		}
	} else {
		var buf = new(bytes.Buffer)
		os := NewOutputStream(buf)
		os.WriteUI16(uint16(len(payload)))
		if err != nil {
			return faults.Wrap(err)
		}
		for _, v := range payload {
			os.WriteString(v)
		}
		data = buf.Bytes()
	}

	c.SetWriteDeadline(timeouttime(timeout))

	var w = bufio.NewWriter(c)
	var os = NewOutputStream(w)
	// uuid
	_, err = os.writer.Write(uuid)
	if err != nil {
		return err
	}
	err = os.WriteBytes(data)
	if err != nil {
		return err
	}
	return w.Flush()
}

func deserializeHandshake(c net.Conn, codec Codec, timeout time.Duration) ([]byte, string, map[string]bool, error) {
	c.SetReadDeadline(timeouttime(timeout))

	r := NewInputStream(c)
	var data, err = r.ReadNBytes(16)
	if err != nil {
		return nil, "", nil, faults.Wrap(err)
	}
	var uuid = data
	data, err = r.ReadBytes()
	if err != nil {
		return nil, "", nil, faults.Wrap(err)
	}
	var topics []string
	if codec != nil {
		var e = codec.Decode(data, &topics)
		if e != nil {
			panic(fmt.Sprintf("Unable to decode %s; cause=%s", data, e))
		}

	} else {
		var buf = bytes.NewBuffer(data)
		is := NewInputStream(buf)
		size, err := is.ReadUI16()
		if err != nil {
			return nil, "", nil, faults.Wrap(err)
		}
		topics = make([]string, size)
		var length = int(size)
		for i := 0; i < length; i++ {
			topics[i], err = is.ReadString()
			if err != nil {
				return nil, "", nil, faults.Wrap(err)
			}
		}
	}
	var identity = topics[0]
	topics = topics[1:]
	remoteTopics := make(map[string]bool)
	for _, v := range topics {
		remoteTopics[v] = true
	}
	return uuid, identity, remoteTopics, nil
}

// connect is seperated to allow the definition and use of OnConnect
func (this *Client) Connect(addr string) <-chan error {
	this.addr = addr
	var cherr = make(chan error, 1)
	go this.dial(this.reconnectInterval, cherr)
	return cherr
}

func (this *Client) Address() string {
	return this.addr
}

func (this *Client) Reconnect() <-chan error {
	this.Disconnect()
	return this.Connect(this.addr)
}

func (this *Client) Disconnect() {
	if this.wire != nil {
		this.wire.disconnected(this.wire.conn, nil)
	}
}

func (this *Client) Destroy() {
	this.reconnectInterval = 0
	this.wire.Destroy()
	this.sendListeners = make(map[uint64]SendListener)
	this.newTopicListeners = make(map[uint64]TopicListener)
	this.dropTopicListeners = make(map[uint64]TopicListener)
	atomic.StoreUint64(&this.listenersIdx, 0)
	this.Disconnect()
}

// Active check if this wire is running, i.e., if it has a connection
func (this *Client) Active() bool {
	return this.wire.conn != nil
}

// tries do dial. If dial fails and if reconnectInterval > 0
// it schedules a new try within reconnectInterval milliseconds
func (this *Client) dial(retry time.Duration, cherr chan error) {
	var c net.Conn
	var err error
	for {
		// gets the connection
		c, err = net.DialTimeout("tcp", this.addr, time.Second)
		if err != nil {
			logger.Tracef("%s: failed to connect to %s", this.Name, this.addr)
			if this.reconnectInterval > 0 {
				logger.Tracef("%s: retry connecting to %s in %v", this.Name, this.addr, retry)
				time.Sleep(retry)
				if this.reconnectMaxInterval > 0 && retry < this.reconnectMaxInterval {
					retry = retry * 2
					if retry > this.reconnectMaxInterval {
						retry = this.reconnectMaxInterval
					}
				}
			} else {
				logger.Tracef("%X: NO retry will be performed to %s!", this.uuid, this.addr)
				return
			}
		} else {
			break
		}
	}

	logger.Tracef("%s: connected to %s", c.LocalAddr(), this.addr)

	this.muconn.Lock()
	defer this.muconn.Unlock()

	// topic exchange
	err = this.handshake(c)
	if err != nil {
		logger.Errorf("%s: error while handshaking %s: %s", c.LocalAddr(), this.addr, err)
		this.wire.SetConn(nil)
		c.Close()
	} else {
		this.wire.SetConn(c)

		if this.OnConnect != nil {
			this.OnConnect(this.wire)
		}
	}
	cherr <- err
}

func (this *Client) Conn() net.Conn {
	return this.wire.conn
}

// name can have an '*' at the end, meaning that it will handle messages
// with the destiny name starting with the reply name whitout the '*'.
// When handling request messages, the function handler can have a return value and/or an error.
// When handling publish/push messages, any return from the function handler is discarded.
// When handling Request/RequestAll messages, if a return is not specified,
// the caller will not receive a reply until you explicitly call gomsg.Request.ReplyAs()
func (this *Client) Handle(name string, middlewares ...interface{}) {
	logger.Infof("Registering handler for %s", name)

	var size = len(middlewares)
	var hnds = make([]Middleware, size)
	for i := 0; i < size; i++ {
		hnds[i] = CreateRequestHandler(this.wire.codec, middlewares[i])
	}
	this.addHandler(name, hnds)

	this.muconn.Lock()
	defer this.muconn.Unlock()

	if this.wire.conn != nil {
		msg, _ := createEnvelope(SUB, name, nil, nil, time.Second, this.wire.codec)
		this.wire.Send(msg)
	}
}

func (this *Client) Cancel(name string) {
	this.removeHandler(name)

	this.muconn.Lock()
	defer this.muconn.Unlock()

	if this.wire.conn != nil {
		msg, _ := createEnvelope(UNSUB, name, nil, nil, time.Second, this.wire.codec)
		this.wire.Send(msg)
	}
}

// Request sends a message and waits for the reply
func (this *Client) Request(name string, payload interface{}, handler interface{}) <-chan error {
	return this.RequestTimeout(name, payload, handler, this.defaultTimeout)
}

// RequestTimeout is the same as Request with a timeout definition
func (this *Client) RequestTimeout(name string, payload interface{}, handler interface{}, timeout time.Duration) <-chan error {
	return this.Send(REQ, name, payload, handler, timeout)
}

// RequestAll requests messages to all connected clients of the same server. If a client is not connected it is forever lost.
func (this *Client) RequestAll(name string, payload interface{}, handler interface{}) <-chan error {
	return this.Send(REQALL, name, payload, handler, this.defaultTimeout)
}

// RequestAllTimeout requests messages to all connected clients of the same server. If a client is not connected it is forever lost.
func (this *Client) RequestAllTimeout(name string, payload interface{}, handler interface{}, timeout time.Duration) <-chan error {
	return this.Send(REQALL, name, payload, handler, timeout)
}

// Push sends a message and receive an acknowledge
func (this *Client) Push(name string, m interface{}) <-chan error {
	return this.PushTimeout(name, m, this.defaultTimeout)
}

// PushTimeout  is the same as Push with a timeout definition
func (this *Client) PushTimeout(name string, m interface{}, timeout time.Duration) <-chan error {
	return this.Send(PUSH, name, m, nil, timeout)
}

// Publish sends a message without any reply
func (this *Client) Publish(name string, m interface{}) <-chan error {
	return this.PublishTimeout(name, m, this.defaultTimeout)
}

// If the type of the payload is *mybus.Msg
// it will ignore encoding and use the internal bytes as the payload.
func (this *Client) PublishTimeout(name string, m interface{}, timeout time.Duration) <-chan error {
	return this.Send(PUB, name, m, nil, timeout)
}

// When the payload is of type []byte it passes the raw bytes without encoding.
func (this *Client) Send(kind EKind, name string, payload interface{}, handler interface{}, timeout time.Duration) <-chan error {
	msg, err := createEnvelope(kind, name, payload, handler, timeout, this.wire.codec)
	if err != nil {
		ch := make(chan error, 1)
		ch <- err
		return ch
	}
	return this.wire.Send(msg)
}

// Wires manages a collection of connections as if they were one.
// Connections are grouped accordingly to its group id.
// A wire with an empty group id means all nodes are different.
//
// Wires with the same non empty id are treated as mirrors of each other.
// This means that we only need to call one of them.
// The other nodes function as High Availability and load balancing nodes
type Wires struct {
	sync.RWMutex

	wires              []*Wire
	groups             map[string][]*Wire
	cursor             int
	codec              Codec
	sendListeners      map[uint64]SendListener
	newTopicListeners  map[uint64]TopicListener
	dropTopicListeners map[uint64]TopicListener
	listenersIdx       uint64
	loadBalancer       LoadBalancer
	rateLimiterFactory func() toolkit.Rate
	bufferSize         int
	defaultTimeout     time.Duration
}

type SendListener func(event SendEvent)

type SendEvent struct {
	Kind    EKind
	Name    string
	Payload interface{}
	Handler interface{}
}

type TopicListener func(event TopicEvent)

type TopicEvent struct {
	Source     []byte // source uuid
	SourceAddr string
	Name       string // topic name
}

// NewWires creates a Wires structure
func NewWires(codec Codec) *Wires {
	return &Wires{
		wires:              make([]*Wire, 0),
		groups:             make(map[string][]*Wire),
		codec:              codec,
		sendListeners:      make(map[uint64]SendListener),
		newTopicListeners:  make(map[uint64]TopicListener),
		dropTopicListeners: make(map[uint64]TopicListener),
		loadBalancer:       NewSimpleLB(),
		bufferSize:         1000,
		defaultTimeout:     time.Second * 5,
	}
}

func (this *Wires) SetDefaultTimeout(timeout time.Duration) {
	this.defaultTimeout = timeout
}

// AddSendListener adds a listener on send messages (Publis/Push/RequestAll/Request)
func (this *Wires) AddSendListener(listener SendListener) uint64 {
	var idx = atomic.AddUint64(&this.listenersIdx, 1)
	this.sendListeners[idx] = listener
	return idx
}

// RemoveSendListener removes a previously added listener on send messages
func (this *Wires) RemoveSendListener(idx uint64) {
	delete(this.sendListeners, idx)
}

func (this *Wires) fireSendListener(event SendEvent) {
	for _, listener := range this.sendListeners {
		listener(event)
	}
}

func (this *Wires) AddNewTopicListener(listener TopicListener) uint64 {
	this.Lock()
	defer this.Unlock()

	var idx = atomic.AddUint64(&this.listenersIdx, 1)
	this.newTopicListeners[idx] = listener
	return idx
}

// RemoveSendListener removes a previously added listener on send messages
func (this *Wires) RemoveNewTopicListener(idx uint64) {
	this.Lock()
	defer this.Unlock()

	delete(this.newTopicListeners, idx)
}

func (this *Wires) fireNewTopicListener(event TopicEvent) {
	for _, listener := range this.newTopicListeners {
		listener(event)
	}
}

func (this *Wires) AddDropTopicListener(listener TopicListener) uint64 {
	this.Lock()
	defer this.Unlock()

	var idx = atomic.AddUint64(&this.listenersIdx, 1)
	this.dropTopicListeners[idx] = listener
	return idx
}

// RemoveSendListener removes a previously added listener on send messages
func (this *Wires) RemoveDropTopicListener(idx uint64) {
	this.Lock()
	defer this.Unlock()

	delete(this.dropTopicListeners, idx)
}

func (this *Wires) fireDropTopicListener(event TopicEvent) {
	for _, listener := range this.dropTopicListeners {
		listener(event)
	}
}

func (this *Wires) SetBufferSize(size int) {
	this.bufferSize = size
}

func (this *Wires) SetRateLimiterFactory(factory func() toolkit.Rate) {
	this.rateLimiterFactory = factory
}

func (this *Wires) SetLoadBalancer(loadBalancer LoadBalancer) {
	this.loadBalancer = loadBalancer
}

func (this *Wires) LoadBalancer() LoadBalancer {
	return this.loadBalancer
}

func (this *Wires) SetCodec(codec Codec) *Wires {
	this.Lock()
	defer this.Unlock()

	this.codec = codec
	for _, v := range this.wires {
		v.codec = codec
		v.Destroy()
	}

	return this
}

func (this *Wires) Codec() Codec {
	return this.codec
}

func (this *Wires) Destroy() {
	this.Lock()
	defer this.Unlock()

	for _, v := range this.wires {
		v.Destroy()
	}
	this.wires = make([]*Wire, 0)
	this.groups = make(map[string][]*Wire)
	this.sendListeners = make(map[uint64]SendListener)
	this.newTopicListeners = make(map[uint64]TopicListener)
	this.dropTopicListeners = make(map[uint64]TopicListener)
	atomic.StoreUint64(&this.listenersIdx, 0)
	this.cursor = 0
}

func (this *Wires) Add(wire *Wire) {
	this.Lock()
	defer this.Unlock()

	// if already defined (same connection) panics
	for _, v := range this.wires {
		if v.conn == wire.conn {
			panic("Already exists a Wire with the same connection.")
		}
	}

	// set common codec
	wire.codec = this.codec
	if this.rateLimiterFactory != nil {
		wire.rateLimiter = this.rateLimiterFactory()
	}

	// define trigger on new topic
	wire.OnNewTopic = func(event TopicEvent) {
		this.fireNewTopicListener(event)
	}

	var addr = wire.conn.RemoteAddr().String()
	for k := range wire.remoteTopics {
		// use trigger
		wire.OnNewTopic(TopicEvent{wire.remoteUuid, addr, k})
	}

	this.wires = append(this.wires, wire)
	this.loadBalancer.Add(wire)

	var group = this.groups[wire.remoteGroupID]
	if group == nil {
		group = make([]*Wire, 0)
	}
	this.groups[wire.remoteGroupID] = append(group, wire)
}

func (this *Wires) Get(conn net.Conn) *Wire {
	return this.Find(func(w *Wire) bool {
		return w.conn == conn
	})
}

func (this *Wires) Find(fn func(w *Wire) bool) *Wire {
	this.Lock()
	defer this.Unlock()

	for _, v := range this.wires {
		if fn(v) {
			return v
		}
	}
	return nil
}

func (this *Wires) Kill(conn net.Conn) {
	this.Lock()
	defer this.Unlock()

	var w *Wire
	this.wires, w = remove(conn, this.wires)

	if w != nil {
		// define trigger on drop topic
		w.OnDropTopic = func(event TopicEvent) {
			this.fireDropTopicListener(event)
		}

		var addr = w.conn.RemoteAddr().String()
		for k := range w.remoteTopics {
			// use trigger
			w.OnDropTopic(TopicEvent{w.remoteUuid, addr, k})
		}

		this.loadBalancer.Remove(w)

		var group = this.groups[w.remoteGroupID]
		if group != nil {
			this.groups[w.remoteGroupID], _ = remove(conn, group)
		}
		w.Destroy()
	}

}

func remove(conn net.Conn, wires []*Wire) ([]*Wire, *Wire) {
	for k, v := range wires {
		if v.conn == conn {
			return removeWire(wires, k), v
		}
	}
	return wires, nil
}

func removeWire(wires []*Wire, k int) []*Wire {
	// since the slice has a non-primitive, we have to zero it
	copy(wires[k:], wires[k+1:])
	wires[len(wires)-1] = nil // zero it
	return wires[:len(wires)-1]
}

func (this *Wires) GetAll() []*Wire {
	this.Lock()
	defer this.Unlock()

	return clone(this.wires)
}

func clone(wires []*Wire) []*Wire {
	// clone
	w := make([]*Wire, len(wires))
	copy(w, wires)

	return w
}

func (this *Wires) Size() int {
	this.RLock()
	defer this.RUnlock()
	return len(this.wires)
}

// Request sends a message and waits for the reply
// If the type of the payload is *mybus.Msg it will ignore encoding and use the internal bytes as the payload. This is useful if we want to implement a broker.
func (this *Wires) Request(name string, payload interface{}, handler interface{}) <-chan error {
	return this.Send(REQ, name, payload, handler, time.Second)
}

// RequestTimeout sends a message and waits for the reply
func (this *Wires) RequestTimeout(name string, payload interface{}, handler interface{}, timeout time.Duration) <-chan error {
	return this.Send(REQ, name, payload, handler, timeout)
}

// RequestAll requests messages to all connected clients. If a client is not connected it is forever lost.
func (this *Wires) RequestAll(name string, payload interface{}, handler interface{}) <-chan error {
	return this.Send(REQALL, name, payload, handler, this.defaultTimeout)
}

// RequestAll requests messages to all connected clients. If a client is not connected it is forever lost.
func (this *Wires) RequestAllTimeout(name string, payload interface{}, handler interface{}, timeout time.Duration) <-chan error {
	return this.Send(REQALL, name, payload, handler, timeout)
}

// Push sends a message and receive an acknowledge
// If the type of the payload is *mybus.Msg
// it will ignore encoding and use the internal bytes as the payload.
func (this *Wires) Push(name string, payload interface{}) <-chan error {
	return this.Send(PUSH, name, payload, nil, this.defaultTimeout)
}

func (this *Wires) PushTimeout(name string, payload interface{}, timeout time.Duration) <-chan error {
	return this.Send(PUSH, name, payload, nil, time.Second)
}

// Publish sends a message without any reply
// If the type of the payload is *mybus.Msg
// it will ignore encoding and use the internal bytes as the payload.
func (this *Wires) Publish(name string, payload interface{}) <-chan error {
	return this.Send(PUB, name, payload, nil, this.defaultTimeout)
}

func (this *Wires) PublishTimeout(name string, payload interface{}, timeout time.Duration) <-chan error {
	return this.Send(PUB, name, payload, nil, timeout)
}

// Send is the generic function to send messages
// When the payload is of type []byte it passes the raw bytes without encoding.
// When the payload is of type mybus.Msg it passes the FRAMED raw bytes without encoding.
func (this *Wires) Send(kind EKind, name string, payload interface{}, handler interface{}, timeout time.Duration) <-chan error {
	return this.SendSkip(nil, kind, name, payload, handler, timeout)
}

func (this *Wires) send(w *Wire, msg Envelope) <-chan error {
	this.loadBalancer.Prepare(w, msg)

	// fires all registered listeners
	this.fireSendListener(SendEvent{
		Kind:    msg.kind,
		Name:    msg.name,
		Payload: msg.payload,
		Handler: msg.handler,
	})

	return w.justsend(msg)
}

func wiresTriage(name string, wires []*Wire, skipWire *Wire) []*Wire {
	var ws = make([]*Wire, 0)
	for _, w := range wires {
		// does not send to self
		if (skipWire == nil || skipWire != w) && w.hasRemoteTopic(name) {
			ws = append(ws, w)
		}
	}
	return ws
}

// SendSkip is the generic function to send messages with the possibility of ignoring the sender
func (this *Wires) SendSkip(skipWire *Wire, kind EKind, name string, payload interface{}, handler interface{}, timeout time.Duration) <-chan error {
	msg, err := createEnvelope(kind, name, payload, handler, timeout, this.codec)
	errch := make(chan error, 1)
	if err != nil {
		errch <- err
		return errch
	}

	err = UnknownTopic(fmt.Errorf(UNKNOWNTOPIC, msg.name))
	if msg.kind == PUSH || msg.kind == REQ {
		go func() {
			this.RLock()
			var wires = wiresTriage(name, this.wires, skipWire)
			this.RUnlock()
			var size = len(wires)
			for i := 0; i < size; i++ {
				var w = this.loadBalancer.Next(name, wires)
				// REQ can also receive multiple messages from ONE replier
				err = <-this.send(w, msg)
				// exit on success
				if err == nil {
					this.loadBalancer.Success(w, msg)
					break
				} else {
					this.loadBalancer.Failure(w, msg, err)
				}
			}
			errch <- err
		}()
	} else if msg.kind == PUB {
		go func() {
			for id, group := range this.groups {
				if id == noGROUP {
					for _, w := range wiresTriage(name, group, skipWire) {
						e := <-this.send(w, msg)
						if e == nil {
							this.loadBalancer.Success(w, msg)
							err = nil
						} else {
							this.loadBalancer.Failure(w, msg, e)
						}
					}
				} else {
					go func(wires []*Wire) {
						var ws = wiresTriage(name, wires, skipWire)
						var size = len(ws)
						for i := 0; i < size; i++ {
							var w = this.loadBalancer.Next(name, ws)
							e := <-this.send(w, msg)
							// send only to one.
							// stop if there was a success.
							if e == nil {
								this.loadBalancer.Success(w, msg)
								break
							} else {
								this.loadBalancer.Failure(w, msg, e)
							}
						}
					}(group)
				}
			}
			errch <- err
		}()
	} else if msg.kind == REQALL {
		var wg sync.WaitGroup
		// we wrap the reply handler so that we can control the number o replies delivered.
		handler := msg.handler
		msg.handler = func(ctx Response) {
			// since the end mark will be passed to the caller when ALL replies from ALL repliers arrive,
			// the handler must be called only if it is not a end marker.
			// It is also necessary to adjust the kind so that a last kind (REQ, ERR) is not passed.
			if !ctx.EndMark() {
				kind := ctx.Kind
				if ctx.Kind == REP {
					ctx.Kind = REP_PARTIAL
				} else if ctx.Kind == ERR {
					ctx.Kind = ERR_PARTIAL
				}
				if handler != nil {
					handler(ctx)
				}
				// resets the kind
				ctx.Kind = kind
			}
		}

		// collects wires into groups.
		for id, group := range this.groups {
			if id == noGROUP {
				for _, w := range wiresTriage(name, group, skipWire) {
					// increment reply counter
					logger.Tracef("sending message to %s (ungrouped)", w.Conn().RemoteAddr())
					wg.Add(1)
					ch := this.send(w, msg)
					go func() {
						// waits for the request completion
						e := <-ch
						if e == nil {
							this.loadBalancer.Success(w, msg)
							err = nil // at least one got through
						} else {
							this.loadBalancer.Failure(w, msg, e)
							logger.Errorf("Failed requesting all: %s", e)
						}
						wg.Done()
					}()
				}
			} else {
				// increment reply counter
				wg.Add(1)
				go func(wires []*Wire) {
					var ws = wiresTriage(name, wires, skipWire)
					var size = len(ws)
					for i := 0; i < size; i++ {
						var w = this.loadBalancer.Next(name, ws)
						logger.Tracef("sending message to %s (group %s)", w.Conn().RemoteAddr(), id)
						// waits for the request completion
						e := <-this.send(w, msg)
						// send only to one.
						// stop if there was a success.
						if e == nil {
							this.loadBalancer.Success(w, msg)
							err = nil
							break
						} else {
							this.loadBalancer.Failure(w, msg, e)
						}
					}
					wg.Done()
				}(group)
			}
		}
		go func() {
			// Wait for all requests to complete.
			wg.Wait()
			logger.Tracef("all requests finnished")
			// pass the end mark
			if handler != nil {
				handler(NewResponse(skipWire, nil, ACK, 0, nil))
			}
			errch <- err
		}()
	}

	return errch
}

type BindListener func(net.Listener)

type Server struct {
	ClientServer
	*Wires

	listener      net.Listener
	bindListeners map[uint64]BindListener
	listenersIdx  uint64
}

func NewServer() *Server {
	server := &Server{
		bindListeners: make(map[uint64]BindListener),
	}
	server.Wires = NewWires(JsonCodec{})
	server.ClientServer = NewClientServer()
	return server
}

func (this *Server) AddBindListeners(listener BindListener) uint64 {
	var idx = atomic.AddUint64(&this.listenersIdx, 1)
	this.bindListeners[idx] = listener
	return idx

}

// RemoveBindListener removes a previously added listener on send messages
func (this *Server) RemoveBindListener(idx uint64) {
	delete(this.bindListeners, idx)
}

func (this *Server) fireBindListeners(event net.Listener) {
	for _, listener := range this.bindListeners {
		listener(event)
	}
}

// BindAddress returns the listener address
func (this *Server) BindPort() int {
	if this.listener != nil {
		return this.listener.Addr().(*net.TCPAddr).Port
	}
	return 0
}

func (this *Server) SetBufferSize(size int) {
	this.Wires.SetBufferSize(size)
}

func (this *Server) SetRateLimiterFactory(factory func() toolkit.Rate) {
	this.Wires.SetRateLimiterFactory(factory)
}

func (this *Server) Listener() net.Listener {
	return this.listener
}

func (this *Server) Listen(service string) <-chan error {
	var cherr = make(chan error, 1)
	// listen all interfaces
	l, err := net.Listen("tcp", service)
	if err != nil {
		cherr <- err
		return cherr
	}
	this.listener = l
	this.fireBindListeners(l)
	logger.Tracef("listening at %s", l.Addr())
	go func() {
		for {
			// notice that c is changed in the disconnect function
			c, err := l.Accept()
			if err != nil {
				// happens when the listener is closed
				logger.Debugf("Stoped listening at %s", l.Addr())
				cherr <- err
				return
			}
			wire := NewWire(this.codec)
			wire.findHandlers = this.findHandlers

			// topic exchange
			group, remoteTopics, err := this.handshake(c, wire)
			logger.Tracef("%s: accepted connection from %s", l.Addr(), c.RemoteAddr())

			if err != nil {
				logger.Errorf("Failed to handshake. Rejecting connection: %s", err)
				c.Close()
			} else {
				wire.remoteGroupID = group
				wire.remoteTopics = remoteTopics
				wire.disconnected = func(conn net.Conn, e error) {
					this.muconn.Lock()
					defer this.muconn.Unlock()

					// check to see if we are disconnecting the same connection
					if c == conn {
						// handle errors during a connection
						if faults.Has(e, io.EOF) || isClosed(e) {
							logger.Debugf("client %s - closed connection", conn.RemoteAddr())
						} else if e != nil {
							logger.Errorf("Client %s droped with error: %s", conn.RemoteAddr(), faults.Wrap(e))
						}

						this.Wires.Kill(conn)
						if this.OnClose != nil {
							this.OnClose(conn)
						}
						c = nil
					}
				}

				wire.SetConn(c)

				this.Wires.Add(wire)

				if this.OnConnect != nil {
					this.OnConnect(wire)
				}
			}
		}
	}()

	return cherr
}

func (this *Server) Port() int {
	return this.listener.Addr().(*net.TCPAddr).Port
}

func (this *Server) handshake(c net.Conn, wire *Wire) (string, map[string]bool, error) {
	// get remote topics
	uuid, group, payload, err := deserializeHandshake(c, wire.codec, time.Second)
	if err != nil {
		return "", nil, err
	}
	// use the uuid from the client
	wire.remoteUuid = uuid

	// send identity and local topics
	this.muhnd.RLock()
	var size = len(this.handlers) + 1
	var topics = make([]string, size)
	topics[0] = wire.localGroupID
	i := 1
	for _, v := range this.handlers {
		topics[i] = v.rule
		i++
	}
	this.muhnd.RUnlock()

	err = serializeHanshake(c, wire.codec, time.Second, this.uuid, topics)
	if err != nil {
		return "", nil, err
	}

	return group, payload, nil
}

func (this *Server) findHandlers(name string) []handler {
	this.muhnd.RLock()
	defer this.muhnd.RUnlock()

	return findHandlers(name, this.handlers)
}

// find the first match
func findHandlers(name string, handlers []handler) []handler {
	var funcs = make([]handler, 0)
	var prefix string
	for _, v := range handlers {
		if strings.HasSuffix(v.rule, FILTER_TOKEN) {
			prefix = v.rule[:len(v.rule)-1]
			if strings.HasPrefix(name, prefix) {
				funcs = append(funcs, v)
			}
		} else if name == v.rule {
			funcs = append(funcs, v)
		}
	}
	return funcs
}

// Destroy closes all connections and the listener
func (this *Server) Destroy() {
	this.Wires.Destroy()
	if this.listener != nil {
		this.listener.Close()
	}
	this.listener = nil
	this.bindListeners = make(map[uint64]BindListener)
	this.listenersIdx = 0
}

// Handle defines the function that will handle messages for a topic.
// name can have an '*' at the end, meaning that it will handle messages
// with the destiny name starting with the reply name (whitout the '*').
// When handling request messages, the function handler can have a return value and/or an error.
// When handling publish/push messages, any return from the function handler is discarded.
// When handling Request/RequestAll messages, if a return is not specified,
// the caller will not receive a reply until you explicitly call gomsg.Request.ReplyAs()
func (this *Server) Handle(name string, middlewares ...interface{}) {
	logger.Infof("Registering handler for %s", name)
	var size = len(middlewares)
	var hnds = make([]Middleware, size)
	for i := 0; i < size; i++ {
		hnds[i] = CreateRequestHandler(this.codec, middlewares[i])
	}
	this.addHandler(name, hnds)

	msg, _ := createEnvelope(SUB, name, nil, nil, time.Second, this.codec)
	var ws = this.Wires.GetAll()
	for _, w := range ws {
		w.Send(msg)
	}
}

// Messages are from one client and delivered to another client.
// The sender client does not receive his message.
// The handler execution is canceled. Arriving replies from endpoints are piped to the requesting wire
func (this *Server) Route(name string, timeout time.Duration, before func(x *Request) bool, after func(x *Response)) {
	// This handle is called by the wire.
	// Since this handler has no declared return type, upon return no data will be sent to the caller.
	// Eventually the reply must be sent or a timeout will occur.
	this.Handle(name, func(r *Request) {
		// decide if it should continue (and do other thinhs like store the request data).
		if before != nil && !before(r) {
			return
		}

		/*
			We defer the reply with req.DeferReply(),
			and then we send the reply when it comes with req.ReplyAs().
		*/
		var err = <-this.SendSkip(r.wire, r.Kind, r.Name, r.Payload(), func(resp Response) {
			if after != nil {
				// do something (ex: store the response data)
				after(&resp)
			}

			// make sure that when asking for REQALL, we reply as REP_PARTIAL
			var kind EKind
			if r.Kind == REQALL && resp.Kind != ACK {
				kind = REP_PARTIAL
			} else {
				kind = resp.Kind
			}
			// sending data to the caller
			r.ReplyAs(kind, resp.Reply())
		}, timeout)
		if err != nil {
			r.ReplyAs(ERR, []byte(err.Error()))
		}
		// avoids sending the REP kind when exiting
		r.DeferReply()
	})
}

func (this *Server) Cancel(name string) {
	this.removeHandler(name)

	msg, _ := createEnvelope(UNSUB, name, nil, nil, time.Second, this.codec)
	var ws = this.Wires.GetAll()
	for _, w := range ws {
		w.Send(msg)
	}
}

/*
func isTimeout(err error) bool {
	e, ok := err.(net.Error)
	return ok && e.Timeout()
}
*/

func test(t EKind, values ...EKind) bool {
	for _, v := range values {
		if v == t {
			return true
		}
	}
	return false
}

type Handler interface {
	Handle(string, ...interface{})
}

type Sender interface {
	Send(kind EKind, name string, payload interface{}, handler interface{}, timeout time.Duration) <-chan error
}

var _ Handler = &Client{}
var _ Handler = &Server{}
var _ Sender = &Client{}
var _ Sender = &Server{}

// Route messages between to different binding ports.
func Route(name string, src Handler, dest Sender, relayTimeout time.Duration, before func(c *Request) bool, after func(c *Response)) {
	src.Handle(name, func(req *Request) {
		if before != nil && !before(req) {
			return
		}

		/*
			We defer the reply with req.DeferReply(),
			and then we send the reply when it comes with req.ReplyAs().
		*/
		var err = <-dest.Send(req.Kind, req.Name, req.Payload(), func(resp Response) {
			if after != nil {
				after(&resp)
			}

			// make sure that when asking for REQALL, we reply as REP_PARTIAL
			var kind EKind
			if req.Kind == REQALL && resp.Kind != ACK {
				kind = REP_PARTIAL
			} else {
				kind = resp.Kind
			}
			req.ReplyAs(kind, resp.Reply())
		}, relayTimeout)

		if err != nil {
			req.ReplyAs(ERR, []byte(err.Error()))
		}

		// avoids sending the kind=REP when exiting
		req.DeferReply()
	})

}

// IcClosed checks for common text messages regarding a closed connection.
// Ugly but can't find another way :(
func isClosed(e error) bool {
	if e != nil {
		var s = e.Error()
		if strings.Contains(s, "EOF") ||
			strings.Contains(s, "use of closed network connection") {
			return true
		}
	}
	return false
}

type LoadBalancer interface {
	// Add is called when a new wire is created
	Add(*Wire)
	// Remove is called when the wire is killed
	Remove(*Wire)
	// Prepare is called before the message is sent
	Prepare(*Wire, Envelope)
	// Success is called when the message is sent successfuly
	Success(*Wire, Envelope)
	// Failure is called when a call fails
	Failure(*Wire, Envelope, error)
	//
	Next(string, []*Wire) *Wire
}
