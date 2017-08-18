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

	tk "github.com/quintans/toolkit"
	"github.com/quintans/toolkit/faults"
	"github.com/quintans/toolkit/log"
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
	NOCODEC   = errors.New("No codec defined")
	EOR       = errors.New("End Of Multiple Reply")
	NACKERROR = errors.New("Not Acknowledge Error")

	UNKNOWNTOPIC       = "No registered subscriber for %s."
	TIMEOUT            = "Timeout (%s) while waiting for reply of call #%d %s(%s)=%s"
	UNAVAILABLESERVICE = "No service is available for %s."
)

// Specific error types are define so that we can use type assertion, if needed
type UnknownTopic error
type TimeoutError error
type SystemError error
type ServiceUnavailableError error

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
				Kind:     msg.Kind,
				Name:     msg.Name,
				sequence: msg.Sequence,
			},
		},
		payload: msg.Payload,
	}

	return ctx
}

// Next calls the next handler
func (this *Request) Next() {
	if this.middlewarePos < len(this.middleware)-1 {
		this.middlewarePos++
		// call it
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
	Kind     EKind
	Sequence uint32
	Name     string
	Payload  []byte
	handler  func(ctx Response)
	timeout  time.Duration
	errch    chan error
	done     chan error
}

func callMe(env Envelope) bool {
	return test(env.Kind, SUB, UNSUB, REQ, REQALL, PUSH)
}

func (this Envelope) String() string {
	return fmt.Sprintf("{kind:%s, sequence:%d, name:%s, payload:%s, handler:%p, timeout:%s, errch:%p",
		this.Kind, this.Sequence, this.Name, this.Payload, this.handler, this.timeout, this.errch)
}

func (this Envelope) Reply(r Response) {
	if this.handler != nil {
		// if it is the last one,
		// the handler will take care of it
		this.handler(r)
		if r.Last() {
			// signal end of request all, canceling the timeout event
			this.done <- nil
		}
	} else if r.Kind == NACK {
		this.done <- NACKERROR
	} else {
		this.done <- nil
	}
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
	callbacks map[uint32]chan Response

	disconnect   func(net.Conn, error)
	findHandlers func(name string) []handler

	mutop          sync.RWMutex
	remoteGroupID  string // it is used for High Availability
	localGroupID   string // it is used for High Availability
	remoteUuid     tk.UUID
	remoteTopics   map[string]bool
	remoteMetadata map[string]interface{}
	OnSend         SendListener
	OnNewTopic     TopicListener
	OnDropTopic    TopicListener

	// load balancer failure policy
	Policy LBPolicy

	rateLimiter tk.Rate
	bufferSize  int
	logger      log.ILogger
}

func NewWire(codec Codec, l log.ILogger) *Wire {
	wire := &Wire{
		callbacks:    make(map[uint32]chan Response),
		timeout:      time.Second * 20,
		codec:        codec,
		remoteTopics: make(map[string]bool),
		bufferSize:   1000,
		logger:       l,
	}

	return wire
}

func (this *Wire) cloneTopics() []string {
	this.mutop.RLock()
	tps := make([]string, len(this.remoteTopics))
	var i = 0
	for k := range this.remoteTopics {
		tps[i] = k
		i++
	}
	this.mutop.RUnlock()

	return tps
}

func (this *Wire) RemoteMetadata() map[string]interface{} {
	return this.remoteMetadata
}

func (this *Wire) SetLogger(l log.ILogger) {
	this.logger = l
}

func (this *Wire) Logger() log.ILogger {
	return this.logger
}

func (this *Wire) SetBufferSize(size int) {
	this.bufferSize = size
}

func (this *Wire) SetRateLimiter(limiter tk.Rate) {
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
		go this.reader(c, this.handlerch)
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

		this.callbacks = make(map[uint32]chan Response)

		this.mutop.Lock()
		this.remoteTopics = make(map[string]bool)
		this.mutop.Unlock()
	}
}

func (this *Wire) Destroy() {
	if this.OnDropTopic != nil {
		var topics = this.cloneTopics()
		for _, name := range topics {
			// use trigger
			this.OnDropTopic(TopicEvent{this, name})
		}
	}

	this.stop()
	this.OnNewTopic = nil
	this.OnDropTopic = nil
}

func (this *Wire) addRemoteTopic(name string) {
	if this.OnNewTopic != nil {
		this.OnNewTopic(TopicEvent{this, name})
	}
	this.mutop.Lock()
	this.remoteTopics[name] = true
	this.mutop.Unlock()
}

func (this *Wire) deleteRemoteTopic(name string) {
	this.mutop.Lock()
	delete(this.remoteTopics, name)
	this.mutop.Unlock()

	if this.OnDropTopic != nil {
		this.OnDropTopic(TopicEvent{this, name})
	}
}

func (this *Wire) HasRemoteTopic(name string) bool {
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

func (this *Wire) getCallback(seq uint32, last bool) chan Response {
	this.mucb.Lock()
	defer this.mucb.Unlock()

	var msg = this.callbacks[seq]
	if last {
		delete(this.callbacks, seq)
	}
	return msg
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
	// check first if the peer has the topic before sending.
	// This is done because remote topics are only available after a connection
	if test(msg.Kind, PUB, PUSH, REQ, REQALL) && !this.HasRemoteTopic(msg.Name) {
		msg.errch <- UnknownTopic(fmt.Errorf(UNKNOWNTOPIC, msg.Name))
		return msg.errch
	}

	if this.OnSend != nil {
		this.OnSend(SendEvent{
			Kind:    msg.Kind,
			Name:    msg.Name,
			Payload: msg.Payload,
			Handler: msg.handler,
		})
	}

	return this.sendit(msg)
}

func (this *Wire) sendit(msg Envelope) <-chan error {

	msg.Sequence = atomic.AddUint32(&this.sequence, 1)
	if this.rateLimiter != nil {
		this.rateLimiter.TakeN(1)
	}

	this.enqueue(msg)
	return msg.errch
}

func (this *Wire) enqueue(msg Envelope) {
	this.mucb.Lock()
	defer this.mucb.Unlock()

	if this.chin != nil {
		// every call will have a new channel, specially when doing REQALL
		msg.done = make(chan error, 1)

		var respch chan Response
		if callMe(msg) {
			// can have several responses (partial or from multiple clients)
			respch = make(chan Response, 100)
			this.callbacks[msg.Sequence] = respch
			go handleReply(msg, respch)
		}

		go func() {
			select {
			case <-time.After(msg.timeout):
				this.delCallback(msg.Sequence)
				var e = TimeoutError(faults.New(TIMEOUT, msg.timeout, msg.Sequence, msg.Kind, msg.Name, msg.Payload))
				this.logger.Tracef("Timeout failure: %s", e)
				msg.errch <- e
			case err := <-msg.done:
				msg.errch <- err
			}
			if respch != nil {
				close(respch)
			}
		}()
		this.chin <- msg
	} else {
		msg.errch <- SystemError(faults.New("Closed connection. Unable to send %s", msg))
	}
}

func (this *Wire) reply(kind EKind, seq uint32, payload []byte) {
	var msg = Envelope{
		Kind:     kind,
		Sequence: seq,
		Payload:  payload,
		timeout:  this.timeout,
		errch:    make(chan error, 1),
	}

	this.enqueue(msg)
	go func() {
		err := <-msg.errch
		if err != nil {
			this.logger.Errorf("%v", err)
		}
	}()
}

// using a channel the messages are sent in order
func (this *Wire) writer(c net.Conn, chin chan Envelope) {
	for msg := range chin {
		this.logger.Debugf("Writing %s to %s", msg, c.RemoteAddr())
		var err = this.write(c, msg)
		if err != nil {
			msg.done <- faults.Wrapf(err, "Error while writing to %s", c.RemoteAddr())
			break
		} else if !callMe(msg) {
			// no callback
			msg.done <- nil
		}
		// else, the reply will be handled on read.
	}
	// exit
	this.disconnect(c, nil)
}

func (this *Wire) write(c net.Conn, msg Envelope) (err error) {
	c.SetWriteDeadline(timeouttime(msg.timeout))

	buf := bufio.NewWriter(c)
	w := NewOutputStream(buf)

	// kind
	err = w.WriteUI8(uint8(msg.Kind))
	if err != nil {
		return
	}
	// channel sequence
	err = w.WriteUI32(msg.Sequence)
	if err != nil {
		return
	}

	if test(msg.Kind, PUB, PUSH, SUB, UNSUB, REQ, REQALL) {
		// topic
		err = w.WriteString(msg.Name)
		if err != nil {
			return
		}
	}
	if test(msg.Kind, REQ, REQALL, PUB, PUSH, REP, REP_PARTIAL, ERR, ERR_PARTIAL) {
		// payload
		err = w.WriteBytes(msg.Payload)
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

func (this *Wire) reader(c net.Conn, handlerch chan EnvelopeConn) {
	r := NewInputStream(c)
	var err error

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
				this.reply(ACK, seq, nil)
			} else {
				name, data, err = read(r, true, true)
				if err != nil {
					break
				}

				handlerch <- EnvelopeConn{
					message: Envelope{
						Kind:     kind,
						Sequence: seq,
						Name:     name,
						Payload:  data,
					},
					conn: c,
				}
			}
		} else {
			switch kind {
			case REP_PARTIAL, REP:
				_, data, err = read(r, false, true)
				if err != nil {
					break
				}

			case ERR, ERR_PARTIAL:
				_, data, err = read(r, false, true)
				if err != nil {
					break
				}
				// reply was an error
				var s string
				if this.codec != nil {
					var e = this.codec.Decode(data, &s)
					if e != nil {
						this.logger.Errorf("Unable to decode %s\n%+v", data, faults.Wrap(e))
						s = fmt.Sprintf("Unable to decode %s; cause=%v", data, e)
					}
					data = []byte(s)
				}
			}

			this.logger.Debugf("Read Reply {kind: %s; seq: %d; data: %s} from %s", kind, seq, data, c.RemoteAddr())
			var respch = this.getCallback(seq, test(kind, REP, ERR))
			if respch != nil {
				respch <- NewResponse(this, c, kind, seq, data)
			} else {
				this.logger.Errorf("No callback found in %s for kind=%s, sequence=%d.", this.conn.LocalAddr(), kind, seq)
			}
		}
	}

	// if there was a bad read, we force a disconnect to start over
	this.disconnect(c, err)
}

func handleReply(msg Envelope, responses <-chan Response) {
	for r := range responses {
		msg.Reply(r)
	}
}

// Executes the function that handles the request. By default the reply of this handler is allways final (REP or ERR).
// If we wish  to send multiple replies, we must cancel the response and then send the several replies.
func (this *Wire) runHandler(handlerch chan EnvelopeConn) {
	for msgconn := range handlerch {
		msg := msgconn.message
		c := msgconn.conn

		if msg.Kind == REQ || msg.Kind == REQALL {
			// Serve
			var reply []byte
			var err error
			var deferReply bool
			var terminate bool
			var handlers = this.findHandlers(msg.Name)
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
				err = UnknownTopic(faults.New(UNKNOWNTOPIC, msg.Name))
			}

			// only sends a reply if there was some kind of return action
			if terminate {
				this.reply(ACK, msg.Sequence, nil)
			} else if !deferReply {
				if err != nil {
					msg.Kind = ERR
					if this.codec != nil {
						reply, _ = this.codec.Encode(err.Error())
					} else {
						reply = []byte(err.Error())
					}
				} else {
					msg.Kind = REP
				}
				this.reply(msg.Kind, msg.Sequence, reply)
			}
		} else { // PUSH & PUB
			var handlers = this.findHandlers(msg.Name)

			if msg.Kind == PUSH {
				if len(handlers) > 0 {
					msg.Kind = ACK
				} else {
					msg.Kind = NACK
				}
				this.reply(msg.Kind, msg.Sequence, nil)
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

func (this *Wire) RemoteUuid() tk.UUID {
	return this.remoteUuid
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
					ctx.fault = faults.Wrapf(e, "Unable to decode %s", ctx.reply)
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
		var params = make([]reflect.Value, 0)
		if hasContext {
			params = append(params, reflect.ValueOf(req))
		}
		if payloadType != nil {
			if codec != nil && req.payload != nil {
				var p reflect.Value
				if payloadType.Kind() == reflect.Slice &&
					payloadType.Elem().Kind() == reflect.Uint8 {
					p = reflect.ValueOf(req.payload)
				} else {
					p = reflect.New(payloadType).Elem()

					var e = codec.Decode(req.payload, p.Interface())
					if e != nil {
						req.SetFault(faults.Wrapf(e, "Unable to decode payload %s", req.payload))
						return
					}
				}
				params = append(params, p)
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
							err = faults.Wrap(err)
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

type ClientServerConfig struct {
}

type ClientServer struct {
	uuid     tk.UUID
	muhnd    sync.RWMutex
	handlers []handler

	muconn    sync.RWMutex
	OnConnect func(w *Wire)
	OnClose   func(c net.Conn)
	// this data will be transmited only on connect
	// and it will be available on the destination wire.
	metadata map[string]interface{}
}

func NewClientServer() ClientServer {
	var uuid = tk.NewUUID()
	return ClientServer{
		handlers: make([]handler, 0),
		uuid:     uuid,
		metadata: make(map[string]interface{}),
	}
}

func (this *ClientServer) Metadata() map[string]interface{} {
	return this.metadata
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
		Kind:    kind,
		Name:    name,
		handler: handler,
		timeout: timeout,
		errch:   make(chan error, 1),
	}

	if payload != nil {
		switch m := payload.(type) {
		case []byte:
			msg.Payload = m
		case *Msg:
			msg.Payload = m.buffer.Bytes()
		default:
			var err error
			msg.Payload, err = codec.Encode(payload)
			if err != nil {
				return Envelope{}, faults.Wrap(err)
			}
		}
	}

	return msg, nil
}

type Client struct {
	ClientServer
	*Wire

	addr                 string
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
		Wire:                 NewWire(JsonCodec{}, Log{}),
		sendListeners:        make(map[uint64]SendListener),
		newTopicListeners:    make(map[uint64]TopicListener),
		dropTopicListeners:   make(map[uint64]TopicListener),
		defaultTimeout:       time.Second * 5,
	}
	this.ClientServer = NewClientServer()

	this.Wire.findHandlers = func(name string) []handler {
		this.muhnd.RLock()
		defer this.muhnd.RUnlock()

		return findHandlers(name, this.handlers)
	}

	this.Wire.disconnect = func(c net.Conn, e error) {
		this.muconn.Lock()
		defer this.muconn.Unlock()

		// Since this can be called from several places at the same time (reader goroutine, Reconnect(), ),
		// we must check if it is still the same connection
		if this.Wire.conn == c {
			// handle errors during a connection
			if faults.Has(e, io.EOF) || isClosed(e) {
				this.logger.Debugf("%s closed connection to %s", c.LocalAddr(), c.RemoteAddr())
			} else if e != nil {
				this.logger.Errorf("Client %s droped with error: %+v", c.RemoteAddr(), faults.Wrap(e))
			}

			this.Wire.Destroy()
			if this.OnClose != nil {
				this.OnClose(c)
			}
			this.Wire.conn = nil
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
	if this.Wire.OnSend == nil {
		this.Wire.OnSend = func(event SendEvent) {
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
	if this.Wire.OnNewTopic == nil {
		this.Wire.OnNewTopic = func(event TopicEvent) {
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
	if this.Wire.OnDropTopic == nil {
		this.Wire.OnDropTopic = func(event TopicEvent) {
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

func (this *Client) SetReconnectInterval(reconnectInterval time.Duration) *Client {
	this.reconnectInterval = reconnectInterval
	return this
}

func (this *Client) SetReconnectMaxInterval(reconnectMaxInterval time.Duration) *Client {
	this.reconnectMaxInterval = reconnectMaxInterval
	return this
}

func (this *Client) SetCodec(codec Codec) *Client {
	this.Wire.codec = codec
	return this
}

// Make it belong to a group.
// Only one element at a time (round-robin) handles the messages.
func (this *Client) SetGroupId(groupId string) *Client {
	this.Wire.localGroupID = groupId
	return this
}

func (this *Client) handshake(c net.Conn) error {
	this.muhnd.RLock()
	var size = len(this.handlers) + 1
	var payload = make([]string, size)
	payload[0] = this.Wire.localGroupID
	i := 1
	for _, v := range this.handlers {
		payload[i] = v.rule
		i++
	}
	this.muhnd.RUnlock()

	err := serializeHanshake(c, this.Wire.codec, time.Second, this.uuid, payload, this.metadata)
	if err != nil {
		return err
	}

	// get reply
	uuid, group, remoteTopics, metadata, err := deserializeHandshake(c, this.Wire.codec, time.Second)
	if err != nil {
		return err
	}
	this.Wire.mutop.Lock()
	this.Wire.remoteUuid = uuid
	this.Wire.remoteTopics = remoteTopics
	this.Wire.remoteMetadata = metadata
	for k := range remoteTopics {
		this.fireNewTopicListeners(TopicEvent{this.Wire, k})
	}
	this.Wire.remoteGroupID = group
	this.Wire.mutop.Unlock()

	return nil
}

func serializeHanshake(
	c net.Conn,
	codec Codec,
	timeout time.Duration,
	uuid tk.UUID,
	payload []string,
	metadata map[string]interface{},
) error {
	var data, err = codec.Encode(payload)
	if err != nil {
		return faults.Wrap(err)
	}
	var meta []byte
	meta, err = codec.Encode(metadata)
	if err != nil {
		return faults.Wrap(err)
	}

	c.SetWriteDeadline(timeouttime(timeout))

	var w = bufio.NewWriter(c)
	var os = NewOutputStream(w)
	// uuid
	_, err = os.writer.Write(uuid.Bytes())
	if err != nil {
		return faults.Wrap(err)
	}
	err = os.WriteBytes(data)
	if err != nil {
		return err
	}
	err = os.WriteBytes(meta)
	if err != nil {
		return err
	}
	return w.Flush()
}

func deserializeHandshake(c net.Conn, codec Codec, timeout time.Duration) (tk.UUID, string, map[string]bool, map[string]interface{}, error) {
	c.SetReadDeadline(timeouttime(timeout))

	r := NewInputStream(c)
	// remote uuid
	var data, err = r.ReadNBytes(tk.UUID_SIZE)
	if err != nil {
		return tk.UUID{}, "", nil, nil, faults.Wrap(err)
	}
	var uuid, _ = tk.ToUUID(data)
	// group id + topics
	data, err = r.ReadBytes()
	if err != nil {
		return tk.UUID{}, "", nil, nil, faults.Wrap(err)
	}
	// metada
	var meta []byte
	meta, err = r.ReadBytes()
	if err != nil {
		return tk.UUID{}, "", nil, nil, faults.Wrap(err)
	}

	// DECODING
	// group id + topics
	var topics []string
	err = codec.Decode(data, &topics)
	if err != nil {
		return tk.UUID{}, "", nil, nil, faults.Wrap(err)
	}

	var identity = topics[0]
	topics = topics[1:]
	var remoteTopics = make(map[string]bool)
	for _, v := range topics {
		remoteTopics[v] = true
	}
	// metada
	var metadata = make(map[string]interface{})
	err = codec.Decode(meta, &metadata)
	if err != nil {
		return tk.UUID{}, "", nil, nil, faults.Wrap(err)
	}

	return uuid, identity, remoteTopics, metadata, nil
}

// connect is seperated to allow the definition and use of OnConnect
func (this *Client) Connect(addr string) <-chan error {
	this.addr = addr
	var cherr = make(chan error, 1)
	go this.dial(this.reconnectInterval, cherr)
	return cherr
}

// Address returns the Server address
func (this *Client) Address() string {
	return this.addr
}

func (this *Client) Reconnect() <-chan error {
	this.Disconnect()
	return this.Connect(this.addr)
}

func (this *Client) Disconnect() {
	if this.Wire != nil {
		this.Wire.disconnect(this.Wire.conn, nil)
	}
}

func (this *Client) Destroy() {
	this.reconnectInterval = 0
	this.Wire.Destroy()
	this.sendListeners = make(map[uint64]SendListener)
	this.newTopicListeners = make(map[uint64]TopicListener)
	this.dropTopicListeners = make(map[uint64]TopicListener)
	atomic.StoreUint64(&this.listenersIdx, 0)
	this.Disconnect()
}

// Active check if this wire is running, i.e., if it has a connection
func (this *Client) Active() bool {
	return this.Wire.conn != nil
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
			this.logger.Tracef("Failed to connect to %s", this.addr)
			if this.reconnectInterval > 0 {
				this.logger.Tracef("retry connecting to %s in %v", this.addr, retry)
				time.Sleep(retry)
				if this.reconnectMaxInterval > 0 && retry < this.reconnectMaxInterval {
					retry = retry * 2
					if retry > this.reconnectMaxInterval {
						retry = this.reconnectMaxInterval
					}
				}
			} else {
				this.logger.Debugf("NO retry will be performed to %s!", this.addr)
				return
			}
		} else {
			break
		}
	}

	this.logger.Debugf("Connected to %s", this.addr)

	this.muconn.Lock()
	defer this.muconn.Unlock()

	// topic exchange
	err = this.handshake(c)
	if err != nil {
		//this.logger.Errorf("%s: error while handshaking %s: %+v", c.LocalAddr(), this.addr, err)
		faults.Wrapf(err, "Error while handshaking %s", this.addr)
		this.Wire.SetConn(nil)
		c.Close()
	} else {
		this.Wire.SetConn(c)

		if this.OnConnect != nil {
			this.OnConnect(this.Wire)
		}
	}
	cherr <- err
}

// name can have an '*' at the end, meaning that it will handle messages
// with the destiny name starting with the reply name whitout the '*'.
// When handling request messages, the function handler can have a return value and/or an error.
// When handling publish/push messages, any return from the function handler is discarded.
// When handling Request/RequestAll messages, if a return is not specified,
// the caller will not receive a reply until you explicitly call gomsg.Request.ReplyAs()
func (this *Client) Handle(name string, middlewares ...interface{}) {
	this.logger.Infof("Registering handler for %s", name)

	var size = len(middlewares)
	var hnds = make([]Middleware, size)
	for i := 0; i < size; i++ {
		hnds[i] = CreateRequestHandler(this.Wire.codec, middlewares[i])
	}
	this.addHandler(name, hnds)

	this.muconn.Lock()
	defer this.muconn.Unlock()

	if this.Wire.conn != nil {
		msg, _ := createEnvelope(SUB, name, nil, nil, time.Second, this.Wire.codec)
		this.Wire.Send(msg)
	}
}

func (this *Client) Cancel(name string) {
	this.removeHandler(name)

	this.muconn.Lock()
	defer this.muconn.Unlock()

	if this.Wire.conn != nil {
		msg, _ := createEnvelope(UNSUB, name, nil, nil, time.Second, this.Wire.codec)
		this.Wire.Send(msg)
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
	msg, err := createEnvelope(kind, name, payload, handler, timeout, this.Wire.codec)
	if err != nil {
		ch := make(chan error, 1)
		ch <- err
		return ch
	}
	return this.Wire.Send(msg)
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
	rateLimiterFactory func() tk.Rate
	bufferSize         int
	defaultTimeout     time.Duration
	logger             log.ILogger
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
	Wire *Wire
	// Name is the topic name
	Name string
}

func (e TopicEvent) String() string {
	return fmt.Sprintf("{Name: %q, SourceAddr: %q, Source: %s}", e.Name, e.Wire.Conn().RemoteAddr(), e.Wire.remoteUuid)
}

// NewWires creates a Wires structure
func NewWires(codec Codec, l log.ILogger) *Wires {
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
		logger:             Log{},
	}
}

func (this *Wires) SetLogger(l log.ILogger) {
	this.logger = l
}

func (this *Wires) Logger() log.ILogger {
	return this.logger
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

func (this *Wires) SetRateLimiterFactory(factory func() tk.Rate) {
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

	// if already defined (same connection) panics
	for _, v := range this.wires {
		if v.conn == wire.conn {
			panic("Already exists a Wire with the same connection.")
		}
	}

	this.wires = append(this.wires, wire)

	var group = this.groups[wire.remoteGroupID]
	if group == nil {
		group = make([]*Wire, 0)
	}
	this.groups[wire.remoteGroupID] = append(group, wire)

	this.Unlock()

	wire.mutop.RLock()
	// set common codec
	wire.codec = this.codec
	if this.rateLimiterFactory != nil {
		wire.rateLimiter = this.rateLimiterFactory()
	}

	// define trigger on new topic
	wire.OnNewTopic = func(event TopicEvent) {
		this.fireNewTopicListener(event)
	}
	// define trigger on drop topic
	wire.OnDropTopic = func(event TopicEvent) {
		this.fireDropTopicListener(event)
	}

	this.loadBalancer.Add(wire)

	var topics = wire.cloneTopics()
	for _, name := range topics {
		// use trigger
		wire.OnNewTopic(TopicEvent{wire, name})
	}
	wire.mutop.RUnlock()
}

// TopicCount returns the number of clients providing the topic
func (this *Wires) TopicCount(name string) int {
	this.RLock()
	var count = 0
	for _, v := range this.wires {
		if v.HasRemoteTopic(name) {
			count++
		}
	}
	this.RUnlock()
	return count
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

	return cloneWires(this.wires)
}

func cloneWires(wires []*Wire) []*Wire {
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
	// fires all registered listeners
	this.fireSendListener(SendEvent{
		Kind:    msg.Kind,
		Name:    msg.Name,
		Payload: msg.Payload,
		Handler: msg.handler,
	})

	return w.sendit(msg)
}

func wiresTriage(msg Envelope, wires []*Wire, skipWire *Wire) ([]*Wire, error) {
	var ws = make([]*Wire, 0)
	for _, w := range wires {
		// does not send to self
		if (skipWire == nil || skipWire != w) && w.HasRemoteTopic(msg.Name) {
			ws = append(ws, w)
		}
	}
	if len(ws) == 0 {
		return nil, UnknownTopic(fmt.Errorf(UNKNOWNTOPIC, msg.Name))
	}
	return ws, nil
}

func wiresTriageLB(lb LoadBalancer, msg Envelope, wires []*Wire, skipWire *Wire) ([]*Wire, error) {
	var ws, err = wiresTriage(msg, wires, skipWire)
	if err != nil {
		return nil, err
	}
	ws, err = lb.PickAll(msg, ws)
	if err != nil {
		return nil, err
	}

	return ws, nil
}

// SendSkip is the generic function to send messages with the possibility of ignoring the sender
func (this *Wires) SendSkip(skipWire *Wire, kind EKind, name string, payload interface{}, handler interface{}, timeout time.Duration) <-chan error {
	var msg, ex = createEnvelope(kind, name, payload, handler, timeout, this.codec)
	errch := make(chan error, 1)
	if ex != nil {
		errch <- ex
		return errch
	}

	var defErr = ServiceUnavailableError(fmt.Errorf(UNAVAILABLESERVICE, msg.Name))
	if msg.Kind == PUSH || msg.Kind == REQ {
		go func() {
			this.RLock()
			var wires, err = wiresTriage(msg, this.wires, skipWire)
			this.RUnlock()
			if err != nil {
				defErr = err
			} else {
				var size = len(wires)
				for i := 0; i < size; i++ {
					var w, err = this.loadBalancer.PickOne(msg, wires)
					if err != nil {
						defErr = err
						break
					}
					// REQ can also receive multiple messages from ONE replier
					err = <-this.send(w, msg)
					this.loadBalancer.Done(w, msg, err)
					// exit on success
					if err == nil {
						defErr = nil
						break
					} else {
						this.logger.Debugf("Failed to send %s to %s. Cause=%s", msg, w.Conn().RemoteAddr(), err)
					}
				}
			}
			errch <- this.loadBalancer.AllDone(msg, defErr)
		}()
	} else if msg.Kind == PUB {
		var wg sync.WaitGroup
		var errs = faults.Errors(make([]error, 0))

		for id, group := range this.groups {
			var ws, err = wiresTriageLB(this.loadBalancer, msg, group, skipWire)
			if err != nil {
				errs.Add(err)
				defErr = errs
				continue
			}
			if id == noGROUP {
				wg.Add(1)
				go func() {
					for _, w := range ws {
						var e = <-this.send(w, msg)
						this.loadBalancer.Done(w, msg, e)
						if err == nil {
							// At least one got through.
							// Ignoring all errors
							defErr = nil
						} else {
							this.logger.Debugf("Failed to send %s to %s. Cause=%s", msg, w.Conn().RemoteAddr(), e)
						}
					}
					wg.Done()
				}()
			} else {
				wg.Add(1)
				go func(wires []*Wire) {
					for _, w := range wires {
						var err = <-this.send(w, msg)
						this.loadBalancer.Done(w, msg, err)
						// send only to one.
						// stop if there was a success.
						if err == nil {
							// Ignoring errors
							defErr = nil
							break
						} else {
							this.logger.Debugf("Failed to send %s to %s. Cause=%s", msg, w.Conn().RemoteAddr(), err)
						}
					}
					wg.Done()
				}(ws)
			}
		}
		go func() {
			wg.Wait()
			errch <- this.loadBalancer.AllDone(msg, defErr)
		}()

	} else if msg.Kind == REQALL {
		var wg sync.WaitGroup
		var errs = faults.Errors(make([]error, 0))

		// we wrap the reply handler so that we can control the number o replies delivered.
		var handler = msg.handler
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
				// recovers the kind
				ctx.Kind = kind
			}
		}

		// collects wires into groups.
		for id, group := range this.groups {
			var ws, err = wiresTriageLB(this.loadBalancer, msg, group, skipWire)
			if err != nil {
				errs.Add(err)
				defErr = errs
				continue
			}
			if id == noGROUP {
				for _, wire := range ws {
					// increment reply counter
					wg.Add(1)
					go func(w *Wire) {
						// waits for the request completion
						var e = <-this.send(w, msg)
						this.loadBalancer.Done(w, msg, e)
						if e == nil {
							// at least one got through
							// Ignoring errors
							defErr = nil
						} else {
							this.logger.Errorf("Failed requesting all: %+v", e)
						}
						wg.Done()
					}(wire)
				}
			} else {
				// increment reply counter
				wg.Add(1)
				go func(wires []*Wire) {
					for _, w := range ws {
						this.logger.Tracef("sending message to %s (group %s)", w.Conn().RemoteAddr(), id)
						// waits for the request completion
						e := <-this.send(w, msg)
						// send only to one.
						// stop if there was a success.
						this.loadBalancer.Done(w, msg, e)
						if e == nil {
							// Ignoring errors
							defErr = nil
							break
						}
					}
					wg.Done()
				}(ws)
			}
		}
		go func() {
			// Wait for all requests to complete.
			wg.Wait()
			this.logger.Tracef("all requests finnished for %s", msg)
			// pass the end mark
			if handler != nil {
				handler(NewResponse(skipWire, nil, ACK, 0, nil))
			}
			errch <- this.loadBalancer.AllDone(msg, defErr)
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
	server.Wires = NewWires(JsonCodec{}, Log{})
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
func (this *Server) BindAddress() net.Addr {
	if this.listener != nil {
		return this.listener.Addr()
	}
	return nil
}

// BindPort returns the listener port
func (this *Server) BindPort() int {
	if this.listener != nil {
		return this.listener.Addr().(*net.TCPAddr).Port
	}
	return 0
}

func (this *Server) SetBufferSize(size int) {
	this.Wires.SetBufferSize(size)
}

func (this *Server) SetRateLimiterFactory(factory func() tk.Rate) {
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
		cherr <- faults.Wrapf(err, "Unable to bind to %s", service)
		return cherr
	}
	this.listener = l
	this.fireBindListeners(l)
	this.logger.Tracef("listening at %s", l.Addr())
	go func() {
		for {
			// notice that c is changed in the disconnect function
			c, err := l.Accept()
			if err != nil {
				// happens when the listener is closed
				cherr <- faults.Wrapf(err, "Stoped listening at %s", l.Addr())
				return
			}
			wire := NewWire(this.codec, this.logger)
			wire.findHandlers = this.findHandlers

			// topic exchange
			group, remoteTopics, err := this.handshake(c, wire)
			this.logger.Tracef("%s: accepted connection from %s", l.Addr(), c.RemoteAddr())

			if err != nil {
				this.logger.Errorf("Failed to handshake. Rejecting connection: %+v", err)
				c.Close()
			} else {
				wire.remoteGroupID = group
				wire.remoteTopics = remoteTopics
				wire.disconnect = func(conn net.Conn, e error) {
					this.muconn.Lock()
					defer this.muconn.Unlock()

					// check to see if we are disconnecting the same connection
					if c == conn {
						// handle errors during a connection
						if faults.Has(e, io.EOF) || isClosed(e) {
							this.logger.Debugf("%s closed connection to %s", conn.LocalAddr(), conn.RemoteAddr())
						} else if e != nil {
							this.logger.Errorf("Client %s droped with error: %+v", conn.RemoteAddr(), faults.Wrap(e))
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
	uuid, group, payload, metadata, err := deserializeHandshake(c, wire.codec, time.Second)
	if err != nil {
		return "", nil, err
	}

	wire.remoteUuid = uuid
	wire.remoteMetadata = metadata
	this.logger.Debugf("Remote Metadata: %v", metadata)

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

	err = serializeHanshake(c, wire.codec, time.Second, this.uuid, topics, this.metadata)
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
	this.logger.Infof("Registering handler for %s", name)
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

// isClosed checks for common text messages regarding a closed connection.
// Ugly but can't find another way :(
func isClosed(e error) bool {
	if e != nil {
		var s = faults.Cause(e).Error()
		if strings.Contains(s, "EOF") ||
			strings.Contains(s, "use of closed network connection") {
			return true
		}
	}
	return false
}

type LoadBalancer interface {
	SetPolicyFactory(func() LBPolicy)
	// Add is called when a new wire is created
	Add(*Wire)
	// Remove is called when the wire is killed
	Remove(*Wire)
	// PickOne is called before the message is sent
	PickOne(Envelope, []*Wire) (*Wire, error)
	// PickAll is called before the message is sent
	PickAll(Envelope, []*Wire) ([]*Wire, error)
	// Done is called when we are done with one wire
	Done(*Wire, Envelope, error)
	// AllDone is called when ALL wires have been processed
	AllDone(Envelope, error) error
}

type LBPolicy interface {
	IncLoad(string) uint64
	Load(string) uint64
	// Failed is called when an error ocurrs.
	Failed(string)
	// Failed is called when a success ocurrs.
	Succeeded(string)
	// InQuarantine returns if it is in quarantine
	InQuarantine(string) bool
}
