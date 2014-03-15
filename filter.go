package gobus

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/quintans/toolkit/log"
)

var logger = log.LoggerFor("gobus")

type Action uint8

const (
	ACT_ASK    Action = iota + 1 // asking if client can accept a request
	ACT_PUBONE                   // publishing messages to ONE subscriber (queue).
	ACT_PUBALL                   // publishing messages to ALL subscribers
	ACT_REG                      // registering an endpoint for subscriptions and requests
	ACT_REGS                     // registering the client's subscriptions and requests replyers
	ACT_REQONE                   // requesting for only ONE reply of ONE of the producers
	ACT_REQALL                   // requesting for ALL the replies of ALL of the producers
)

func (this Action) String() string {
	switch this {
	case ACT_ASK:
		return "ACT_ASK"
	case ACT_PUBONE:
		return "ACT_PUBONE"
	case ACT_PUBALL:
		return "ACT_PUBALL"
	case ACT_REG:
		return "ACT_REG"
	case ACT_REGS:
		return "ACT_REGS"
	case ACT_REQONE:
		return "ACT_REQONE"
	case ACT_REQALL:
		return "ACT_REQALL"
	}
	return ""
}

type MsgKind uint8

// kind
const (
	NONE          MsgKind = iota // just send and do not expect a REPLY
	REQUEST                      // REQUEST a REPLY from the client
	REPLY                        // sending a REPLY to a REQUEST
	REPLY_PARTIAL                // sending a PARTIAL REPLY to a REQUEST of multiple replies
	ERROR                        // ERROR for a REQUEST
	ERROR_PARTIAL                // ERROR for a REQUEST of multiple replies
)

func (this MsgKind) String() string {
	switch this {
	case NONE:
		return "NONE"
	case REQUEST:
		return "REQUEST"
	case REPLY:
		return "REPLY"
	case REPLY_PARTIAL:
		return "REPLY_PARTIAL"
	case ERROR:
		return "ERROR"
	case ERROR_PARTIAL:
		return "ERROR_PARTIAL"
	}
	return ""
}

type Payload struct {
	Kind   MsgKind
	Header []byte // partialy implemented
	Data   []byte
}

type callback struct {
	timer         *time.Timer
	success       func(Payload)
	remoteFailure func(Payload)
	completed     func(error)
}

type Response struct {
	Kind  MsgKind
	Head  io.Writer
	Body  io.Writer
	Flush func()
}

type Communication struct {
	Emiter interface{}

	Version   uint8
	Kind      MsgKind
	Id        uint32
	Timestamp time.Time
	Action    Action
	Name      string
	Header    []byte
	Data      []byte
	callback  *callback
}

type IContext interface {
	Proceed() error
	GetResponse() *Response
	GetRequest() *Communication
	GetAttribute(string) interface{}
	SetAttribute(string, interface{})
	GetCurrentFilter() *Filter
	SetCurrentFilter(*Filter)
}

func NewContext(w *Response, r *Communication) *Context {
	this := new(Context)
	this.Init(this, w, r)
	return this
}

type Context struct {
	Response      *Response
	Request       *Communication
	Attributes    map[string]interface{} // attributes only valid in this request
	CurrentFilter *Filter
	overrider     IContext
}

func (this *Context) Init(ctx IContext, w *Response, r *Communication) {
	this.overrider = ctx
	this.Response = w
	this.Request = r
	this.Attributes = make(map[string]interface{})
}

func (this *Context) Proceed() error {
	next := this.CurrentFilter.next
	if next != nil {
		this.CurrentFilter = next
		// is this filter applicable?
		if next.IsValid(this.overrider) {
			return next.handlerFunc(this.overrider)
		} else {
			// proceed to next filter
			return this.overrider.Proceed()
		}
	}
	return nil
}

func (this *Context) GetResponse() *Response {
	return this.Response
}

func (this *Context) GetRequest() *Communication {
	return this.Request
}

func (this *Context) GetAttribute(key string) interface{} {
	return this.Attributes[key]
}

func (this *Context) SetAttribute(key string, value interface{}) {
	this.Attributes[key] = value
}

func (this *Context) GetCurrentFilter() *Filter {
	return this.CurrentFilter
}

func (this *Context) SetCurrentFilter(current *Filter) {
	this.CurrentFilter = current
}

type Filter struct {
	rule        string
	next        *Filter
	handlerFunc func(ctx IContext) error
}

func (this *Filter) IsValid(ctx IContext) bool {
	path := ctx.GetRequest().Name

	if this.rule == "" {
		return true
	} else if strings.HasPrefix(this.rule, "*") {
		return strings.HasSuffix(path, this.rule[1:])
	} else if strings.HasSuffix(this.rule, "*") {
		return strings.HasPrefix(path, this.rule[:len(this.rule)-1])
	} else {
		return path == this.rule
	}
}

// DO NOT FORGET: filters are applied in reverse order (LIFO)
func NewFilterHandler(contextFactory func(*Response, *Communication) IContext) *FilterHandler {
	this := new(FilterHandler)
	this.contextFactory = contextFactory
	return this
}

type FilterHandler struct {
	first          *Filter
	contextFactory func(*Response, *Communication) IContext
}

type IServer interface {
	ServeStream(*Response, *Communication) error
}

func (this *FilterHandler) ServeStream(resp *Response, r *Communication) error {
	if this.first != nil {
		var ctx IContext
		if this.contextFactory == nil {
			// default
			ctx = NewContext(resp, r)
		} else {
			ctx = this.contextFactory(resp, r)
		}
		ctx.SetCurrentFilter(&Filter{next: this.first})
		return ctx.Proceed()
	}
	return nil
}

// the last added filter will be the first to be called
func (this *FilterHandler) Push(rule string, filters ...func(ctx IContext) error) {
	for _, filter := range filters {
		current := &Filter{
			rule:        rule,
			next:        this.first,
			handlerFunc: filter,
		}
		this.first = current
	}
}

func MakeFilter(returnable bool, handler interface{}, codec Codec) (func(ctx IContext) error, error) {
	function := reflect.ValueOf(handler)
	typ := function.Type()
	if typ.Kind() != reflect.Func {
		return nil, errors.New("Supplied instance must be a Function.")
	}

	// validate argument types
	var hasContext = false
	size := typ.NumIn()
	if size > 2 {
		return nil, errors.New("Invalid function. The function can only have at the most two  parameters.")
	} else if size > 1 {
		t := typ.In(0)
		if t != contextType {
			return nil, errors.New("Invalid function. In a two paramater handler function the first must be the interface gobus.IContext.")
		}
	}

	var payloadType reflect.Type
	if size == 2 {
		payloadType = typ.In(1)
		hasContext = true
	} else if size == 1 {
		t := typ.In(0)
		if t != contextType {
			payloadType = t
		} else {
			hasContext = true
		}
	}

	// validate return types
	size = typ.NumOut()
	if !returnable && size > 0 {
		return nil, errors.New("Invalid handler function. Handler function must not return.")
	} else if size > 2 {
		return nil, errors.New("Invalid handler function. Handler functions can only have at the most two return values.")
	} else if size > 1 && errorType != typ.Out(1) {
		return nil, errors.New("Invalid handler function. In a two return values actions the second must be an error.")
	}

	return func(ctx IContext) error {
		params := make([]reflect.Value, 0)
		if hasContext {
			params = append(params, reflect.ValueOf(ctx))
		}

		if payloadType != nil {
			payload := ctx.GetRequest().Data
			p := reflect.New(payloadType)
			codec.Decode(payload, p.Interface())
			params = append(params, p.Elem())
		}
		results := function.Call(params)

		// check for error
		r := ctx.GetResponse()
		var data interface{}
		for k, v := range results {
			if v.Type() == errorType {
				if !v.IsNil() {
					data = v.Interface()
					r.Kind = ERROR
				}
				break
			} else {
				// stores the result to return at the end of the check
				data = results[k].Interface()
				r.Kind = REPLY
			}
		}

		if data != nil {
			result, err := codec.Encode(data)
			if err != nil {
				logger.Errorf("An error ocurred when marshalling the response/error from %s\n\tresponse: %v\n\terror: %s\n", ctx.GetRequest().Name, data, err)
				return err
			}
			r.Body.Write(result)
		}

		return nil
	}, nil
}
