package gobus

import (
	"net"
	"sync"

	//"code.google.com/p/go-uuid/uuid"
)

var NoProviderFault = NewFault(ERR_NO_PROVIDER, "No provider available")
var NoReliableProviderFault = NewFault(ERR_NO_PROVIDER, "No reliable provider available")

const (
	ERR_NO_PROVIDER = "BUS01"

	ASK_CMD    = "ask:"              // prefix for asking if client can accept a request
	PUB_CMD    = "pub:"              // prefix for publishing messages
	REG_CMD    = "reg:"              // prefix for registering an endpoint for subscriptions and requests
	CMD_REGS   = "cmd:registrations" // prefix for asking the client for its subscriptions and requests replyers
	REQONE_CMD = "reqone:"           // prefix for requesting for only one reply of one of the producers
	REQALL_CMD = "reqall:"           // prefix for requesting for all the replies of all of the producers
)

type Pipe struct {
	Wire   *Wire
	Topics map[string]bool // useful when removing a client, so that whe know waht topics to consider
}

type Server struct {
	mu       sync.Mutex
	filter   *FilterHandler
	codec    Codec
	clients  map[net.Conn]*Pipe            // maps the connection with the consumer instance
	registry map[string]map[net.Conn]*Wire // maps all connections under a topic
}

func NewServer(codec Codec) *Server {
	this := new(Server)

	this.clients = make(map[net.Conn]*Pipe)
	this.registry = make(map[string]map[net.Conn]*Wire)

	if codec == nil {
		this.codec = JsonCodec{}
	} else {
		this.codec = codec
	}

	this.filter = new(FilterHandler)
	// register publisher dispacher filter
	this.filter.Push(CMD_REGS, this.registrations)
	this.filter.Push(REG_CMD+"*", this.register)
	this.filter.Push(REQALL_CMD+"*", this.answerAll)
	this.filter.Push(REQONE_CMD+"*", this.answerOne)
	this.filter.Push(PUB_CMD+"*", this.broadcast)

	return this
}

func (this *Server) getConsumers(name string) []*Wire {
	this.mu.Lock()
	defer this.mu.Unlock()

	wires := this.registry[name]
	consumers := make([]*Wire, 0)
	if wires != nil {
		for _, wire := range wires {
			consumers = append(consumers, wire)
		}
	}
	return consumers
}

func (this *Server) AddFilter(rule string, filters ...func(ctx IContext) error) {
	for _, filter := range filters {
		this.filter.Push(rule, filter)
	}
}

func (this *Server) Start(protocol string, port string) error {
	listener, err := net.Listen(protocol, ":"+port)
	if err != nil {
		return err
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Errorf("%s", err.Error())
			continue
		}
		go this.handleConnection(conn, this.filter)
	}
	return nil
}

func (this *Server) Stop() {
	this.mu.Lock()
	defer this.mu.Unlock()

	for _, client := range this.clients {
		client.Wire.Stop()
	}
	this.clients = make(map[net.Conn]*Pipe)
}

func (this *Server) handleConnection(c net.Conn, server IServer) {
	client := NewWire(c, server)

	client.OnDisconnect = func() {
		//log.Printf("Connection from %v closed.\n", c.RemoteAddr())
		this.removeClient(client)
	}

	this.addClient(client)
}

func (this *Server) answerAll(ctx IContext) error {
	comm := ctx.GetRequest()
	name := comm.Name[len(REQALL_CMD):]
	emiter := comm.Emiter.(*Wire)
	logger.Debugf("Requesting ALL '%s's", name)
	clients := this.getConsumers(name)
	count := len(clients)
	answerChan := make(chan []byte)
	errorChan := make(chan []byte)
	for _, wire := range clients {
		// Does not send to the same client
		if wire.Connection != emiter.Connection {
			// request all endpoints for their data
			go func(wire *Wire, name string, data []byte) {
				wire.Send(
					name,
					data,
					func(answer Payload) {
						logger.Debugf("One of Many replies to %s", name)
						answerChan <- answer.Data
					},
					func(e Payload) {
						errorChan <- e.Data
					},
					func(fault error) {
						e, err := this.codec.Encode(NewFaultError(fault))
						if err != nil {
							logger.Errorf("%s", fault.Error())
						}
						errorChan <- e
					})

			}(wire, name, comm.Data)
		}
	}
	r := ctx.GetResponse()
	for {
		select {
		case data := <-answerChan:
			r.Body.Write(data)
			count--
			if count == 0 {
				logger.Debugf("Last reply of Many for %s", name)
				r.Kind = REPLY // this will close the communication
				return nil
			} else {
				r.Kind = REPLY_PARTIAL
				r.Flush()
			}

		case data := <-errorChan:
			r.Body.Write(data)
			count--
			if count == 0 {
				r.Kind = ERROR // this will close the communication
				return nil
			} else {
				r.Kind = ERROR_PARTIAL
				r.Flush()
			}
		}
	}
	return nil
}

func (this *Server) answerOne(ctx IContext) error {
	comm := ctx.GetRequest()
	name := comm.Name[len(REQONE_CMD):]
	emiter := comm.Emiter.(*Wire)
	logger.Debugf("Requesting ONE '%s'", name)
	clients := this.getConsumers(name)
	count := len(clients)
	clientChan := make(chan *Wire)
	// ask all endpoints if they accept a request
	for _, wire := range clients {
		// Does not send to the same client
		if wire.Connection != emiter.Connection {
			go func(wire *Wire, name string, data []byte) {
				logger.Debugf("asking %s to reply", wire.Connection.RemoteAddr())
				wire.Call(
					ASK_CMD+name,
					nil,
					func(ok bool) {
						if ok {
							logger.Debugf("%s has accepted to reply", wire.Connection.RemoteAddr())
							clientChan <- wire
						} else {
							logger.Debugf("%s has denied to reply", wire.Connection.RemoteAddr())
							clientChan <- nil
						}
					},
					func(e error) {
						clientChan <- nil
					},
					this.codec,
					false)

			}(wire, name, comm.Data)
		}
	}

	var foundClient *Wire
	var others = make([]*Wire, 0)
	// finds the first one
	for wire := range clientChan {
		count--
		if wire != nil {
			logger.Debugf("choosing %s to reply", wire.Connection.RemoteAddr())
			foundClient = wire
			// this will handle the remaining clients
			if count > 0 {
				logger.Debugf("collecting the remaining %v client(s)", count)
				go func(cc chan *Wire, cnt int) {
					for other := range clientChan {
						others = append(others, other)
						cnt--
						if count == 0 {
							return
						}
					}
				}(clientChan, count)
			}
			break
		}
		// no one accepted
		if count == 0 {
			logger.Debugf("no one accepted to reply to '%s' :(", name)
			break
		}
	}

	// if the request fails to the choosen one, try with others
	var providerError error
	r := ctx.GetResponse()
	if foundClient != nil {
		failed := make(chan bool)
		this.callEllected(foundClient, name, comm.Data, failed, r)
		// wait for the answer
		if <-failed {
			for _, foundClient = range others {
				this.callEllected(foundClient, name, comm.Data, failed, r)
				if <-failed {
					continue
				} else {
					return nil
				}
			}
			providerError = NoReliableProviderFault
		}
	} else {
		providerError = NoProviderFault
	}

	if providerError != nil {
		logger.Debugf("There is no answer so reply with error: %s", providerError.Error())
		e, err := this.codec.Encode(providerError)
		if err != nil {
			logger.Errorf("%s", err.Error())
		} else {
			logger.Debugf("????????????: %s", e)
			r.Body.Write(e)
		}
		r.Kind = ERROR
	}

	return nil
}

func (this *Server) callEllected(wire *Wire, name string, data []byte, failed chan bool, r *Response) {
	logger.Debugf("requesting %s", wire.Connection.RemoteAddr())
	go func(wire *Wire, name string, data []byte) {
		wire.Send(
			name,
			data,
			func(answer Payload) {
				logger.Debugf("%s answered successfully with %s", wire.Connection.RemoteAddr(), answer.Data)
				r.Body.Write(answer.Data)
				r.Kind = REPLY
				failed <- false
			},
			func(e Payload) {
				logger.Debugf("%s answered with failure: %s", wire.Connection.RemoteAddr(), e.Data)
				failed <- true
			},
			func(fault error) {
				logger.Debugf("%s answered with failure: %s", wire.Connection.RemoteAddr(), fault.Error())
				failed <- true
			})

	}(wire, name, data)
}

// Broadcast to all clients under this topic.
// This is executed by the client goroutine
func (this *Server) broadcast(ctx IContext) error {
	comm := ctx.GetRequest()
	name := comm.Name[len(PUB_CMD):]
	emiter := comm.Emiter.(*Wire)
	logger.Debugf("Broadcasting topic %s from %s", name, emiter.Connection.RemoteAddr())
	clients := this.getConsumers(name)
	for _, wire := range clients {
		// Does not send to the same client
		if wire.Connection != emiter.Connection {
			//logger.Debugf("Broadcasting topic %s to %s", name, wire.Connection.RemoteAddr())
			go func(wire *Wire, name string, data []byte) {
				wire.Send(name, data, nil, nil, func(fault error) {
					logger.Errorf("%s", fault.Error())
				})
			}(wire, name, comm.Data)
		}
	}
	return nil
}

func (this *Server) register(ctx IContext) error {
	comm := ctx.GetRequest()
	name := comm.Name[len(REG_CMD):]
	emiter := comm.Emiter.(*Wire)
	logger.Debugf("Subscribing %s to '%s'", emiter.Connection.RemoteAddr(), name)

	this.mu.Lock()
	defer this.mu.Unlock()

	pipe := this.clients[emiter.Connection]
	this.registration(name, pipe)

	return nil
}

func (this *Server) registration(topic string, pipe *Pipe) {
	client := pipe.Wire
	// topics under a client
	pipe.Topics[topic] = true

	// clients under a topics
	conns := this.registry[topic]
	if conns == nil {
		conns = make(map[net.Conn]*Wire)
		this.registry[topic] = conns
	}
	conns[client.Connection] = client
}

func (this *Server) registrations(ctx IContext) error {
	comm := ctx.GetRequest()

	topics := make([]string, 0)
	err := this.codec.Decode(comm.Data, &topics)
	if err != nil {
		return err
	}

	client := comm.Emiter.(*Wire)
	logger.Debugf("Client %s subscriptions: %s", client.Connection.RemoteAddr(), topics)

	this.mu.Lock()
	defer this.mu.Unlock()

	// clients under a topics
	pipe := this.clients[client.Connection]
	if pipe != nil {
		pipe.Topics = make(map[string]bool)
		for _, topic := range topics {
			this.registration(topic, pipe)
		}
	}

	return nil
}

func (this *Server) addClient(client *Wire) {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.clients[client.Connection] = &Pipe{client, make(map[string]bool)}
	logger.Debugf("client %s connected", client.Connection.RemoteAddr())
}

func (this *Server) removeClient(client *Wire) {
	this.mu.Lock()
	defer this.mu.Unlock()

	pipe := this.clients[client.Connection]
	if pipe != nil {
		var connMap map[net.Conn]*Wire
		//logger.Debugf("topics under %s: %v", client.Connection.RemoteAddr(), pipe.Topics)
		for topic, _ := range pipe.Topics {
			connMap = this.registry[topic]
			//logger.Debugf("removing client %s from topic '%s'", client.Connection.RemoteAddr(), topic)
			delete(connMap, client.Connection)
		}
	}
	delete(this.clients, client.Connection)
	logger.Debugf("client %s disconnected", client.Connection.RemoteAddr())
}
