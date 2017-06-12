gomsg
=====

Building blocks for message networks solutions in Go

This is a **WORK IN PROGRESS**.

This is an exercise to see what I could do in Go 
regarding networking and also to learn a bit more about Go.

The idea is that one client endpoint connects to a server endpoint,
and as soon the connection is established you can send and receive messages in either endpoint.
Also a server can act as a router/broker for messages between clients.

This is loosely inspired by [zeromq](http://zeromq.org/)

Features
-
* Publish/Subscribe
* Push/Pull
* Request/Reply (only one provider replies)
* RequestAll/ReplyAll (all providers reply)
* Message handler functions receiving any kind of data
* Message Filters
* Client Groups
* Temporary sticky endpoint by topic

A simple example

```go
// my custom data
type Sample struct {
    instant     time.Time
    voltage     float64
}
```
```go
// create server
var server = gomsg.NewServer()
// we can handler messages in the server or in the client
server.Handle("TELEMETRY", func(data Sample) {
    fmt.Printf("received %+v\n", data)
})
server.Listen(":7777")

// create a client - the one that connects
var cli = gomsg.NewClient()
cli.Connect("localhost:7777")
// Publish data to the other end
cli.Publish("TELEMETRY", Sample{time.Now(), 234.56})

// give time for things to happen
time.Sleep(time.Millisecond * 10)
```

The following example show a simple message bus.
Three clients connect to a server, and one of them publishs a message that will be received by the other two.

```go
server := gomsg.NewServer()
 // route all messages between the clients
server.Route("*", time.Second, nil)
server.Listen(":7777")

cli1 := gomsg.NewClient()
cli1.Handle("HELLO", func(m string) {
	fmt.Println("[1] Hello", m)
})
cli1.Connect("localhost:7777")

cli2 := gomsg.NewClient()
cli2.Handle("HELLO", func(m string) {
	fmt.Println("[2] Hello", m)
})
cli2.Connect("localhost:7777")

cli := gomsg.NewClient()
cli.Connect("localhost:7777")

cli.Publish("HELLO", "World")

// give time for things to happen
time.Sleep(time.Millisecond * 100)

```

Another nice feature is possibility of grouping clients.

If a client identifies the group that it belongs the server will only send the message
to one of the clients. This way we can have the server publish messages and have the clients
decide if they want to behave like Publish/Subscribe (ungrouped) or like Push/Push (grouped).

```go
server := gomsg.NewServer()
server.Listen(":7777")
server.Route("*", time.Second, nil)

// Group HA subscriber
cli := gomsg.NewClient()
cli.Handle("HELLO", func(m string) {
	fmt.Println("<=== [1] processing:", m)
})
cli.SetGroupId("HA")
cli.Connect("localhost:7777")

// Group HA subscriber
cli2 := gomsg.NewClient()
cli2.Handle("HELLO", func(m string) {
	fmt.Println("<=== [2] processing:", m)
})
cli2.SetGroupId("HA")
cli2.Connect("localhost:7777")

// publisher
cli3 := gomsg.NewClient()
cli3.Connect("localhost:7777")

// Only one element of the group HA will process
// each message, alternately (round robin).
cli3.Publish("HELLO", "Hello World!")
cli3.Publish("HELLO", "Olá Mundo!")
cli3.Publish("HELLO", "YESSSS!")
cli3.Publish("HELLO", "one")
cli3.Publish("HELLO", "two")
cli3.Publish("HELLO", "three")
cli3.Publish("HELLO", "test")

time.Sleep(time.Millisecond * 100)
```


Dependencies
-
Go 1.1+

Todo
-
* More Tests
* Documentation
