gomsg
=====

Building blocks for message queues solutions in Go

This is a **work in progress**.

This is an exercise to see what I could do in Go regarding networking and also to learn a bit more about Go.

The idea is that one client endpoint connects to a server endpoint, and as soon the connection is established you can send and receive messages in either endpoint. Also a server can act as a router/broker for messages between clients.

This is loosely inspired by [zeromq](http://zeromq.org/)

Features
-
* Publish/Subscribe
* Push/Pull
* Request/Reply (single or multiple replies)
* RequestAll/Reply (multiple endpoints replying)
* Message Filters

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

Dependencies
-
Go 1.1+

Todo
-
* Tests
* Documentation
