gomsg
=====

Building blocks for message queues solutions in Go

This is a **work in progress**.

This is an exercise to see what I could do in Go regarding networking and also to learn a bit more about Go.

The idea is that one client endpoint connects to a server endpoint, and as soon the connection is established you can send and receive messages in either endpoint. Also a server can act as a router/broker for messages between clients.

This is loosely inspired in [zeromq](http://zeromq.org/)

Features
-
* Publish/Subscribe
* Push/Pull
* Request/Reply (single or multiple replies)
* RequestAll/Reply (multiple endpoints replying)
* Message Filters

The following example shows two clients connecting to a server, and the server publishing a message.

```go
server := gomsg.NewServer()
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

server.Publish("HELLO", "World")

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
