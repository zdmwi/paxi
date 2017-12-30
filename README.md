
## What is WPaxos?

**WPaxos** is a multileader Paxos protocol that provides low-latency and high-throughput consensus across wide-area network (WAN) deployments. Unlike statically partitioned multiple Paxos deployments, WPaxos perpetually adapts to the changing access locality through object stealing. Multiple concurrent leaders coinciding in different zones steal ownership of objects from each other using phase-1 of Paxos, and then use phase-2 to commit update-requests on these objects locally until they are stolen by other leaders. To achieve fast phase-2 commits, WPaxos adopts the flexible quorums idea in a novel manner, and appoints phase-2 acceptors to be close to their respective leaders.

WPaxos (WAN Paxos) paper (first version) can be found in https://arxiv.org/abs/1703.08905.


## What is Paxi?

**Paxi** is the framework that implements WPaxos and other Paxos protocol variants. Paxi provides most of the elements that any Paxos implementation or replication protocol needs, including network communication, state machine of a key-value store, client API and multiple types of quorum systems.

Warning: Paxi project is still under heavy development, with more features and protocols to include. Paxi API may change too.


## What is included?

- [x] [WPaxos](https://arxiv.org/abs/1703.08905)
- [x] [EPaxos](https://dl.acm.org/citation.cfm?id=2517350)
- [x] KPaxos (Static partitioned Paxos)
- [ ] [Vertical Paxos](https://www.microsoft.com/en-us/research/wp-content/uploads/2009/08/Vertical-Paxos-and-Primary-Backup-Replication-.pdf)
- [ ] [WanKeeper](http://ieeexplore.ieee.org/abstract/document/7980095/)
- [ ] Transactions
- [ ] Dynamic quorums
- [ ] Fault injection
- [ ] Linerizability checker

# How to build

1. Install [Go 1.9](https://golang.org/dl/).
2. [Download](https://github.com/wpaxos/paxi/archive/master.zip) WPaxos source code from GitHub page or use following command:
```
go get github.com/ailidani/paxi
```

3. Compile everything.
```
cd github.com/ailidani/paxi/bin
./build.sh
```

After compile, Golang will generate 4 executable files under `bin` folder.
* `master` is the easy way to distribute configurations to all replica nodes.
* `server` is one replica instance.
* `client` is a simple benchmark that generates read/write reqeust to servers.
* `cmd` is a command line tool to test Get/Set requests.


# How to run

Each executable file expects some parameters which can be seen by `-help` flag, e.g. `./master -help`.

1. Start master node with 6 replicas running WPaxos:
```
./master.sh -n 6 -protocol "wpaxos"
```

2. Start 6 servers with different zone id and node ids.
```
./server -id 1.1 -master 127.0.0.1 &
./server -id 1.2 -master 127.0.0.1 &
./server -id 2.1 -master 127.0.0.1 &
./server -id 2.2 -master 127.0.0.1 &
./server -id 3.1 -master 127.0.0.1 &
./server -id 3.2 -master 127.0.0.1 &
```

3. Start benchmarking client with 10 threads, 1000 keys, 50 percent conflicting commands and run for 60 seconds in 1 round.
```
./client -id 1.1 -master 127.0.0.1 -bconfig benchmark.json
```

# How to implement algorithms in Paxi

Replication algorithm in Paxi follows the message passing model, where several message types and their handle function are registered.

1. Define messages, register with gob in `init()` function if using gob codec.

2. Define handle function for each message type.

3. Register the messages with their handle function using `Node.Register(interface{}, func())` interface.

For sending messages, use `Send(to ID, msg interface{})`, `Broadcast(msg interface{})` functions in Node.Socket.

For data-store related functions check db.go file.

For quorum types check quorum.go file.

Client uses a simple RESTful API to submit requests. GET method with URL "http://ip:port/key" will read the value of given key. POST method with URL "http://ip:port/key" and body as the value, will write the value to key.
