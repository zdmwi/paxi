package main

import (
	"flag"
	"sync"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/ailidani/paxi/matchmakerpaxos"
)

var algorithm = flag.String("algorithm", "paxos", "Distributed algorithm")
var id = flag.String("id", "", "ID in format of Zone.Node.")
var simulation = flag.Bool("sim", false, "simulation mode")

var master = flag.String("master", "", "Master address.")

func replica(id paxi.ID, acceptors []paxi.ID) {
	if *master != "" {
		paxi.ConnectToMaster(*master, false, id)
	}

	log.Infof("node %v starting...", id)

	switch *algorithm {

	case "matchmakerpaxos":
		matchmakerpaxos.NewReplica(id, acceptors).Run()

	default:
		panic("Unknown algorithm")
	}
}

func main() {
	paxi.Init()

	if *simulation {
		var wg sync.WaitGroup
		wg.Add(1)
		paxi.Simulation()
		for id := range paxi.GetConfig().Addrs {
			n := id
			// use all nodes in the configuration as acceptors for now
			go replica(n, paxi.GetConfig().IDs())
		}
		wg.Wait()
	} else {
		// use all nodes in the configuration as acceptors for now
		replica(paxi.ID(*id), paxi.GetConfig().IDs())
	}
}
