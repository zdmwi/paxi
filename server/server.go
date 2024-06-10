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

func proposer(id paxi.ID, acceptors []paxi.ID, matchmakers []paxi.ID) {
	if *master != "" {
		paxi.ConnectToMaster(*master, false, id)
	}

	log.Infof("proposer node %v starting...", id)

	switch *algorithm {

	case "matchmakerpaxos":
		matchmakerpaxos.NewProposer(id, acceptors, matchmakers).Run()

	default:
		panic("Unknown algorithm")
	}
}

func acceptor(id paxi.ID, acceptors []paxi.ID, matchmakers []paxi.ID) {
	if *master != "" {
		paxi.ConnectToMaster(*master, false, id)
	}

	log.Infof("acceptor node %v starting...", id)

	switch *algorithm {

	case "matchmakerpaxos":
		matchmakerpaxos.NewAcceptor(id, acceptors, matchmakers).Run()

	default:
		panic("Unknown algorithm")
	}
}

func matchmaker(id paxi.ID, acceptors []paxi.ID, matchmakers []paxi.ID) {
	if *master != "" {
		paxi.ConnectToMaster(*master, false, id)
	}

	log.Infof("matchmaker node %v starting...", id)

	switch *algorithm {

	case "matchmakerpaxos":
		matchmakerpaxos.NewMatchmaker(id, acceptors, matchmakers).Run()

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

		// start the proposers
		for _, nodeID := range paxi.GetConfig().Proposers {
			n := nodeID
			go proposer(n, paxi.GetConfig().Acceptors, paxi.GetConfig().Matchmakers)
		}

		// start the acceptors
		for _, nodeID := range paxi.GetConfig().Acceptors {
			n := nodeID
			go acceptor(n, paxi.GetConfig().Acceptors, paxi.GetConfig().Matchmakers)
		}

		// start the matchmakers
		for _, nodeID := range paxi.GetConfig().Matchmakers {
			n := nodeID
			go matchmaker(n, paxi.GetConfig().Acceptors, paxi.GetConfig().Matchmakers)
		}
		wg.Wait()
	} else {
		// naively search for the node id in the grouping of proposers, acceptors and matchmakers
		// not ideal, but works for now

		nID := paxi.ID(*id)
		for _, pID := range paxi.GetConfig().Proposers {
			if pID == nID {
				proposer(nID, paxi.GetConfig().Acceptors, paxi.GetConfig().Matchmakers)
			}
		}

		for _, aID := range paxi.GetConfig().Acceptors {
			if aID == nID {
				acceptor(nID, paxi.GetConfig().Acceptors, paxi.GetConfig().Matchmakers)
			}
		}

		for _, mID := range paxi.GetConfig().Matchmakers {
			if mID == nID {
				matchmaker(nID, paxi.GetConfig().Acceptors, paxi.GetConfig().Matchmakers)
			}
		}
	}
}
