package matchmakerpaxos

import (
	"github.com/ailidani/paxi"
)

type Proposer struct {
	paxi.Node
	*MatchmakerPaxos
}

func NewProposer(id paxi.ID, acceptors []paxi.ID, matchmakers []paxi.ID) *Proposer {
	p := new(Proposer)
	p.Node = paxi.NewNode(id)
	p.MatchmakerPaxos = NewMatchmakerPaxos(p, acceptors, matchmakers)
	p.Register(paxi.Request{}, p.handleRequest)
	p.Register(MatchB{}, p.handleMatchB)
	p.Register(Phase1B{}, p.handlePhase1B)
	p.Register(Phase2B{}, p.handlePhase2B)

	return p
}

func (r *Proposer) handleRequest(m paxi.Request) {
	// log.Debugf("Proposer %s received %v\n", r.ID(), m)
	r.MatchmakerPaxos.HandleClientRequest(m)
}

func (r *Proposer) handleMatchB(m MatchB) {
	// log.Debugf("Proposer %s received %v\n", r.ID(), m)
	r.MatchmakerPaxos.HandleMatchB(m)
}

func (r *Proposer) handlePhase1B(m Phase1B) {
	// log.Debugf("Proposer %s received %v\n", r.ID(), m)
	r.MatchmakerPaxos.HandlePhase1B(m)
}

func (r *Proposer) handlePhase2B(m Phase2B) {
	// log.Debugf("Proposer %s received %v\n", r.ID(), m)
	r.MatchmakerPaxos.HandlePhase2B(m)
}
