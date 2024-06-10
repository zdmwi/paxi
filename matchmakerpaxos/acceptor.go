package matchmakerpaxos

import (
	"github.com/ailidani/paxi"
)

type Acceptor struct {
	paxi.Node
	*MatchmakerPaxos
}

func NewAcceptor(id paxi.ID, acceptors []paxi.ID, matchmakers []paxi.ID) *Acceptor {
	a := new(Acceptor)
	a.Node = paxi.NewNode(id)
	a.MatchmakerPaxos = NewMatchmakerPaxos(a, acceptors, matchmakers)
	a.Register(Phase1A{}, a.handlePhase1A)
	a.Register(Phase2A{}, a.handlePhase2A)
	return a
}

func (a *Acceptor) handlePhase1A(m Phase1A) {
	// log.Debugf("Acceptor %s received %v\n", a.ID(), m)
	a.MatchmakerPaxos.HandlePhase1A(m)
}

func (a *Acceptor) handlePhase2A(m Phase2A) {
	// log.Debugf("Acceptor %s received %v\n", a.ID(), m)
	a.MatchmakerPaxos.HandlePhase2A(m)
}
