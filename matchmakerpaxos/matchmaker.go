package matchmakerpaxos

import (
	"github.com/ailidani/paxi"
)

// Replica for one Paxos instance
type Matchmaker struct {
	paxi.Node
	*MatchmakerPaxos
}

// NewReplica generates new Paxos replica
func NewMatchmaker(id paxi.ID, acceptors []paxi.ID, matchmakers []paxi.ID) *Matchmaker {
	m := new(Matchmaker)
	m.Node = paxi.NewNode(id)
	m.MatchmakerPaxos = NewMatchmakerPaxos(m, acceptors, matchmakers)
	m.Register(MatchA{}, m.handleMatchA)
	return m
}

func (m *Matchmaker) handleMatchA(ma MatchA) {
	// log.Debugf("Matchmaker %s received %v\n", m.ID(), ma)
	m.MatchmakerPaxos.HandleMatchA(ma)
}
