package matchmakerpaxos

import (
	"flag"
	"strconv"
	"time"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

var read = flag.String("read", "", "read from \"leader\", \"quorum\" or \"any\" replica")

const (
	HTTPHeaderSlot       = "Slot"
	HTTPHeaderBallot     = "Ballot"
	HTTPHeaderExecute    = "Execute"
	HTTPHeaderInProgress = "Inprogress"
)

// Replica for one Paxos instance
type Replica struct {
	paxi.Node
	*MatchmakerPaxos
}

// NewReplica generates new Paxos replica
func NewReplica(id paxi.ID, acceptors []paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.MatchmakerPaxos = NewMatchmakerPaxos(r, acceptors)
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(P1a{}, r.HandleP1a)
	r.Register(P1b{}, r.HandleP1b)
	r.Register(P2a{}, r.HandleP2a)
	r.Register(P2b{}, r.HandleP2b)
	r.Register(P3{}, r.HandleP3)
	return r
}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)

	if m.Command.IsRead() && *read != "" {
		v, inProgress := r.readInProgress(m)
		reply := paxi.Reply{
			Command:    m.Command,
			Value:      v,
			Properties: make(map[string]string),
			Timestamp:  time.Now().Unix(),
		}
		reply.Properties[HTTPHeaderSlot] = strconv.Itoa(r.MatchmakerPaxos.slot)
		reply.Properties[HTTPHeaderBallot] = r.MatchmakerPaxos.ballot.String()
		reply.Properties[HTTPHeaderExecute] = strconv.Itoa(r.MatchmakerPaxos.execute - 1)
		reply.Properties[HTTPHeaderInProgress] = strconv.FormatBool(inProgress)
		m.Reply(reply)
		return
	}

	if r.MatchmakerPaxos.IsLeader() || r.MatchmakerPaxos.GetBallot() == 0 {
		r.MatchmakerPaxos.HandleRequest(m)
	} else {
		go r.Forward(r.MatchmakerPaxos.GetLeaderID(), m)
	}
}

func (r *Replica) readInProgress(m paxi.Request) (paxi.Value, bool) {
	// is in progress
	for i := r.MatchmakerPaxos.slot; i >= r.MatchmakerPaxos.execute; i-- {
		entry, exist := r.MatchmakerPaxos.log[i]
		if exist && entry.command.Key == m.Command.Key {
			return entry.command.Value, true
		}
	}

	// not in progress key
	return r.Node.Execute(m.Command), false
}
