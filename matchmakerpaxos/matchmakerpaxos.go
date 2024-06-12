package matchmakerpaxos

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// entry in log
type entry struct {
	ballot    paxi.Ballot
	command   paxi.Command
	commit    bool
	request   *paxi.Request
	quorum    *paxi.Quorum
	timestamp time.Time
}

type QuorumSystemType int

const (
	SimpleMajority QuorumSystemType = iota + 1
)

func (qs QuorumSystemType) String() string {
	return [...]string{"SimpleMajority"}[qs-1]
}

type QuorumSystem struct {
	QuorumType QuorumSystemType
	Members    []paxi.ID
}

func newQuorumSystem(quorumType QuorumSystemType, members []paxi.ID) *QuorumSystem {
	q := &QuorumSystem{
		QuorumType: quorumType,
		Members:    members,
	}
	return q
}

func (qs QuorumSystem) String() string {
	return fmt.Sprintf("QuorumSystem {t=%v, m=%v}", qs.QuorumType, qs.Members)
}

type Configuration struct {
	Acceptors    []paxi.ID
	Phase1Quorum QuorumSystem
	Phase2Quorum QuorumSystem
}

func newConfiguration(acceptors []paxi.ID, phase1Quorum QuorumSystem, phase2Quorum QuorumSystem) *Configuration {
	c := &Configuration{
		Acceptors:    acceptors,
		Phase1Quorum: phase1Quorum,
		Phase2Quorum: phase2Quorum,
	}
	return c
}

func (c Configuration) String() string {
	return fmt.Sprintf("Configuration {A=%v, P1=%v, P2=%v}", c.Acceptors, c.Phase1Quorum, c.Phase2Quorum)
}

// Paxos instance
type MatchmakerPaxos struct {
	paxi.Node

	log                    map[int]*entry         // log ordered by slot
	matchmakerLog          map[int]*Configuration // log for a matchmakerpaxos node
	matchmakerHistoryUnion map[paxi.ID]bool       // combination of the union of the config histories
	numMatchReplies        int                    // count for the number of matchA replies
	execute                int                    // next execute slot number
	ballot                 paxi.Ballot            // highest ballot number
	slot                   int                    // highest slot number
	highestVoteRound       int                    // highest round voted in

	// service discovery
	acceptors   []paxi.ID
	matchmakers []paxi.ID

	// matchmaker paxos specific
	configuration       *Configuration
	phase1QuorumReplies map[int]map[paxi.ID][]time.Time
	phase2QuorumReplies map[int]map[paxi.ID][]time.Time

	quorum   *paxi.Quorum    // phase 1 quorum
	requests []*paxi.Request // phase 1 pending requests

	// tracking performance
	latencies map[paxi.ID][]time.Duration
	rwLoads   map[paxi.ID][]int // read/write loads for each acceptor as array [read, write]
}

// NewMatchmakerPaxos creates new paxos instance
func NewMatchmakerPaxos(n paxi.Node, acceptors []paxi.ID, matchmakers []paxi.ID, options ...func(*MatchmakerPaxos)) *MatchmakerPaxos {
	phase1Quorum := newQuorumSystem(SimpleMajority, make([]paxi.ID, 0))
	phase2Quorum := newQuorumSystem(SimpleMajority, make([]paxi.ID, 0))

	p := &MatchmakerPaxos{
		Node:                   n,
		log:                    make(map[int]*entry, paxi.GetConfig().BufferSize),
		matchmakerLog:          make(map[int]*Configuration, 0),
		matchmakerHistoryUnion: make(map[paxi.ID]bool, 0),
		numMatchReplies:        0,
		slot:                   -1,
		highestVoteRound:       -1,
		acceptors:              acceptors,
		matchmakers:            matchmakers,
		configuration:          newConfiguration(paxi.GetConfig().Acceptors, *phase1Quorum, *phase2Quorum),
		phase1QuorumReplies:    make(map[int]map[paxi.ID][]time.Time, 0),
		phase2QuorumReplies:    make(map[int]map[paxi.ID][]time.Time, 0),
		quorum:                 paxi.NewQuorum(),
		requests:               make([]*paxi.Request, 0),
		latencies:              make(map[paxi.ID][]time.Duration, 0),
		rwLoads:                make(map[paxi.ID][]int, 0),
	}

	for _, opt := range options {
		opt(p)
	}

	return p
}

func (p *MatchmakerPaxos) selectRandomConfiguration() *Configuration {
	phase1QuorumSeen := make(map[paxi.ID]bool)
	phase2QuorumSeen := make(map[paxi.ID]bool)

	phase1Members := make([]paxi.ID, 0)
	phase2Members := make([]paxi.ID, 0)

	// base decision on latencies and loads
	log.Debugf("Latencies: %v\n", p.latencies)
	log.Debugf("RW Loads: %v\n", p.rwLoads)

	if len(p.acceptors) >= paxi.GetConfig().F*2+1 {
		for len(phase1Members) <= paxi.GetConfig().F+1 {
			randomMember := p.acceptors[rand.Intn(len(p.acceptors))]

			_, isInConfig := phase1QuorumSeen[randomMember]
			if !isInConfig {
				phase1Members = append(phase1Members, randomMember)
				phase1QuorumSeen[randomMember] = true
			}
		}

		for len(phase2Members) <= paxi.GetConfig().F+1 {
			randomMember := p.acceptors[rand.Intn(len(p.acceptors))]

			_, isInConfig := phase2QuorumSeen[randomMember]
			if !isInConfig {
				phase2Members = append(phase2Members, randomMember)
				phase2QuorumSeen[randomMember] = true
			}
		}
	} else {
		phase1Members = append(phase1Members, p.acceptors...)
		phase2Members = append(phase2Members, p.acceptors...)
	}
	phase1Quorum := newQuorumSystem(SimpleMajority, phase1Members)
	phase2Quorum := newQuorumSystem(SimpleMajority, phase2Members)

	c := newConfiguration(p.acceptors, *phase1Quorum, *phase2Quorum)
	return c
}

func (p *MatchmakerPaxos) HandleClientRequest(r paxi.Request) {
	// update next largest round owned by this proposer
	p.ballot.Next(p.ID())

	// save the request with the value sent by the client
	p.requests = append(p.requests, &r)

	// selects an arbitrary simple majority configuration from the known acceptors
	// this will be done dynamically later
	p.configuration = p.selectRandomConfiguration()

	for _, matchmakerID := range p.matchmakers {
		m := MatchA{
			ReplyTo:       p.ID(),
			Ballot:        p.ballot, // has the largest round owned by this proposer
			Slot:          p.slot,
			Configuration: *p.configuration,
		}
		p.Send(matchmakerID, m)
	}
}

func (p *MatchmakerPaxos) HandleMatchA(r MatchA) {
	// if there exists a configuration C_j in round j >= p.Slot
	// then ignore the MatchA message
	for j := range p.matchmakerLog {
		if j >= r.Ballot.N() {
			// maybe send a nack back to the proposer
			return
		}
	}

	H := make(map[int]*Configuration, 0)
	for j, C_j := range p.matchmakerLog {
		H[j] = C_j
	}

	p.matchmakerLog[r.Ballot.N()] = &r.Configuration
	m := MatchB{
		Ballot:               r.Ballot,
		Slot:                 r.Slot,
		ConfigurationHistory: H,
	}

	p.Send(r.ReplyTo, m)
}

func (p *MatchmakerPaxos) HandleMatchB(r MatchB) {
	// accumulate responses until we get to f+1 MatchB requests
	if p.numMatchReplies < paxi.GetConfig().F+1 {
		p.numMatchReplies++
		for i, C_i := range r.ConfigurationHistory {
			if i == r.Ballot.N() {
				for _, nID := range C_i.Phase1Quorum.Members {
					p.matchmakerHistoryUnion[nID] = true
					if _, exists := p.phase1QuorumReplies[p.ballot.N()]; !exists {
						p.phase1QuorumReplies[r.Ballot.N()] = make(map[paxi.ID][]time.Time)
						p.phase1QuorumReplies[r.Ballot.N()][nID] = make([]time.Time, 2)
					}

					if _, exists := p.rwLoads[nID]; !exists {
						p.rwLoads[nID] = make([]int, 2)
					}
					p.rwLoads[nID][0]++ // we increment the read load for this acceptor
				}

				for _, nID := range C_i.Phase2Quorum.Members {
					p.matchmakerHistoryUnion[nID] = true
				}
			}
		}
		return
	}

	// once we have f+1 matchB requests we take the union of all
	// the histories
	acceptors := make([]paxi.ID, 0)
	for nID := range p.matchmakerHistoryUnion {
		acceptors = append(acceptors, nID)
	}

	// if we don't have any history yet, just select a random configuration
	// as the acceptors for this round and send the phase1A messages
	if len(acceptors) == 0 {
		// based on FPaxos paper, it should be safe to only include the phase1Quorum acceptors
		// but we follow along with the MatchmakerPaxos paper's algorithms
		randomConfig := p.selectRandomConfiguration()

		acceptors = append(acceptors, randomConfig.Phase1Quorum.Members...)
		if _, exists := p.phase1QuorumReplies[r.Ballot.N()]; !exists {
			p.phase1QuorumReplies[r.Ballot.N()] = make(map[paxi.ID][]time.Time)
		}

		acceptors = append(acceptors, randomConfig.Phase2Quorum.Members...)
	}

	for _, nID := range acceptors {
		m := Phase1A{Ballot: p.ballot}
		p.Send(nID, m)
		if _, exists := p.phase1QuorumReplies[r.Ballot.N()][nID]; !exists {
			p.phase1QuorumReplies[r.Ballot.N()][nID] = make([]time.Time, 2)
		}
		p.phase1QuorumReplies[r.Ballot.N()][nID][0] = time.Now()

		if _, exists := p.rwLoads[nID]; !exists {
			p.rwLoads[nID] = make([]int, 2)
		}
		p.rwLoads[nID][0]++ // we increment the read load for this acceptor
	}

	// reset numMatchReplies and matchmaker history
	p.numMatchReplies = 0
	p.matchmakerHistoryUnion = make(map[paxi.ID]bool)
}

func (p *MatchmakerPaxos) HandlePhase1A(r Phase1A) {
	// if current largest round seen is bigger than
	// proposers never get sent these messages so maybe unnecessary
	if p.ballot < r.Ballot {
		p.ballot = r.Ballot
		p.forward()
	}

	l := make(map[int]CommandBallot)
	for s := p.execute; s <= p.slot; s++ {
		if p.log[s] == nil || p.log[s].commit {
			continue
		}
		l[s] = CommandBallot{p.log[s].command, p.log[s].ballot}
	}

	p.Send(r.Ballot.ID(), Phase1B{
		Ballot: p.ballot,
		ID:     p.ID(),
		Log:    l,
	})
}

func (p *MatchmakerPaxos) update(scb map[int]CommandBallot) {
	for s, cb := range scb {
		p.slot = paxi.Max(p.slot, s)
		if e, exists := p.log[s]; exists {
			if !e.commit && cb.Ballot > e.ballot {
				e.ballot = cb.Ballot
				e.command = cb.Command
			}
		} else {
			p.log[s] = &entry{
				ballot:  cb.Ballot,
				command: cb.Command,
				commit:  false,
			}
		}
	}
}

func (p *MatchmakerPaxos) updatePhase1QuorumReceipts(roundNum int, acceptor paxi.ID) {
	if _, exists := p.phase1QuorumReplies[roundNum][acceptor]; exists {
		p.phase1QuorumReplies[roundNum][acceptor][1] = time.Now()
	}
}

func (p *MatchmakerPaxos) receivedPhaseBFromAllPhase1Quorums(roundNum int) bool {
	roundQuorum := p.phase1QuorumReplies[roundNum]
	for _, timeStartAndEnd := range roundQuorum {
		if timeStartAndEnd[1].IsZero() {
			return false
		}
	}
	return true
}

func (p *MatchmakerPaxos) updatePhase2QuorumReceipts(roundNum int, acceptor paxi.ID) {
	if _, exists := p.phase2QuorumReplies[roundNum][acceptor]; exists {
		p.phase2QuorumReplies[roundNum][acceptor][1] = time.Now()
	}
	// log.Debugf("Updated phase2 quorum replies for round %d: %v\n", roundNum, p.phase2QuorumReplies[roundNum])
}

func (p *MatchmakerPaxos) receivedPhaseBFromAllPhase2Quorums(roundNum int) bool {
	roundQuorum := p.phase2QuorumReplies[roundNum]
	// log.Debugf("Checking phase2 quorum replies for round %d: %v\n", roundNum, roundQuorum)
	for _, timeStartAndEnd := range roundQuorum {
		if timeStartAndEnd[1].IsZero() {
			// log.Debugf("Not all phase2 quorum replies received for round %d\n", roundNum)
			return false
		}
	}
	// log.Debugf("All phase2 quorum replies received for round %d\n", roundNum)
	return true
}

// HandleP1b handles P1b message
func (p *MatchmakerPaxos) HandlePhase1B(r Phase1B) {
	p.update(r.Log)

	// ignore stale messages
	if p.ballot > r.Ballot {
		return
	}

	// mark as received from the sending acceptor
	p.updatePhase1QuorumReceipts(r.Ballot.N(), r.ID)

	// update the latencies
	startTime := p.phase1QuorumReplies[r.Ballot.N()][r.ID][0]
	endTime := p.phase1QuorumReplies[r.Ballot.N()][r.ID][1]
	latency := endTime.Sub(startTime)

	if _, exists := p.latencies[r.ID]; !exists {
		p.latencies[r.ID] = make([]time.Duration, 0)
	}
	p.latencies[r.ID] = append(p.latencies[r.ID], latency)

	p.highestVoteRound = paxi.Max(p.highestVoteRound, r.Ballot.N()) // mark the highest vr in Phase1B

	// wait to receive phase1B messages from a phase1Quorum from every configuration in H
	if !p.receivedPhaseBFromAllPhase1Quorums(r.Ballot.N()) {
		// log.Debugf("Replies for round %d: %v\n", r.Ballot.N(), p.phase1QuorumReplies[r.Ballot.N()])
		return
	}

	// log.Debugf("Received phase1B messages from all phase1Quorums for round %d\n", r.Ballot.N())

	// get ready to track which acceptors have responded to phase2A messages
	if _, exists := p.phase2QuorumReplies[p.ballot.N()]; !exists {
		p.phase2QuorumReplies[p.ballot.N()] = make(map[paxi.ID][]time.Time)
	}

	// send phase2A messages to every acceptor in our configuration

	// propose any uncommitted entries
	for i := p.execute; i <= p.slot; i++ {
		if p.log[i] == nil || p.log[i].commit {
			continue
		}
		p.log[i].ballot = p.ballot

		for _, nID := range p.configuration.Phase1Quorum.Members {
			p.Send(nID, Phase2A{
				Ballot:  p.ballot,
				Slot:    i,
				Command: p.log[i].command,
			})
		}

		for _, nID := range p.configuration.Phase2Quorum.Members {
			p.Send(nID, Phase2A{
				Ballot:  p.ballot,
				Slot:    i,
				Command: p.log[i].command,
			})
			if _, exists := p.phase2QuorumReplies[p.ballot.N()][nID]; !exists {
				p.phase2QuorumReplies[p.ballot.N()][nID] = make([]time.Time, 2)
			}
			p.phase2QuorumReplies[p.ballot.N()][nID][0] = time.Now()

			if _, exists := p.rwLoads[nID]; !exists {
				p.rwLoads[nID] = make([]int, 2)
			}
			p.rwLoads[nID][1]++ // we increment the write load for this acceptor
		}
	}

	// propose new commands to every acceptor in our configuration
	for _, req := range p.requests {
		p.slot++
		p.log[p.slot] = &entry{
			ballot:    p.ballot,
			command:   req.Command,
			request:   req,
			quorum:    paxi.NewQuorum(), // just a place holder for now
			timestamp: time.Now(),
		}

		for _, nID := range p.configuration.Phase1Quorum.Members {
			p.Send(nID, Phase2A{
				Ballot:  p.ballot,
				Slot:    p.slot,
				Command: req.Command,
			})
		}

		for _, nID := range p.configuration.Phase2Quorum.Members {
			p.Send(nID, Phase2A{
				Ballot:  p.ballot,
				Slot:    p.slot,
				Command: req.Command,
			})

			if _, exists := p.phase2QuorumReplies[p.ballot.N()][nID]; !exists {
				p.phase2QuorumReplies[p.ballot.N()][nID] = make([]time.Time, 2)
			}
			p.phase2QuorumReplies[p.ballot.N()][nID][0] = time.Now()

			if _, exists := p.rwLoads[nID]; !exists {
				p.rwLoads[nID] = make([]int, 2)
			}
			p.rwLoads[nID][1]++ // we increment the write load for this acceptor
		}
	}

	// reset requests
	p.requests = make([]*paxi.Request, 0)
}

// HandleP2a handles P2a message
func (p *MatchmakerPaxos) HandlePhase2A(m Phase2A) {
	if m.Ballot >= p.ballot {
		p.ballot = m.Ballot
		// update slot number
		p.slot = paxi.Max(p.slot, m.Slot)
		// update entry
		if e, exists := p.log[m.Slot]; exists {
			if !e.commit && m.Ballot > e.ballot {
				// different command and request is not nil
				if !e.command.Equal(m.Command) && e.request != nil {
					p.Forward(m.Ballot.ID(), *e.request)
					// p.Retry(*e.request)
					e.request = nil
				}
				e.command = m.Command
				e.ballot = m.Ballot
			}
		} else {
			p.log[m.Slot] = &entry{
				ballot:  m.Ballot,
				command: m.Command,
				commit:  false,
			}
		}
	}

	p.Send(m.Ballot.ID(), Phase2B{
		Ballot: p.ballot,
		Slot:   m.Slot,
		ID:     p.ID(),
	})
}

// HandleP2b handles P2b message
func (p *MatchmakerPaxos) HandlePhase2B(m Phase2B) {
	// old message
	entry, exist := p.log[m.Slot]
	if !exist || m.Ballot < entry.ballot || entry.commit {
		// log.Debugf("Recevied old message %v\n", m)
		return
	}

	// node update its ballot number and falls back to acceptor
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
	}

	// wait to receive phase2B messages from phase 2 quorum, mark the entry as committed
	p.updatePhase2QuorumReceipts(m.Ballot.N(), m.ID)

	if _, exists := p.phase2QuorumReplies[m.Ballot.N()][m.ID]; exists {
		startTime := p.phase2QuorumReplies[m.Ballot.N()][m.ID][0]
		endTime := p.phase2QuorumReplies[m.Ballot.N()][m.ID][1]
		latency := endTime.Sub(startTime)

		if _, exists := p.latencies[m.ID]; !exists {
			p.latencies[m.ID] = make([]time.Duration, 0)
		}
		p.latencies[m.ID] = append(p.latencies[m.ID], latency)
	}

	if !p.receivedPhaseBFromAllPhase2Quorums(m.Ballot.N()) {
		log.Debugf("Replies for round %d: %v\n", m.Ballot.N(), p.phase2QuorumReplies[m.Ballot.N()])
		return
	}

	p.log[m.Slot].commit = true

	// execute the log and send a reply
	if m.Ballot == p.log[m.Slot].ballot {
		log.Debugf("Executing log... %v\n", p.log)
		p.exec()
	}
}

func (p *MatchmakerPaxos) exec() {
	for {
		e, ok := p.log[p.execute]
		if !ok || !e.commit {
			break
		}
		// log.Debugf("Replica %s execute [s=%d, cmd=%v]", p.ID(), p.execute, e.command)
		value := p.Execute(e.command)
		if e.request != nil {
			log.Debugf("Sending reply to client\n")
			reply := paxi.Reply{
				Command:    e.command,
				Value:      value,
				Properties: make(map[string]string),
			}
			reply.Properties[HTTPHeaderSlot] = strconv.Itoa(p.execute)
			reply.Properties[HTTPHeaderBallot] = e.ballot.String()
			reply.Properties[HTTPHeaderExecute] = strconv.Itoa(p.execute)
			e.request.Reply(reply)
			e.request = nil
		}
		// TODO clean up the log periodically
		delete(p.log, p.execute)
		p.execute++
	}
}

func (p *MatchmakerPaxos) forward() {
	for _, m := range p.requests {
		p.Forward(p.ballot.ID(), *m)
	}
	p.requests = make([]*paxi.Request, 0)
}
