package matchmakerpaxos

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(Phase1A{})
	gob.Register(Phase1B{})
	gob.Register(Phase2A{})
	gob.Register(Phase2B{})
	gob.Register(Phase3{})
	gob.Register(MatchA{})
	gob.Register(MatchB{})
}

// P1a prepare message
type Phase1A struct {
	Ballot paxi.Ballot
}

func (m Phase1A) String() string {
	return fmt.Sprintf("Phase1A {b=%v}", m.Ballot)
}

// CommandBallot conbines each command with its ballot number
type CommandBallot struct {
	Command paxi.Command
	Ballot  paxi.Ballot
}

func (cb CommandBallot) String() string {
	return fmt.Sprintf("cmd=%v b=%v", cb.Command, cb.Ballot)
}

// P1b promise message
type Phase1B struct {
	Ballot paxi.Ballot
	ID     paxi.ID               // from node id
	Log    map[int]CommandBallot // uncommitted logs
}

func (m Phase1B) String() string {
	return fmt.Sprintf("Phase1B {b=%v id=%s log=%v}", m.Ballot, m.ID, m.Log)
}

// P2a accept message
type Phase2A struct {
	Ballot  paxi.Ballot
	Slot    int
	Command paxi.Command
}

func (m Phase2A) String() string {
	return fmt.Sprintf("Phase2A {b=%v s=%d cmd=%v}", m.Ballot, m.Slot, m.Command)
}

// P2b accepted message
type Phase2B struct {
	Ballot paxi.Ballot
	ID     paxi.ID // from node id
	Slot   int
}

func (m Phase2B) String() string {
	return fmt.Sprintf("Phase2B {b=%v id=%s s=%d}", m.Ballot, m.ID, m.Slot)
}

// P3 commit message
type Phase3 struct {
	Ballot  paxi.Ballot
	Slot    int
	Command paxi.Command
}

func (m Phase3) String() string {
	return fmt.Sprintf("Phase3 {b=%v s=%d cmd=%v}", m.Ballot, m.Slot, m.Command)
}

type MatchA struct {
	ReplyTo       paxi.ID
	Ballot        paxi.Ballot
	Slot          int
	Configuration Configuration
}

func (m MatchA) String() string {
	return fmt.Sprintf("MatchA {b=%v, s=%d conf=%v}", m.Ballot, m.Slot, m.Configuration)
}

type MatchB struct {
	Ballot               paxi.Ballot
	Slot                 int
	ConfigurationHistory map[int]*Configuration
}

func (m MatchB) String() string {
	return fmt.Sprintf("MatchB {b=%v, s=%d conf=%v}", m.Ballot, m.Slot, m.ConfigurationHistory)
}
