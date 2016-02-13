package main

import (
	"fmt"
	"reflect"
	"sync"
	"time"
)

type LogEntry struct {
	data      []byte
	committed bool
	term      int
	logIndex  int
}

type AppendMsg struct {
	Data []byte
}

type server struct {
	term        int
	leaderID    int
	serverID    int
	state       string
	voteGranted map[int]bool
	votedFor    int
	nextIndex   map[int]int
	matchIndex  map[int]int
	commitIndex int
	log         []LogEntry
	clientCh    chan interface{} // client sends messages on this channel
	netCh       chan interface{} // channel and for network amongs servers
	actionCh    chan interface{} // my own actions
	timer       *time.Timer
	mutex       sync.RWMutex
	peerChan    chan interface{} // server sends it actions on this channel
}

type VoteReq struct {
	term         int
	candidateId  int
	lastLogIndex int
	from         int
	lastLogTerm  int
}

type VoteResp struct {
	from        int
	term        int
	voteGranted bool
}

type AppendEntriesReq struct {
	term         int //leader's current term
	leaderID     int //for follower's to update themselves
	prevLogIndex int
	prevLogTerm  int
	entry        []LogEntry
	leaderCommit int
}

type AppendEntriesResp struct {
	from       int
	term       int
	nextIndex  int
	matchIndex int // how much of log matches the server
	success    bool
}

func (sm *server) getLogTerm(i int) int {
	if i >= 0 {
		return sm.log[i].term
	} else {
		return sm.log[len(sm.log)+i].term
	}
}

func (sm *server) eventloop() {
	state := sm.state

	for {
		switch state {
		case "Follower":
			sm.followerLoop()
		case "Candidate":
			//sm.candidateLoop()
		case "Leader":
			//sm.leaderLoop()
		}
		state = sm.state
	}
}

// The event loop that is run when the server is in a Follower state.
// Responds to RPCs from candidates and leaders.
// Converts to candidate if election timeout elapses without either:
//   1.Receiving valid AppendEntries RPC, or
//   2.Granting vote to candidate
func (sm *server) followerLoop() {
	//Alarm(time.now() + rand(s.ElectionTimeout(), s.ElectionTimeout()*2))

	for sm.state == "Follower" {
		select {
		case appendMsg := <-sm.clientCh:
			t := reflect.TypeOf(appendMsg)
			fmt.Println(t)

		case msg1 := <-sm.netCh:
			t := reflect.TypeOf(msg1)
			switch t.Name() {
			case "AppendEntriesReq":
				msg := msg1.(AppendEntriesReq)
				if msg.term > sm.term {
					sm.term = msg.term
					sm.leaderID = msg.leaderID
					sm.votedFor = -1
				}

				if sm.term > msg.term { // Server is more updates
					//action = Send(msg.leaderId, &AppendEntriesResp{term : sm.currentTerm, from : sm.serverID, success :false})
				} else if sm.log[msg.prevLogIndex].term != msg.prevLogTerm { // log doesn't match
					//sm.matchIndex = 0
					//action = Send(msg.leaderId, &AppendEntriesResp{term : sm.currentTerm, from : sm.serverID, success :false})
				} else if len(sm.log) > msg.prevLogIndex || msg.prevLogIndex == 0 { // CHECK CONDITION
					//LogStore(sm.lastLogIndex+1, msg.entries)
					//action = Send(msg.leaderID, AppendEntriesResp(sm.currentTerm, sm.lastLogIndex, success :yes))
					if sm.commitIndex < msg.leaderCommit {
						//commitIndex := math.Min(leaderCommit, sm.lastLogIndex)
						//Commit(index, data, err)
					}
				}
			//Alarm(time.now() + rand(s.ElectionTimeout(), s.ElectionTimeout()*2))

			case "VoteReq":
				msg := msg1.(VoteReq)
				if sm.term <= msg.term {
					sm.term = msg.term
					sm.votedFor = -1
				}

				if sm.term == msg.term && (sm.votedFor == -1 || sm.votedFor == msg.candidateId) {
					if msg.lastLogTerm > sm.getLogTerm(-1) || (msg.lastLogTerm == sm.getLogTerm(-1) && msg.lastLogIndex >= len(sm.log)-1) {
						sm.term = msg.term
						sm.votedFor = msg.candidateId
						//action = Send(msg.from, &VoteResp{term :sm.term, from : sm.serverID, voteGranted:yes})
					}
				} else { //reject vote:
					//action = Send(msg.from, &VoteResp(term: sm.term, from : sm.serverID,  voteGranted:no})
				}
			}
		case <-time.After(5 * time.Minute):
			sm.state = "Candidate"
			//Alarm(time.now() + rand(s.ElectionTimeout(), s.ElectionTimeout()*2))
			//end select
		}
	}
}
