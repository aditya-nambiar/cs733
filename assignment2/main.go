package main

// ASSUMPTION log starts from 0
import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"time"
)

const (
	ELECTION_TIMEOUT  = 500 // milliseconds
	HEARTBEAT_TIMEOUT = 50  // milliseconds
	NUM_SERVERS       = 5
)

type LogEntry struct {
	data      []byte
	committed bool
	term      int
}

type AppendMsg struct {
	Data []byte
}

type god struct {
	term int
}
type server struct {
	term        int
	leaderID    int
	serverID    int
	state       string
	voteGranted map[int]bool
	votedFor    int
	nextIndex   map[int]int // index of next log entry to send to that follower
	matchIndex  map[int]int
	commitIndex int
	log         []LogEntry
	clientCh    chan interface{} // client sends messages on this channel
	netCh       chan interface{} // channel and for network amongs servers
	actionCh    chan interface{} // channel to send my output action
	timer       <-chan time.Time
	mutex       sync.RWMutex
	allPeers    []int
	Stopped     chan bool
}

type Timeout struct {
}

type respEvent struct {
	from int
}

type Alarm struct {
	from int
	time int // time in milliseconds
}

type Send struct {
	from   int
	peerID int         // Id to send to
	event  interface{} // Can be AppendEntriesReq/Resp or VoteReq/Resp
}

type Commit struct {
	from  int
	index int
	data  LogEntry
	err   string
}

type LogStore struct {
	from  int
	index int
	data  []byte
}

type VoteReq struct {
	term         int
	candidateID  int
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
	prevLogIndex int // index of entry just preceding new ones
	prevLogTerm  int // term of entry just preceding new ones
	entries      []LogEntry
	leaderCommit int
}

type AppendEntriesResp struct {
	from       int
	term       int
	matchIndex int // how much of log matches the server
	success    bool
}

func min(x int, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func max(x int, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}

func (sm *server) getLogTerm(i int) int {
	if i >= 0 {
		return sm.log[i].term
	} else {
		return sm.log[len(sm.log)+i].term
	}
}

func (sm *server) Alarm(d time.Duration) {
	sm.timer = time.After(d * time.Millisecond)
}

func (sm *server) countVotes() int {
	count := 0
	for peer, vote := range sm.voteGranted {
		if vote {
			count += 1
			fmt.Println(peer)
		}

	}
	fmt.Println("Count" + strconv.Itoa(count))
	return count
}

func (sm *server) logit(index int, data []byte, term int) {
	if index < len(sm.log) {
		sm.log[index] = LogEntry{data: data, term: term, committed: false}
	} else {
		sm.log = append(sm.log, make([]LogEntry, index+1)...)
		sm.log[index] = LogEntry{data: data, term: term, committed: false}
	}
}

func genRand(min int, max int) int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(max-min) + min
}
func (sm *server) termCheck(mterm int) {
	if sm.term < mterm {
		sm.term = mterm
		sm.votedFor = -1
		sm.voteGranted = make(map[int]bool)
		sm.state = "Follower"
	}
}

func (sm *server) stopServer() {
	sm.Stopped <- true
}

//Erase extraneous entries post mismatch 41:00
func (sm *server) doAppendEntriesReq(msg AppendEntriesReq) {

	sm.termCheck(msg.term)

	if sm.term > msg.term {
		appendresp := AppendEntriesResp{from: sm.serverID, term: sm.term, matchIndex: -1, success: false}
		sm.actionCh <- appendresp
	} else {
		sm.actionCh <- Alarm{from: sm.serverID, time: genRand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2)}
		sm.leaderID = msg.leaderID
		sm.state = "Follower"
		var index int
		if msg.prevLogIndex == -1 || (msg.prevLogIndex <= len(sm.log)-1 && sm.getLogTerm(msg.prevLogIndex) == msg.prevLogTerm) {
			index = msg.prevLogIndex
			for j := 0; j < len(msg.entries); j += 1 {
				index += 1
				if sm.getLogTerm(index) != msg.entries[j].term {
					sm.logit(index, msg.entries[j].data, msg.entries[j].term)
					entryToStore := LogStore{from: sm.serverID, index: index, data: msg.entries[j].data}
					sm.actionCh <- entryToStore
				}
			}
			sm.commitIndex = min(msg.leaderCommit, index)
		} else {
			index = -1
		}
		appendresp := AppendEntriesResp{from: sm.serverID, term: sm.term, matchIndex: index, success: true}
		sm.actionCh <- appendresp
	}
}

func (sm *server) doVoteReq(msg VoteReq) {
	sm.termCheck(msg.term)

	if (sm.term == msg.term) && (sm.votedFor == -1 || sm.votedFor == msg.candidateID) {
		if msg.lastLogTerm > sm.getLogTerm(-1) || (msg.lastLogTerm == sm.getLogTerm(-1) && msg.lastLogIndex >= len(sm.log)-1) { // ?? Check id candidate is as uptodate
			sm.term = msg.term
			sm.votedFor = msg.candidateID
			sm.actionCh <- Alarm{from: sm.serverID, time: genRand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2)}
			voteresp := VoteResp{from: sm.serverID, term: sm.term, voteGranted: true}
			sm.actionCh <- voteresp
		}
	} else { // reject vote:
		voteresp := VoteResp{from: sm.serverID, term: sm.term, voteGranted: false}
		sm.actionCh <- voteresp
	}
}

func (sm *server) eventloop() {
	state := sm.state

	for sm.state != "Stopped" {
		switch state {
		case "Follower":
			sm.followerLoop()
		case "Candidate":
			sm.candidateLoop()
		case "Leader":
			sm.leaderLoop()

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
	//Alarm(time.now() + rand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2))
	//alarm :=
	sm.actionCh <- Alarm{from: sm.serverID, time: genRand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2)}

	for sm.state == "Follower" {
		select {
		case <-sm.Stopped:
			sm.state = "Stopped"
			return
		case appendMsg := <-sm.clientCh:
			t := reflect.TypeOf(appendMsg)
			fmt.Println(t)

		case msg1 := <-sm.netCh:
			t := reflect.TypeOf(msg1)
			switch t.Name() {
			case "AppendEntriesReq":
				msg := msg1.(AppendEntriesReq)
				sm.doAppendEntriesReq(msg)

			case "VoteReq":
				msg := msg1.(VoteReq)
				sm.doVoteReq(msg)

			case "Timeout":
				sm.state = "Candidate"

			default:
				msg := msg1.(god)
				sm.termCheck(msg.term)
				fmt.Println("God called updated " + strconv.Itoa(sm.term))
				//Do Nothing
			}

		}
	}
}

func (sm *server) candidateLoop() {

	restartElection := true

	for sm.state == "Candidate" {
		if restartElection {
			// Increment current term, vote for self.
			sm.term++
			sm.votedFor = sm.serverID
			sm.voteGranted = make(map[int]bool)
			sm.voteGranted[sm.serverID] = true
			// Send RequestVote RPCs to all other servers.
			for _, peer := range sm.allPeers {
				votereq := VoteReq{term: sm.term, candidateID: sm.serverID, lastLogIndex: len(sm.log) - 1, lastLogTerm: sm.getLogTerm(-1)}
				fmt.Println("Sending to " + strconv.Itoa(peer))
				sm.actionCh <- Send{from: sm.serverID, peerID: peer, event: votereq}
			}
			fmt.Println("resetting")
			sm.actionCh <- Alarm{from: sm.serverID, time: genRand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2)}
			restartElection = false
		}

		select {
		case <-sm.Stopped:
			sm.state = "Stopped"
			return

		case appendMsg := <-sm.clientCh:
			t := reflect.TypeOf(appendMsg)
			fmt.Println(t)

		case msg1 := <-sm.netCh:
			t := reflect.TypeOf(msg1)
			switch t.Name() {
			case "VoteResp":
				msg := msg1.(VoteResp)
				sm.termCheck(msg.term)
				if sm.term == msg.term {
					sm.voteGranted[msg.from] = msg.voteGranted
				}
				if sm.countVotes() > NUM_SERVERS/2 {
					sm.state = "Leader"
					sm.leaderID = sm.serverID
					for _, peer := range sm.allPeers { // send heartbeats to all servers and establish territory
						info := make([]byte, 1)
						logentries := make([]LogEntry, 1)
						logentry := LogEntry{data: info, term: sm.term}
						logentries[0] = logentry
						entryToStore := LogStore{from: sm.serverID, index: len(sm.log), data: info}
						sm.actionCh <- entryToStore
						sm.nextIndex[peer] = len(sm.log) // ?? check
						sm.matchIndex[peer] = -1         // Add this empty entry t ur own log ?
						appendreq := AppendEntriesReq{term: sm.term, leaderID: sm.serverID, prevLogIndex: len(sm.log) - 1, prevLogTerm: sm.getLogTerm(-1), entries: logentries, leaderCommit: sm.commitIndex}
						sm.actionCh <- Send{from: sm.serverID, peerID: peer, event: appendreq}
					}
					sm.actionCh <- Alarm{from: sm.serverID, time: genRand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2)}
				}

			case "VoteReq":
				msg := msg1.(VoteReq)
				sm.doVoteReq(msg)

			case "AppendEntriesReq":
				msg := msg1.(AppendEntriesReq)
				sm.doAppendEntriesReq(msg)

			case "Timeout":
				restartElection = true
				fmt.Println("haha")

			default:
				msg := msg1.(god)
				sm.termCheck(msg.term)
				fmt.Println("God called updated " + strconv.Itoa(sm.term))

			}
		}
	}
}

// The event loop that is run when the server is in a Leader state.
func (sm *server) leaderLoop() {

	for sm.state == "Leader" {
		select {
		case <-sm.Stopped:
			sm.state = "Stopped"
			return

		case appendMsg := <-sm.clientCh:
			t := reflect.TypeOf(appendMsg)
			fmt.Println(t)

		case msg1 := <-sm.netCh:
			t := reflect.TypeOf(msg1)
			switch t.Name() {
			case "AppendEntriesReq":
				msg := msg1.(AppendEntriesReq)
				sm.doAppendEntriesReq(msg)
			//Alarm(time.now() + rand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2))
			case "VoteReq":
				msg := msg1.(VoteReq)
				sm.doVoteReq(msg)

			case "AppendEntriesResp":
				msg := msg1.(AppendEntriesResp)
				sm.termCheck(msg.term)

				if sm.term == msg.term && sm.state == "Leader" {
					if msg.success {
						sm.matchIndex[msg.from] = msg.matchIndex
						sm.nextIndex[msg.from] = msg.matchIndex + 1

						if sm.matchIndex[msg.from] < len(sm.log)-1 { //??
							sm.nextIndex[msg.from] = len(sm.log) - 1 // Doubt
							temp_prevLogIndex := sm.nextIndex[msg.from] - 1
							temp_prevLogTerm := sm.getLogTerm(temp_prevLogIndex)
							appendreq := AppendEntriesReq{term: sm.term, leaderID: sm.serverID, prevLogIndex: temp_prevLogIndex, prevLogTerm: temp_prevLogTerm, entries: sm.log[sm.nextIndex[msg.from]:], leaderCommit: sm.commitIndex}
							sm.actionCh <- Send{from: sm.serverID, peerID: msg.from, event: appendreq}
						}

						cnt := 0
						for _, peer := range sm.allPeers {
							if sm.matchIndex[peer] > sm.commitIndex {
								cnt += 1
							}
						}
						if cnt > NUM_SERVERS/2 {
							sm.commitIndex++
							sm.actionCh <- Commit{from: sm.serverID, data: sm.log[sm.commitIndex], err: ""}
						}
					} else {
						sm.nextIndex[msg.from] = max(0, sm.nextIndex[msg.from]-1)
						appendreq := AppendEntriesReq{term: sm.term, leaderID: sm.serverID, prevLogIndex: sm.nextIndex[msg.from], prevLogTerm: sm.log[sm.nextIndex[msg.from]].term, entries: sm.log[sm.nextIndex[msg.from]:], leaderCommit: sm.commitIndex}
						sm.actionCh <- Send{from: sm.serverID, peerID: msg.from, event: appendreq}
					}
				}
			case "Timeout":
				for _, peer := range sm.allPeers {
					if peer != sm.serverID {
						info := make([]byte, 1)
						var logentries []LogEntry
						logentry := LogEntry{data: info, term: sm.term}
						logentries[0] = logentry
						entryToStore := LogStore{from: sm.serverID, index: len(sm.log), data: info}
						sm.actionCh <- entryToStore
						appendreq := AppendEntriesReq{term: sm.term, leaderID: sm.serverID, prevLogIndex: len(sm.log) - 1, prevLogTerm: sm.getLogTerm(-1), entries: logentries, leaderCommit: sm.commitIndex}
						sm.actionCh <- Send{from: sm.serverID, peerID: peer, event: appendreq}
					}
					sm.actionCh <- Alarm{from: sm.serverID, time: genRand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2)}
				}
				sm.actionCh <- Alarm{from: sm.serverID, time: genRand(HEARTBEAT_TIMEOUT, HEARTBEAT_TIMEOUT*2)}

			default:
				msg := msg1.(god)
				sm.termCheck(msg.term)
				fmt.Println("God called updated " + strconv.Itoa(sm.term))
			} //end of switch
		}
	}
}

func newServer(id int) *server {
	sm := server{
		serverID:    id,
		voteGranted: make(map[int]bool),
		nextIndex:   make(map[int]int),
		matchIndex:  make(map[int]int),
		clientCh:    make(chan interface{}),
		netCh:       make(chan interface{}),
		actionCh:    make(chan interface{}),
		Stopped:     make(chan bool),
		votedFor:    -1,
		state:       "Follower",
		term:        0,
		commitIndex: 0,
	}

	info := make([]byte, 1)
	logentries := make([]LogEntry, 1)
	logentry := LogEntry{data: info, term: 0}
	logentries[0] = logentry
	sm.log = logentries
	sm.allPeers = []int{2, 3, 4, 5, 6}
	return &sm
}
