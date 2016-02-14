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
	logIndex  int
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
	nextIndex   map[int]int
	matchIndex  map[int]int
	commitIndex int
	log         []LogEntry
	clientCh    chan interface{} // client sends messages on this channel
	netCh       chan interface{} // channel and for network amongs servers
	actionCh    chan interface{} // my own actions
	timer       <-chan time.Time
	mutex       sync.RWMutex
	allPeers    map[int](chan interface{})
	Stopped     chan bool
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
	prevLogIndex int
	prevLogTerm  int
	entries      []LogEntry
	leaderCommit int
}

type AppendEntriesResp struct {
	from       int
	term       int
	nextIndex  int
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

func genRand(min int, max int) time.Duration {
	rand.Seed(time.Now().Unix())
	return time.Duration(rand.Intn(max-min) + min)
}
func (sm *server) termCheck(mterm int) {
	if sm.term < mterm {
		sm.term = mterm
		sm.votedFor = -1
		sm.voteGranted = make(map[int]bool)
		sm.Alarm(genRand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2))
		sm.state = "Follower"
	}
}

func (sm *server) stopServer() {
	sm.Stopped <- true
}

func (sm *server) doAppendEntriesReq(msg AppendEntriesReq) {

	sm.termCheck(msg.term)

	if (sm.term) > msg.term {
		// action = Send(msg.leaderId, AppendEntriesResp(sm.currentTerm, success=no, nil))
	} else {
		sm.Alarm(genRand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2))
		sm.leaderID = msg.leaderID
		sm.state = "Follower"
		var index int
		if msg.prevLogIndex == -1 || (msg.prevLogIndex <= len(sm.log)-1 && sm.getLogTerm(msg.prevLogIndex) == msg.prevLogTerm) {
			index = msg.prevLogIndex
			for j := 0; j < len(msg.entries); j += 1 {
				index += 1
				if sm.getLogTerm(index) != msg.entries[j].term {
					// LogStore(i, msg.entries[j])
				}
			}

			sm.commitIndex = min(msg.leaderCommit, index)
		} else {
			index = 0
		}
		// action = Send(msg.leaderId, AppendEntriesResp(sm.term,
		// 			 success=yes, matchIndex : index))
	}
}

func (sm *server) doVoteReq(msg VoteReq) {
	sm.termCheck(msg.term)

	if (sm.term == msg.term) && (sm.votedFor == -1 || sm.votedFor == msg.candidateID) {
		if msg.lastLogTerm > sm.getLogTerm(-1) || (msg.lastLogTerm == sm.getLogTerm(-1) && msg.lastLogIndex >= len(sm.log)-1) { // ?? Check id candidate is as uptodate
			sm.term = msg.term
			sm.votedFor = msg.candidateID
			sm.Alarm(genRand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2))

			// action = Send(msg.From, VoteResp(sm.CurrentTerm, voteGranted=yes))
		}
	} else { // reject vote:
		// action = Send(msg.from, VoteResp(sm.term, voteGranted=no))
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
	sm.Alarm(genRand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2))

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

			default:
				msg := msg1.(god)
				sm.termCheck(msg.term)
				fmt.Println("God called updated " + strconv.Itoa(sm.term))
				//Do Nothing
			}
		case <-sm.timer:
			sm.state = "Candidate"
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
				fmt.Println(peer)
				//action = Send(peer, VoteReq(sm.Id, sm.term, sm.Id, sm.lastLogIndex, sm.lastLogTerm))
			}
			fmt.Println("resetting")
			sm.Alarm(genRand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2))
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
					for peer, _ := range sm.allPeers {
						sm.nextIndex[peer] = len(sm.log) // ??
						sm.matchIndex[peer] = -1
						// send(peer, AppendEntriesReq(sm.Id, sm.term, sm.lastLogIndex, sm.lastLogTerm, [], sm.commitIndex))
					}
					sm.Alarm(genRand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2))

				}

			case "VoteReq":
				msg := msg1.(VoteReq)
				sm.doVoteReq(msg)

			case "AppendEntriesReq":
				msg := msg1.(AppendEntriesReq)
				sm.doAppendEntriesReq(msg)

			default:
				msg := msg1.(god)
				sm.termCheck(msg.term)
				fmt.Println("God called updated " + strconv.Itoa(sm.term))

			}

		case <-sm.timer:
			restartElection = true
			fmt.Println("haha")
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

				if sm.term == msg.term {
					if msg.success {
						sm.matchIndex[msg.from] = msg.matchIndex
						sm.nextIndex[msg.from] = msg.matchIndex + 1

						if sm.matchIndex[msg.from] < len(sm.log)-1 { //??
							sm.nextIndex[msg.from] = len(sm.log) - 1 // Doubt
							temp_prevLogIndex := sm.nextIndex[msg.from] - 1
							temp_prevLogTerm := sm.getLogTerm(temp_prevLogIndex)
							fmt.Println(temp_prevLogTerm)
							// funcall = AppendEntriesReq(sm.term, sm.Id, prevLogIndex, prevLogTerm,
							// entries[sm.nextIndex[msg.from]:len(log)],
							// leaderCommit)
							// action = Send(msg.from, funcall)
						}

						cnt := 0
						for peer, _ := range sm.allPeers {
							if sm.matchIndex[peer] > sm.commitIndex {
								cnt += 1
							}
						}
						if cnt > NUM_SERVERS/2 {
							sm.commitIndex++
							//Commit(index, data, err)
						}
					} else {
						sm.nextIndex[msg.from] = max(1, sm.nextIndex[msg.from]-1)
						/*action = Send(msg.from,
						  AppendEntriesReq(sm.Id, sm.term,
						  sm.nextIndex[msg.from], sm.log[sm.nextIndex[msg.from]].term,
						  sm.log[nextIndex[msg.from]:len(sm.log)],
						  sm.commitIndex))
						*/
					}
				}
			default:
				msg := msg1.(god)
				sm.termCheck(msg.term)
				fmt.Println("God called updated " + strconv.Itoa(sm.term))
			} //end of switch

		case <-sm.timer:
			for peer, _ := range sm.allPeers {
				if peer != sm.serverID {
					// send(i, AppendEntriesReq(sm.Id, sm.term, sm.lastLogIndex, sm.lastLogTerm, [], sm.commitIndex))
				}
				// Alarm(now() + rand(1.0, 2.0) * ELECTION_TIMEOUT)
			}
			sm.Alarm(genRand(HEARTBEAT_TIMEOUT, HEARTBEAT_TIMEOUT*2))
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
	}
	return &sm
}
