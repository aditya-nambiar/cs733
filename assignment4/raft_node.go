package main

// ASSUMPTION log starts from 0
import (
	"math/rand"
	"reflect"
"strconv"
	"sync"
	"time"
//"fmt"
	"fmt"
)

const (
	ELECTION_TIMEOUT  = 800 // milliseconds
	HEARTBEAT_TIMEOUT = 100  // milliseconds
	NUM_SERVERS       = 5
)

type LogEntry struct {
	Data      []byte
	Committed bool
	Term      int
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
	num_servers int
}

type Timeout struct {
}

type respEvent struct {
	From int
}

type Alarm struct {
	From int
	Time int // time in milliseconds
}

type Send struct {
	From   int
	PeerID int         // Id to send to
	Event  interface{} // Can be AppendEntriesReq/Resp or VoteReq/Resp
}

type Commit struct {
	From  int
	Index int
	Data  LogEntry
	Err   string
}

type LogStore struct {
	From  int
	Index int
	Data  []byte
}

type VoteReq struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	From         int
	LastLogTerm  int
}

type VoteResp struct {
	From        int
	Term        int
	VoteGranted bool
}

type AppendEntriesReq struct {
	From int
	Term         int //leader's current term
	LeaderID     int //for follower's to update themselves
	PrevLogIndex int // index of entry just preceding new ones
	PrevLogTerm  int // term of entry just preceding new ones
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesResp struct {
	From       int
	Term       int
	MatchIndex int // how much of log matches the server
	Success    bool
}

type ActionsCompleted struct{
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
		return sm.log[i].Term
	} else {
		return sm.log[len(sm.log)+i].Term
	}
}

func (sm *server) countVotes() int {
	count := 0
	for _, vote := range sm.voteGranted {
		if vote {
			count += 1
			//fmt.Println(peer)
		}

	}
	//fmt.Println("Count" + strconv.Itoa(count))
	return count
}

func (sm *server) logit(index int, data []byte, term int) {
	if index < len(sm.log) {
		sm.log[index] = LogEntry{Data: data, Term: term, Committed: false}
	} else {
		sm.log = append(sm.log, make([]LogEntry, index)...)
		sm.log[index] = LogEntry{Data: data, Term: term, Committed: false}
	}
	fmt.Println("ID : " + strconv.Itoa(sm.serverID) + " || data : " + string(data[:]) + " | index " + strconv.Itoa(index) )
	sm.actionCh <- LogStore{From : sm.serverID, Index: index, Data: data}

}

var  callonce = 1
func genRand(min int, max int) int {
	if callonce == 1 {
		rand.Seed( time.Now().UTC().UnixNano())
		callonce++

	}
	return rand.Intn(max-min) + min
}
func (sm *server) termCheck(mterm int) {
	if sm.term < mterm {
		sm.term = mterm
		sm.votedFor = -1
		sm.voteGranted = make(map[int]bool)
		sm.state = "Follower"
		sm.leaderID = -1
	}
}

func (sm * server) Shutdown(){
	sm.Stopped <- true
	close(sm.clientCh)
	close(sm.actionCh)
	close(sm.Stopped)

}
func (sm *server) doAppendEntriesResp(msg AppendEntriesResp) {
	sm.termCheck(msg.Term)

	if sm.term == msg.Term && sm.state == "Leader" {
		if msg.Success {
			sm.matchIndex[msg.From] = msg.MatchIndex
			sm.nextIndex[msg.From] = msg.MatchIndex + 1

			if sm.matchIndex[msg.From] < len(sm.log)-1 { //??
				sm.nextIndex[msg.From] = len(sm.log) - 1 // Doubt
				temp_prevLogIndex := sm.nextIndex[msg.From] - 1
				temp_prevLogTerm := sm.getLogTerm(temp_prevLogIndex)
				fmt.Println("Sending !@#$%%^%$# to " + strconv.Itoa(len(sm.log[sm.nextIndex[msg.From]:])))

				appendreq := AppendEntriesReq{Term: sm.term, From: sm.serverID, LeaderID: sm.serverID, PrevLogIndex: temp_prevLogIndex, PrevLogTerm: temp_prevLogTerm, Entries: sm.log[sm.nextIndex[msg.From]:], LeaderCommit: sm.commitIndex}
				sm.actionCh <- Send{From: sm.serverID, PeerID: msg.From, Event: appendreq}
			}

			cnt := 1
			for _, peer := range sm.allPeers {
				if peer != sm.serverID &&  sm.matchIndex[peer] > sm.commitIndex {
					cnt += 1
				}
				fmt.Println("MSG from " +strconv.Itoa(msg.From) +" Commit status " + strconv.Itoa(sm.commitIndex) + "Peer " + strconv.Itoa(peer) + " " + strconv.Itoa(sm.matchIndex[peer]))
			}

			if cnt > NUM_SERVERS/2 {
				sm.commitIndex++
				fmt.Println(" Commiting data 215 :: " + strconv.Itoa(sm.commitIndex) + " LEADER :" + strconv.Itoa(sm.serverID))
				dmsg, _ := BytesToMsg(sm.log[sm.commitIndex].Data)
				fmt.Println("I am here inside a RAFT NODE")
				fmt.Println(string(dmsg.Contents))
				fmt.Println(string(dmsg.Filename))
				sm.actionCh <- Commit{From: sm.serverID,Index: sm.commitIndex ,  Data: sm.log[sm.commitIndex], Err: ""}
			}
		} else {
			var lin int
			if sm.nextIndex[msg.From]-1 <= 0 {
				lin = -1
				//sm.nextIndex[msg.From] = 0

				sm.nextIndex[msg.From] = 0
			} else {
				lin = sm.nextIndex[msg.From] - 1
				sm.nextIndex[msg.From] = lin
			}
			//sm.nextIndex[msg.from] = max(0, sm.nextIndex[msg.from]-1)
			fmt.Println("Sending AREQ4 to " + strconv.Itoa(msg.From) +" len" + strconv.Itoa(len(sm.log[sm.nextIndex[msg.From]:])))

			var appendreq AppendEntriesReq
			appendreq = AppendEntriesReq{Term: sm.term, From: sm.serverID, LeaderID: sm.serverID, PrevLogIndex: lin, PrevLogTerm: sm.log[sm.nextIndex[msg.From]].Term, Entries: sm.log[sm.nextIndex[msg.From]:], LeaderCommit: sm.commitIndex}
			sm.actionCh <- Send{From: sm.serverID, PeerID: msg.From, Event: appendreq}
		}
	}
}

//Erase extraneous entries post mismatch 41:00
func (sm *server) doAppendEntriesReq(msg AppendEntriesReq) {

	if sm.term > msg.Term {
		appendresp := AppendEntriesResp{From: sm.serverID, Term: sm.term, MatchIndex: -1, Success: false}
		sm.actionCh <- appendresp
	} else {
		sm.actionCh <- Alarm{From: sm.serverID, Time: genRand(ELECTION_TIMEOUT*2, ELECTION_TIMEOUT*3)}
		sm.state = "Follower"
		var index int
		check := false
		sm.leaderID = msg.LeaderID

		if ( len(msg.Entries[0].Data) >0 &&  string(msg.Entries[0].Data[:1]) == "^"){
			index = msg.PrevLogIndex +1
			var old_val = sm.commitIndex
			sm.commitIndex = min(msg.LeaderCommit, index)
			//fmt.Println("Vals " + strconv.Itoa(msg.LeaderCommit) + " " + strconv.Itoa(index))

			//fmt.Println("Comparing " + strconv.Itoa(old_val) + " " + strconv.Itoa(sm.commitIndex))
			if ( old_val < sm.commitIndex) {
				fmt.Println(" Commiting data 256 " + string(sm.log[sm.commitIndex].Data[:]))
				sm.actionCh <- Commit{From: sm.serverID,Index: sm.commitIndex , Data: sm.log[sm.commitIndex], Err: ""}
			}
			return
		}
		if msg.PrevLogIndex == -1 || (msg.PrevLogIndex <= len(sm.log)-2 && sm.getLogTerm(msg.PrevLogIndex) == msg.PrevLogTerm) {
			index = msg.PrevLogIndex
			check = true
			fmt.Println("Prevlogindex " + strconv.Itoa(msg.PrevLogIndex) + " Length of msg = " + strconv.Itoa(len(msg.Entries)) +" "+ strconv.Itoa(sm.serverID))
			for j := 0; j < len(msg.Entries); j += 1 {

					index += 1
				//fmt.Println("Length of log " + strconv.Itoa(len(sm.log)))
				if (index >= len(sm.log) || sm.getLogTerm(index) != msg.Entries[j].Term ) {

					sm.logit(index, msg.Entries[j].Data, msg.Entries[j].Term)

					//entryToStore := LogStore{From: sm.serverID, Index: index, Data: msg.Entries[j].Data}
					//sm.actionCh <- entryToStore

				}

				//dmsg, _ := BytesToMsg( msg.Entries[j].Data)
				//fmt.Println("I am here crap shit")
				//fmt.Println(string(dmsg.Contents))
				//fmt.Println(string(dmsg.Filename))
				fmt.Println("Index : " + strconv.Itoa(index))
			}
			var old_val = sm.commitIndex
			sm.commitIndex = min(msg.LeaderCommit, index)
			if ( old_val < sm.commitIndex) {
				//fmt.Println(" Commiting data 280 " + string(sm.log[sm.commitIndex].Data[:]))

				sm.actionCh <- Commit{From: sm.serverID, Index: sm.commitIndex ,Data: sm.log[sm.commitIndex], Err: ""}
			}

		} else {
			index = -1
		}
		fmt.Println("From " + strconv.Itoa(sm.serverID) + " index : " + strconv.Itoa(index))
		appendresp := AppendEntriesResp{From: sm.serverID, Term: sm.term, MatchIndex: index, Success: check}
		sm.actionCh <- Send{From: sm.serverID, PeerID: msg.From, Event: appendresp}
	}
}

func (sm *server) doVoteReq(msg VoteReq) {
	sm.termCheck(msg.Term)


	if (sm.term == msg.Term) && (sm.votedFor == -1 || sm.votedFor == msg.CandidateID) {
		if msg.LastLogTerm > sm.getLogTerm(-1) || (msg.LastLogTerm == sm.getLogTerm(-1) && msg.LastLogIndex >= len(sm.log)-1) { // ?? Check id candidate is as uptodate
			sm.term = msg.Term
			sm.votedFor = msg.CandidateID
			sm.actionCh <- Alarm{From: sm.serverID, Time: genRand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2)}
			voteresp := VoteResp{From: sm.serverID, Term: sm.term, VoteGranted: true}
			//fmt.Println("Votegranted to " + strconv.Itoa(msg.CandidateID) + " By " + strconv.Itoa(sm.serverID))
			sm.actionCh <- Send{From: sm.serverID, PeerID: msg.CandidateID, Event: voteresp}
		}
	} else { // reject vote:
		voteresp := VoteResp{From: sm.serverID, Term: sm.term, VoteGranted: false}
		sm.actionCh <- Send{From: sm.serverID, PeerID: msg.From, Event: voteresp}
	}
}

func (sm *server) doVoteResp(msg VoteResp) {
	sm.termCheck(msg.Term)
	//fmt.Println("Received Vote")
	if sm.term == msg.Term {
		sm.voteGranted[msg.From] = msg.VoteGranted
	}
	if sm.countVotes() > NUM_SERVERS/2 {
		sm.state = "Leader"
		fmt.Println("LeaderElected " +strconv.Itoa(sm.serverID))
		sm.leaderID = sm.serverID
		info := []byte("^LeaderElected")
		logentries := make([]LogEntry, 1)
		logentry := LogEntry{Data: info, Term: sm.term}
		logentries[0] = logentry
		//sm.logit(len(sm.log), info, sm.term)
		//entryToStore := LogStore{From: sm.serverID, Index: len(sm.log), Data: info}
		//sm.actionCh <- entryToStore
		for _, peer := range sm.allPeers { // send heartbeats to all servers and establish territory
			if (peer != sm.serverID) {
				sm.nextIndex[peer] = len(sm.log) // ?? check
				//fmt.Println("Sending empty appends ############")
				sm.matchIndex[peer] = -1         // Add this empty entry t ur own log ?
				//fmt.Println("Sending AREQ1 to " + strconv.Itoa(peer))

				appendreq := AppendEntriesReq{Term: sm.term, From: sm.serverID, LeaderID: sm.serverID, PrevLogIndex: len(sm.log) - 1, PrevLogTerm: sm.getLogTerm(-1), Entries: logentries, LeaderCommit: sm.commitIndex}
				sm.actionCh <- Send{From: sm.serverID, PeerID: peer, Event: appendreq}
			}
		}
		sm.actionCh <- Alarm{From: sm.serverID, Time: genRand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2)}
	}

}

func (sm *server) doLeaderTimedOut() {
	info := []byte("^LeaderTimeout")
	logentries := make([]LogEntry, 1)
	logentry := LogEntry{Data: info, Term: sm.term}
	logentries[0] = logentry
	//sm.logit(len(sm.log), info, sm.term)
	//entryToStore := LogStore{From: sm.serverID, Index: len(sm.log), Data: info}
	//sm.actionCh <- entryToStore
	for _, peer := range sm.allPeers {
		if peer != sm.serverID {
			//fmt.Println("Sending AREQ to " + strconv.Itoa(peer))
			appendreq := AppendEntriesReq{Term: sm.term, From: sm.serverID, LeaderID: sm.serverID, PrevLogIndex: len(sm.log) - 1, PrevLogTerm: sm.getLogTerm(-1), Entries: logentries, LeaderCommit: sm.commitIndex}
			sm.actionCh <- Send{From: sm.serverID, PeerID: peer, Event: appendreq}
		}
	}
	//fmt.Print(".")
	sm.actionCh <- Alarm{From: sm.serverID, Time: genRand(HEARTBEAT_TIMEOUT, HEARTBEAT_TIMEOUT*2)}
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
	sm.actionCh <- Alarm{From: sm.serverID, Time: genRand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2)}
	sm.leaderID = -1
	fmt.Println("Follower")
	for sm.state == "Follower" {

		select {
		case <-sm.Stopped:
			sm.state = "Stopped"
			return
		//case  appendmsg := <-sm.clientCh:
		//	logentry := LogEntry{Data:appendmsg.([]byte), Term: sm.term}
		//	fmt.Println("Follower got append")
		//	sm.actionCh <- Commit{From : sm.serverID, Data : logentry, Index : -2}

		case msg1 := <-sm.netCh:
			//fmt.Print("#F "  +strconv.Itoa(sm.serverID) +" ")
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
				//msg := msg1.(god)
				//sm.termCheck(msg.term)
				//fmt.Println("God called updated " + strconv.Itoa(sm.term))
				////Do Nothing
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
			fmt.Println("Restarting Election by " + strconv.Itoa(sm.serverID) + "term : " + strconv.Itoa(sm.term))
			sm.voteGranted[sm.serverID] = true
			// Send RequestVote RPCs to all other servers.
			for _, peer := range sm.allPeers {
				votereq := VoteReq{Term: sm.term, CandidateID: sm.serverID, LastLogIndex: len(sm.log) - 1, LastLogTerm: sm.getLogTerm(-1)}
				sm.actionCh <- Send{From: sm.serverID, PeerID: peer, Event: votereq}
			}
			//fmt.Println("resetting")
			sm.actionCh <- Alarm{From: sm.serverID, Time: genRand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2)}
			restartElection = false
		}

		select {
		case <-sm.Stopped:
			sm.state = "Stopped"
			return

		//case  appendmsg := <-sm.clientCh:
		//	logentry := LogEntry{Data:appendmsg.([]byte), Term: sm.term}
		//	fmt.Println("Candidate got append")
		//
		//	sm.actionCh <- Commit{From : sm.serverID, Data : logentry, Index : -2}

		case msg1 := <-sm.netCh:
			//fmt.Print("#C "  +strconv.Itoa(sm.serverID) +" ")

			t := reflect.TypeOf(msg1)
			switch t.Name() {
			case "VoteResp":
				msg := msg1.(VoteResp)
				sm.doVoteResp(msg)
			case "VoteReq":
				msg := msg1.(VoteReq)
				sm.doVoteReq(msg)

			case "AppendEntriesReq":
				msg := msg1.(AppendEntriesReq)
				sm.doAppendEntriesReq(msg)

			case "Timeout":
				if ( sm.state == "Candidate") {
					restartElection = true
				}


			default:
			//msg := msg1.(god)
			//sm.termCheck(msg.term)
			//fmt.Println("God called updated " + strconv.Itoa(sm.term))

			}
		}
	}
}

// The event loop that is run when the server is in a Leader state.
func (sm *server) leaderLoop() {
	fmt.Println("I am a leader " + strconv.Itoa(sm.serverID))
	sm.leaderID = sm.serverID

	for sm.state == "Leader" {

		select {
		case <-sm.Stopped:
			sm.state = "Stopped"
			return

		case appendMsg := <-sm.clientCh:
			fmt.Println("LEader got append")

			if appendMsg == nil {
				return
			}
			dt := appendMsg.(AppendMsg)

		//fmt.Println("Got message from client")
			for i:=0; i<sm.num_servers; i++ {
				if (i != sm.serverID) {
					logentry := LogEntry{Data: dt.Data, Term: sm.term}
					logentries := make([]LogEntry, 1)
					logentries[0] = logentry

					appendreq := AppendEntriesReq{Term: sm.term, From: sm.serverID, LeaderID: sm.serverID, PrevLogIndex: len(sm.log) - 1, PrevLogTerm: sm.getLogTerm(-1), Entries: logentries, LeaderCommit: sm.commitIndex}
					fmt.Println("YOO " + strconv.Itoa(len(appendreq.Entries)) + " Leader comit " + strconv.Itoa(sm.commitIndex))

					sm.actionCh <- Send{From: sm.serverID, PeerID: i, Event: appendreq}
				}
			}
			sm.logit(len(sm.log), dt.Data, sm.term)


		case msg1 := <-sm.netCh:
			t := reflect.TypeOf(msg1)
			//fmt.Print("#L "  +strconv.Itoa(sm.serverID) +" ")

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
				sm.doAppendEntriesResp(msg)

			case "Timeout":
				sm.doLeaderTimedOut()

			default:
			//msg := msg1.(god)
			//sm.termCheck(msg.term)
			//fmt.Println("God called updated " + strconv.Itoa(sm.term))
			} //end of switch
		}
	}
}

func newServer(id int,l int ) *server {
	sm := server{
		serverID:    id,
		voteGranted: make(map[int]bool),
		nextIndex:   make(map[int]int),
		matchIndex:  make(map[int]int),
		clientCh:    make(chan interface{}, 100),
		netCh:       make(chan interface{}, 100),
		actionCh:    make(chan interface{}, 100),
		Stopped:     make(chan bool, 2),
		votedFor:    -1,
		state:       "Follower",
		term:        0,
		commitIndex: 0,
		leaderID: -1,
	}

	info := make([]byte, 1)
	logentries := make([]LogEntry, 1)
	logentry := LogEntry{Data: info, Term: 0}
	logentries[0] = logentry
	sm.log = logentries
	sm.num_servers = l
	//var f []int
	sm.allPeers = make([]int, 5)
	for j:= 0; j<l; j++{
		sm.allPeers[j] = j
	}
	return &sm
}
