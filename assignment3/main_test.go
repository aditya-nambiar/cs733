package main

import (
	"fmt"
	"log"
	"reflect"
	"strconv"
	"testing"
)

func check(obj interface{}) {
	if obj != nil {
		fmt.Println(obj)
	}
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func errorCheck(expected string, response string, t *testing.T, err string) {
	if expected != response {
		if err != "" {
			t.Fatal("Expected : " + expected + " Got : " + response + " | " + err)
		} else {
			t.Fatal("Expected : " + expected + " Got : " + response)

		}
	}
}

func TestConversionToCandidate(t *testing.T) {
	testserver := newServer(1)
	//defer testserver.stopServer()
	tOut := Timeout{}
	testserver.netCh <- tOut
	testserver.followerLoop()
	out3 := <-testserver.actionCh
	errorCheck("Alarm", reflect.TypeOf(out3).Name(), t, "37")
	errorCheck("Candidate", testserver.state, t, "41")
	testserver.Stopped <- true
	testserver.candidateLoop()

	for i := 2; i <= 6; i++ {
		out2 := <-testserver.actionCh
		errorCheck("Send", reflect.TypeOf(out2).Name(), t, "44")
		out1 := out2.(Send)
		fmt.Println(out1.peerID)

		if out1.peerID > 6 || out1.peerID < 2 {
			errorCheck("2 to 6", strconv.Itoa(out1.peerID), t, "Send to wrong neighbours")
		}
	}
	out3 = <-testserver.actionCh
	errorCheck("Alarm", reflect.TypeOf(out3).Name(), t, "37")

}

func TestSteppingDown(t *testing.T) {
	testserver := newServer(1)
	testserver.state = "Candidate"
	testserver.term = 1
	vr := VoteResp{from: 3, term: 3, voteGranted: true}
	testserver.doVoteResp(vr)
	errorCheck("Follower", testserver.state, t, "65") // Larger term
}

func TestLeaderShipElection(t *testing.T) {
	testserver := newServer(1)
	testserver.state = "Candidate"
	testserver.term = 1
	vr := VoteResp{from: 3, term: testserver.term, voteGranted: true}
	testserver.doVoteResp(vr)
	errorCheck("Candidate", testserver.state, t, "74")
	testserver.doVoteResp(vr)
	testserver.doVoteResp(vr)
	testserver.doVoteResp(vr)

	errorCheck("Candidate", testserver.state, t, "76")
	vr.from = 2
	testserver.doVoteResp(vr)

	errorCheck("Candidate", testserver.state, t, "76")
	vr.from = 4
	testserver.doVoteResp(vr)
	errorCheck("Leader", testserver.state, t, "76")

	for i := 2; i <= 6; i++ {
		out2 := <-testserver.actionCh
		errorCheck("LogStore", reflect.TypeOf(out2).Name(), t, "79")
		out2 = <-testserver.actionCh
		errorCheck("Send", reflect.TypeOf(out2).Name(), t, "94")
	}

}

func TestLeaderSendHearbeats(t *testing.T) {
	testserver := newServer(1)
	testserver.state = "Leader"
	testserver.doLeaderTimedOut()
	for i := 2; i <= 6; i++ {
		out2 := <-testserver.actionCh
		errorCheck("LogStore", reflect.TypeOf(out2).Name(), t, "103")
		out2 = <-testserver.actionCh
		errorCheck("Send", reflect.TypeOf(out2).Name(), t, "105")
	}
	out3 := <-testserver.actionCh
	errorCheck("Alarm", reflect.TypeOf(out3).Name(), t, "108")

}

func TestAppendEntriesReq(t *testing.T) {
	testserver := newServer(1)
	testserver.state = "Follower"
	testserver.term = 3
	info := []byte{'s', 's', 'd', 'd', '#'}
	logentries := make([]LogEntry, 1)
	logentry := LogEntry{data: info, term: 6}
	logentries[0] = logentry
	appendreq := AppendEntriesReq{term: 1, leaderID: 1, prevLogIndex: 1, prevLogTerm: 1, entries: logentries, leaderCommit: 1}
	testserver.doAppendEntriesReq(appendreq)
	errorCheck("1", strconv.Itoa(len(testserver.log)), t, "123") // Dropped because lower term
	out3 := <-testserver.actionCh
	if out3.(AppendEntriesResp).success {
		errorCheck("0", "1", t, "Returned a success 124")
	}
	errorCheck("AppendEntriesResp", reflect.TypeOf(out3).Name(), t, "125")
	appendreq = AppendEntriesReq{term: 4, leaderID: 1, prevLogIndex: 0, prevLogTerm: 0, entries: logentries, leaderCommit: 1}
	testserver.doAppendEntriesReq(appendreq)
	errorCheck("2", strconv.Itoa(len(testserver.log)), t, "130")
	out3 = <-testserver.actionCh
	errorCheck("Alarm", reflect.TypeOf(out3).Name(), t, "132")
	out3 = <-testserver.actionCh
	errorCheck("LogStore", reflect.TypeOf(out3).Name(), t, "134")
	out3 = <-testserver.actionCh
	errorCheck("AppendEntriesResp", reflect.TypeOf(out3).Name(), t, "136")
	if !out3.(AppendEntriesResp).success {
		errorCheck("0", "1", t, "Returned a failure 138")
	}
}

func TestAppendEntriesReqReWriteLog(t *testing.T) {
	testserver := newServer(1)
	testserver.state = "Follower"
	testserver.term = 1
	info := []byte{'#', '#'}
	logentries := make([]LogEntry, 8)
	logentry := LogEntry{data: info, term: 1}
	for i := 0; i < 8; i++ {
		logentries[i] = logentry
	}
	appendreq := AppendEntriesReq{term: 4, leaderID: 1, prevLogIndex: 7, prevLogTerm: 2, entries: logentries, leaderCommit: 1}
	testserver.doAppendEntriesReq(appendreq)
	out3 := <-testserver.actionCh
	errorCheck("Alarm", reflect.TypeOf(out3).Name(), t, "150")
	out3 = <-testserver.actionCh
	errorCheck("AppendEntriesResp", reflect.TypeOf(out3).Name(), t, "152")
	errorCheck("-1", strconv.Itoa(out3.(AppendEntriesResp).matchIndex), t, "156")
	info = []byte{'$', '$'}
	logentries = make([]LogEntry, 8)
	logentry = LogEntry{data: info, term: 4}
	for i := 0; i < 8; i++ {
		logentries[i] = logentry
	}
	appendreq = AppendEntriesReq{term: 4, leaderID: 1, prevLogIndex: -1, prevLogTerm: 4, entries: logentries, leaderCommit: 1}
	testserver.doAppendEntriesReq(appendreq)
	out3 = <-testserver.actionCh
	errorCheck("Alarm", reflect.TypeOf(out3).Name(), t, "168")
	for i := 0; i < 8; i++ {
		out3 = <-testserver.actionCh
		errorCheck("LogStore", reflect.TypeOf(out3).Name(), t, "170")
	}
	out3 = <-testserver.actionCh
	errorCheck("AppendEntriesResp", reflect.TypeOf(out3).Name(), t, "172")
	if !out3.(AppendEntriesResp).success {
		errorCheck("0", "1", t, "Returned a failure 174")
	}
	for i := 0; i < len(testserver.log); i++ {
		if testserver.log[i].data[0] != '$' || testserver.log[i].data[1] != '$' {
			errorCheck("0", "1", t, "Data has not changed 178")

		}
	}
}

func TestLeaderFailuresinAppend(t *testing.T) {
	testserver := newServer(1)
	testserver.state = "Leader"
	testserver.term = 4
	info := []byte{'#', '#'}
	logentries := make([]LogEntry, 8)
	logentry := LogEntry{data: info, term: 1}
	for i := 0; i < 8; i++ {
		logentries[i] = logentry
	}
	testserver.log = logentries
	for i := 2; i <= 6; i++ {
		testserver.nextIndex[i] = i + 2
	}
	appendresp := AppendEntriesResp{from: 2, term: 4, matchIndex: 5, success: false}
	testserver.doAppendEntriesResp(appendresp)
	out3 := <-testserver.actionCh
	resp := out3.(Send).event.(AppendEntriesReq)
	errorCheck("3", strconv.Itoa(resp.prevLogIndex), t, "203")
	testserver.nextIndex[2] = 1
	appendresp = AppendEntriesResp{from: 2, term: 4, matchIndex: 0, success: false}
	testserver.doAppendEntriesResp(appendresp)
	out3 = <-testserver.actionCh
	resp = out3.(Send).event.(AppendEntriesReq)
	errorCheck("-1", strconv.Itoa(resp.prevLogIndex), t, "208")
	errorCheck("8", strconv.Itoa(len(resp.entries)), t, "209")

}

func TestLeaderSuccessinAppend(t *testing.T) {
	testserver := newServer(1)
	testserver.state = "Leader"
	testserver.term = 4
	testserver.commitIndex = 0
	info := []byte{'#', '#'}
	logentries := make([]LogEntry, 8)
	logentry := LogEntry{data: info, term: 1}
	for i := 0; i < 8; i++ {
		logentries[i] = logentry
	}
	testserver.log = logentries
	for i := 2; i <= 6; i++ {
		testserver.nextIndex[i] = i + 2
	}
	appendresp := AppendEntriesResp{from: 2, term: 4, matchIndex: 5, success: true}
	testserver.doAppendEntriesResp(appendresp)
	out3 := <-testserver.actionCh
	resp := out3.(Send).event.(AppendEntriesReq)
	errorCheck("6", strconv.Itoa(resp.prevLogIndex), t, "233")
	appendresp = AppendEntriesResp{from: 3, term: 4, matchIndex: 4, success: true}
	testserver.doAppendEntriesResp(appendresp)
	out3 = <-testserver.actionCh
	resp = out3.(Send).event.(AppendEntriesReq)

	errorCheck("6", strconv.Itoa(resp.prevLogIndex), t, "239")

	appendresp = AppendEntriesResp{from: 4, term: 4, matchIndex: 3, success: true}
	errorCheck("0", strconv.Itoa(testserver.commitIndex), t, "244")
	testserver.doAppendEntriesResp(appendresp)
	out3 = <-testserver.actionCh
	resp = out3.(Send).event.(AppendEntriesReq)
	errorCheck("1", strconv.Itoa(testserver.commitIndex), t, "244")
	errorCheck("6", strconv.Itoa(resp.prevLogIndex), t, "246")

}

func TestVoteReq(t *testing.T) {
	testserver := newServer(1)
	testserver.state = "Follower"
	testserver.term = 2
	votereq := VoteReq{term: 4, candidateID: 2, lastLogIndex: 5, lastLogTerm: 5}
	testserver.doVoteReq(votereq)
	out3 := <-testserver.actionCh
	out3 = <-testserver.actionCh

	voteresp := out3.(VoteResp)
	if !voteresp.voteGranted {
		errorCheck("0", "1", t, "Incorrect vote given 263")
	}
	votereq = VoteReq{term: 1, candidateID: 2, lastLogIndex: 5, lastLogTerm: 5}
	testserver.doVoteReq(votereq)
	out3 = <-testserver.actionCh
	voteresp = out3.(VoteResp)
	if voteresp.voteGranted {
		errorCheck("0", "1", t, "Incorrect vote given 270")
	}
	info := []byte{'#', '#'}
	logentries := make([]LogEntry, 8)
	logentry := LogEntry{data: info, term: 4}
	for i := 0; i < 6; i++ {
		logentries[i] = logentry
	}
	testserver.log = logentries
	errorCheck("2", strconv.Itoa(testserver.votedFor), t, "279")
	testserver.votedFor = -1
	votereq = VoteReq{term: 4, candidateID: 2, lastLogIndex: 2, lastLogTerm: 5}
	testserver.doVoteReq(votereq)

	out3 = <-testserver.actionCh
	out3 = <-testserver.actionCh

	voteresp = out3.(VoteResp)
	if !voteresp.voteGranted {
		errorCheck("0", "1", t, "Incorrect vote given 286")
	}

	testserver.votedFor = -1
	votereq = VoteReq{term: 4, candidateID: 2, lastLogIndex: 6, lastLogTerm: 6}
	testserver.doVoteReq(votereq)

	out3 = <-testserver.actionCh
	out3 = <-testserver.actionCh

	voteresp = out3.(VoteResp)
	if !voteresp.voteGranted {
		errorCheck("0", "1", t, "Incorrect vote given 286")
	}

}
