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
