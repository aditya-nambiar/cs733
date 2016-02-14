package main

import (
	"fmt"
	"log"
	"testing"
	"time"
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
	defer testserver.stopServer()
	go testserver.eventloop()
	errorCheck("Follower", testserver.state, t, "")
	time.Sleep(1 * time.Second)
	errorCheck("Candidate", testserver.state, t, "")
}

func TestLeaderShipElection(t *testing.T) {
	testserver := newServer(1)
	defer testserver.stopServer()
	go testserver.eventloop()
	time.Sleep(1 * time.Second)
	errorCheck("Candidate", testserver.state, t, "66") // Converted to Candidate
	vr := VoteResp{from: 2, term: 2, voteGranted: true}
	testserver.netCh <- vr
	time.Sleep(500 * time.Millisecond)
	errorCheck("Follower", testserver.state, t, "69") // Got bigger term
	time.Sleep(1 * time.Second)
	errorCheck("Candidate", testserver.state, t, "71") // Converted to Candidate timed out
	testserver.netCh <- vr
	vr = VoteResp{from: 3, term: testserver.term, voteGranted: true}
	testserver.netCh <- vr
	vr = VoteResp{from: 4, term: testserver.term, voteGranted: true}
	testserver.netCh <- vr
	time.Sleep(500 * time.Millisecond)
	errorCheck("Leader", testserver.state, t, "75")

}

func TestLeaderShipElection1(t *testing.T) {

}
