package main

import (
	"fmt"
	"log"
	"reflect"
	"strconv"
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
	//defer testserver.stopServer()
	go testserver.eventloop()
	out := <-testserver.actionCh
	errorCheck("Alarm", reflect.TypeOf(out).Name(), t, "37")
	tOut := Timeout{}
	errorCheck("Follower", testserver.state, t, "39")
	testserver.netCh <- tOut
	time.Sleep(100 * time.Millisecond)
	errorCheck("Candidate", testserver.state, t, "41")
	for i := 2; i <= 6; i++ {
		out2 := <-testserver.actionCh
		errorCheck("Send", reflect.TypeOf(out2).Name(), t, "44")
		out1 := out2.(Send)
		fmt.Println(out1.peerID)

		if out1.peerID > 6 || out1.peerID < 2 {
			errorCheck("2 to 6", strconv.Itoa(out1.peerID), t, "Send to wrong neighbours")
		}
	}
}

// func TestLeaderShipElection(t *testing.T) {
// 	testserver := newServer(1)
// 	defer testserver.stopServer()
// 	go testserver.eventloop()
// 	time.Sleep(1 * time.Second)
// 	errorCheck("Candidate", testserver.state, t, "66") // Converted to Candidate
// 	vr := VoteResp{from: 2, term: 2, voteGranted: true}
// 	testserver.netCh <- vr
// 	time.Sleep(500 * time.Millisecond)
// 	errorCheck("Follower", testserver.state, t, "69") // Got bigger term
// 	time.Sleep(1 * time.Second)
// 	errorCheck("Candidate", testserver.state, t, "71") // Converted to Candidate timed out
// 	testserver.netCh <- vr
// 	vr = VoteResp{from: 3, term: testserver.term, voteGranted: true}
// 	testserver.netCh <- vr
// 	vr = VoteResp{from: 4, term: testserver.term, voteGranted: true}
// 	testserver.netCh <- vr
// 	time.Sleep(500 * time.Millisecond)
// 	errorCheck("Leader", testserver.state, t, "75")

// }
