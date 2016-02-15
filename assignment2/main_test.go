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
	defer testserver.stopServer()
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
	out3 := <-testserver.actionCh
	errorCheck("Alarm", reflect.TypeOf(out3).Name(), t, "37")

}

func TestLeaderShipElection(t *testing.T) {
	testserver := newServer(1)
	//defer testserver.stopServer()
	go testserver.eventloop()
	testserver.state = "Candidate"

	for i := 2; i <= 6; i++ {
		<-testserver.actionCh
	}
	<-testserver.actionCh
	vr := VoteResp{from: 2, term: 4, voteGranted: true}
	testserver.netCh <- vr
	errorCheck("Follower", testserver.state, t, "77") // Got bigger term
	<-testserver.actionCh
	tOut := Timeout{}
	testserver.netCh <- tOut
	time.Sleep(100 * time.Millisecond)
	errorCheck("Candidate", testserver.state, t, "80") // Converted to Candidate timed out
	for i := 2; i <= 6; i++ {
		<-testserver.actionCh
	}
	<-testserver.actionCh
	testserver.netCh <- vr
	vr = VoteResp{from: 3, term: testserver.term, voteGranted: true}
	testserver.netCh <- vr
	vr = VoteResp{from: 4, term: testserver.term, voteGranted: true}
	testserver.netCh <- vr
	time.Sleep(100 * time.Millisecond)
	errorCheck("Leader", testserver.state, t, "87")

}
