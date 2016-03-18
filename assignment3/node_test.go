package main

import (
//"fmt"
	"testing"
	"time"
	"fmt"
)

func TestRaft(t *testing.T) {

	rafts := makeRafts() // array of []raft.Node

	for i:=0; i<5; i++ {
	///defer rafts[i].raft_log.Close()
	go rafts[i].processEvents()
	}

	time.Sleep(1*time.Second)
	var ldr *RaftNode
	for {
	ldr = getLeader(rafts)
	if (ldr != nil) {
	break
	}
	}

	ldr.Append([]byte("foo"))
	time.Sleep(1*time.Second)

	for _, node := range rafts {
	select {
	case ci := <- node.CommitChannel():
		if string(ci.Data.data) != "foo" {
		t.Fatal("Got different data")
		} else{
		//fmt.Println("Proper Commit ", ci.Index)
		}
		fmt.Println(string(ci.Data.data))
	}
	}


}
