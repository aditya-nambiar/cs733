package main

import (
//"fmt"
	"testing"
	"time"
	//"fmt"
	"math/rand"
	"sync"
	"fmt"
	//"strconv"
)
func init(){
	rand.Seed( time.Now().UTC().UnixNano())

}

func TestBasicRaft(t *testing.T) {

	rafts := makeRafts() // array of []raft.Node

	for i:=0; i<5; i++ {
	defer rafts[i].raft_log.Close()
	go rafts[i].processEvents()
	}


	time.Sleep(3*time.Second)
	var ldr *RaftNode
	for {
		ldr = getLeader(rafts)
		if ldr != nil{
			break
		}
	}

	ldr.Append([]byte("foo"))
	time.Sleep(1*time.Second)

	for _, node := range rafts {
	select {
	case ci := <- node.CommitChannel():

		if string(string(ci.Data.Data)) != "foo" {
		t.Fatal("Got different data")
		} else{
		//fmt.Println("Proper Commit ", ci.Index)
		}
	}
	}

	for _, node := range rafts {
		node.Shutdown()
	}
}
func TestLeaderCreation(t *testing.T) {

	rafts, _ := makeMockRafts() // array of []raft.Node

	for i:=0; i<5; i++ {
		defer rafts[i].raft_log.Close()
		go rafts[i].processEvents()
	}


	time.Sleep(3*time.Second)
	var ldr *RaftNode
	for {
		ldr = getLeader(rafts)
		if ldr != nil{
			break
		}
	}

	ldr.Shutdown()
	time.Sleep(3*time.Second)
	var ldr2 *RaftNode

	for {
		ldr2 = getLeader(rafts)
		if (ldr2 != nil) {
			break
		}
		fmt.Println("SDfds")
	}

	if (ldr.sm.serverID ==ldr2.sm.serverID){
		t.Fatal("Leader Has not changed")
	}

}


func TestShutDown(t *testing.T) {
	rafts, _ := makeMockRafts() // array of []raft.Node

	for i:=0; i<5; i++ {
		defer rafts[i].raft_log.Close()
		go rafts[i].processEvents()
	}


	time.Sleep(2*time.Second)
	var mutex sync.RWMutex
	var ldr *RaftNode
	for {
		mutex.Lock()
		ldr = getLeader(rafts)

		if ldr != nil{
			break
		}
		mutex.Unlock()


	}

	ldr.Append([]byte("foo"))
	time.Sleep(1*time.Second)

	for _, node := range rafts {
		select {
		case ci := <- node.CommitChannel():

			if string(string(ci.Data.Data)) != "foo" {
				t.Fatal("Got different data")
			} else{
				//fmt.Println("Proper Commit ", ci.Index)
			}
		}
	}

	for _, node := range rafts {
		node.Shutdown()
	}

	time.Sleep(1*time.Second)

	rafts2, _ := makeMockRafts() // array of []raft.Node

	for i:=0; i<5; i++ {
		defer rafts2[i].raft_log.Close()
		go rafts2[i].processEvents()
	}

	time.Sleep(1*time.Second)
	for {
		ldr = getLeader(rafts2)
		if (ldr != nil) {
			break
		}
	}
	ldr.Append([]byte("fooAgain"))
	time.Sleep(2*time.Second)

	for _, node := range rafts2 {
		select {
		case ci := <- node.CommitChannel():
			//if ci.Err != nil {t.Fatal(ci.Err)}
			if string(ci.Data.Data) != "fooAgain" {
				t.Fatal("Got different data " + string(ci.Data.Data))
			}
		default: //fmt.Println("Expected message on all nodes")
		}
	}

	// Leader should have seen previous log entry from its log on disk
	b, _ := ldr.Get(1)
	if string(b) != "fooAgain" {t.Fatal("Log not initialised correctly")}

	for _, node := range rafts2 {
		node.Shutdown()
	}
}

// Test if even after the network partitions the everything works well
func TestPartitionOfCluster(t *testing.T) {


	rafts, cluster := makeMockRafts() // array of []raft.Node

	for i:=0; i<5; i++ {
		defer rafts[i].raft_log.Close()
		go rafts[i].processEvents()
	}

	time.Sleep(2*time.Second)
	var ldr *RaftNode
	var mutex sync.RWMutex
	for {
		mutex.Lock()
		ldr = getLeader(rafts)
		if (ldr != nil) {
			break
		}
		mutex.Unlock()
	}

	ldr.Append([]byte("foo"))
	time.Sleep(2*time.Second)

	for _, node := range rafts {
		select {
		case ci := <- node.CommitChannel():
			//if ci.Err != nil {t.Fatal(ci.Err)}
			if string(ci.Data.Data) != "foo" {
				t.Fatal("Got different data")
			}
		default:
		}
	}


	for {
		ldr = getLeader(rafts)
		if (ldr != nil) {
			break
		}
	}

	if(ldr.Id() ==  1 || ldr.Id() == 0) {
		cluster.Partition([]int{0, 1}, []int{2, 3, 4})
	} else if(ldr.Id()  ==  2)  {
		cluster.Partition([]int{0, 1, 3}, []int{2, 4})
	} else {
		cluster.Partition([]int{0, 1, 2}, []int{3, 4})
	}

	ldr.Append([]byte("foo2"))
	var ldr2 *RaftNode

	time.Sleep(2*time.Second)

	for _, node := range rafts {
		select {
		case ci := <- node.CommitChannel():
			t.Fatal("Got different data "+ string(ci.Data.Data))
		default:
		}
	}

	cluster.Heal()

	time.Sleep(3*time.Second)
	for {
		ldr2 = getLeader(rafts)

		if (ldr2 != nil && ldr2.sm.serverID != ldr.sm.serverID) {
			break
		}
	}

	// Leader will not have "fooAgain" entry, will force new entry to all nodes
	ldr2.Append([]byte("foo3"))
	time.Sleep(2*time.Second)

	for _, node := range rafts {
		select {
		case ci := <- node.CommitChannel():
			if string(ci.Data.Data) != "foo3" {
				t.Fatal("Got different data "+ string(ci.Data.Data))
			}
		}
	}

	for _, node := range rafts {
		node.Shutdown()
	}

}