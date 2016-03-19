package main

import "github.com/cs733-iitb/cluster"
import "time"
import "github.com/cs733-iitb/log"
import "strconv"
import "fmt"
import "reflect"
import "github.com/syndtr/goleveldb/leveldb"
import (
	"sync"
	"encoding/gob"
	"github.com/cs733-iitb/cluster/mock"
)

type Node interface {
	// Client's message to Raft node
	Append([]byte)
	// A channel for client to listen on. What goes into Append must come out of here at some point.
	CommitChannel() <-chan CommitInfo
	// Last known committed index in the log. This could be -1 until the system stabilizes.
	CommittedIndex() int
	// Returns the data at a log index, or an error.
	Get(index int) (error, []byte)
	// Node's id
	Id()
	// Id of leader. -1 if unknown
	LeaderId() int
	// Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.
	Shutdown()
}

// data goes in via Append, comes out as CommitInfo from the node's CommitChannel
// Index is valid only if err == nil
type CommitInfo struct {
	Data  LogEntry
	Index int64 // or int .. whatever you have in your code
	Err   string // Err can be errred
}

// This is an example structure for Config .. change it to your convenience.
type Config struct {
	cluster          cluster.Config // Information about all servers, including this.
	Id               int         // this node's id. One of the cluster's entries should match.
	LogDir           string      // Log file directory for this node
	ElectionTimeout  int
	HeartbeatTimeout int
}

type NetConfig struct {
	Id   int
	Host string
	Port int
}

type RaftNode struct {
	sm          * server
	timer       *time.Timer
	commitCh    chan CommitInfo
	shutdown      chan bool
	cluster     cluster.Config
	serverMailBox  cluster.Server
	LogDir      string // Log file directory for this node
	raft_log          *log.Log
	consistentStore *leveldb.DB
	mutex1	 *sync.RWMutex // Ensure integrity of state machine fields
	mutex2	 *sync.RWMutex // Ensure integrity of state machine fields

	logMutex *sync.RWMutex	// Ensure correctness of log
	electionTimeout time.Duration
}

func NewRaftNode(config Config, id int) RaftNode {
	var node RaftNode
	node.cluster = config.cluster
	node.LogDir = config.LogDir
	node.raft_log, _ = log.Open(node.LogDir + "/Log"+strconv.Itoa(genRand(1,1000)) + strconv.Itoa(config.Id))
	node.commitCh = make(chan CommitInfo, 100)
	node.shutdown = make(chan bool, 5)
	node.sm = newServer(id, len(configs.Peers)) // change to ID
	node.mutex1 = &sync.RWMutex{}
	node.mutex2 = &sync.RWMutex{}

	node.logMutex = &sync.RWMutex{}

	return node
}


// Client's message to Raft node
func (node *RaftNode) Append(data []byte) {
	//fmt.Println("Append")
	node.sm.clientCh <- AppendMsg{Data:data}
}

// A channel for client to listen on. What goes into Append must come out of here at some point.
func (node *RaftNode) CommitChannel() <-chan CommitInfo {
	return node.commitCh
}

// Last known committed index in the log. This could be -1 until the system stabilizes.

func (node *RaftNode) CommittedIndex() int {
	node.mutex1.RLock()
	c := node.sm.commitIndex
	node.mutex1.RUnlock()
	return c
}

// Returns the data at a log index, or an error.

func (node *RaftNode) Get(index int64) ( []byte, error) {
	node.logMutex.RLock()

	c, err := node.raft_log.Get(index)
	node.logMutex.RUnlock()
	return c, err
}

func (node *RaftNode) Id() int {
	return node.sm.serverID
}

// Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.

func (node *RaftNode) Shutdown() {
	node.shutdown <- true
	node.shutdown <- true

	time.Sleep(1 * time.Second)
	node.sm.Shutdown()
	time.Sleep(1 * time.Second)

	node.sm.state = "Shutdown"
	node.sm.leaderID = -1
	db, _ := leveldb.OpenFile("$GOPATH/src/github.com/aditya-nambiar/cs733/assignment3/consistentStore", nil)
	defer db.Close()

	db.Put([]byte("Term:" +strconv.Itoa(node.sm.serverID)), []byte(strconv.Itoa(node.sm.term)), nil)
	db.Put([]byte("VotedFor:"+strconv.Itoa(node.sm.serverID)), []byte(strconv.Itoa(node.sm.votedFor)), nil)

	close(node.commitCh)

	node.timer.Stop()
	node.raft_log.Close()
}

//********************************************************************************************



// Process output events such as Send, Alarm etc.
func (node *RaftNode) process(ev  interface{}) {
	t := reflect.TypeOf(ev)
	if (t.Name() == "Alarm") {
		node.timer.Reset(time.Duration(ev.(Alarm).Time) * time.Millisecond)
		//fmt.Println("Reset alarm for " + strconv.Itoa(node.sm.serverID))
	} else if (t.Name() == "LogStore") {
		obj := ev.(LogStore)

		//fmt.Println("LogStore by " + strconv.Itoa(node.sm.serverID) + "With data :: "+ string(obj.Data[:]))

		node.logMutex.Lock()
		node.raft_log.TruncateToEnd(int64(obj.Index))
		node.raft_log.Append(obj.Data)
		node.logMutex.Unlock()

	} else if (t.Name() == "Commit") {
		obj := ev.(Commit)
		//fmt.Println("Commit by " + strconv.Itoa(node.sm.serverID) +" with data :: " + string(obj.Data.Data[:]))
		out := CommitInfo{obj.Data, int64(obj.Index), obj.Err}
		node.commitCh <- out
	} else if (t.Name() == "Send") {
		ev1 := ev.(Send)
		//fmt.Println(" Sending From " + strconv.Itoa(ev1.From))
		//fmt.Print("Send ", ev1.From, ev1.PeerID)
		//fmt.Println(reflect.TypeOf(ev1.Event).Name())
		node.serverMailBox.Outbox() <- &cluster.Envelope{Pid: int(ev1.PeerID), Msg: ev1.Event}

	}
}

func (node *RaftNode)listening_events(){
	node.mutex1.Lock()

	for {

		select {
		case <-node.shutdown:
		node.mutex1.Unlock()
			return
		/*case appendMsg := <- node.sm.clientCh : // RaftNode gets an Append([]byte) message
			fmt.Println("Here 2")
			node.sm.clientCh <- appendMsg
		*/
		case envMsg := <-node.serverMailBox.Inbox():
		// RaftNode gets a message from other nodes in the cluster

			b := envMsg.Msg.(interface{})
		//fmt.Println("Inbox " + string(envMsg.Pid)+ " "+ reflect.TypeOf(b).Name())
			node.sm.netCh <- b
		case <-node.timer.C:
		// Timeout for internal server
			//fmt.Println("Timer Timer")
			node.sm.netCh <- Timeout{}
		}

	}
	node.mutex1.Unlock()


}

func (node *RaftNode)doing_actions(){
	node.mutex2.Lock()

	var e ActionsCompleted
	node.sm.actionCh <- e
	//var actions []interface{}


	// Collect all actions from the internal server
	for {
		select {
		case t := <- node.sm.actionCh:

			go node.process(t)
		case  <- node.shutdown:
			node.mutex2.Unlock()

			return

		}


		//actions = append(actions, t)
	}

	node.mutex2.Unlock()


}

func (node *RaftNode) processEvents() {
	go node.sm.eventloop()
	genRand(ELECTION_TIMEOUT, ELECTION_TIMEOUT*2)
	var rnd = genRand(0, ELECTION_TIMEOUT*2)
	//fmt.Println(strconv.Itoa(rnd) + " " + strconv.Itoa(node.sm.serverID))
	node.timer = time.NewTimer(time.Duration(rnd)* time.Millisecond )
	go node.listening_events()
	go node.doing_actions()
}

// Returns leader of cluster if present, nil otherwise
func getLeader(r []RaftNode) *RaftNode {
	for _, node := range r {
		if(node.sm.leaderID != -1) {
			return &node
		}
	}
	return nil
}
//********************************************************************************************

func registerStructs() {
	gob.Register(LogEntry{})
	gob.Register(AppendEntriesReq{})
	gob.Register(AppendEntriesResp{})
	gob.Register(VoteReq{})
	gob.Register(VoteResp{})
	gob.Register(Send{})
}

var configs cluster.Config = cluster.Config{
	Peers: []cluster.PeerConfig{
		{Id: 1, Address: "localhost:8001"},
		{Id: 2, Address: "localhost:8002"},
		{Id: 3, Address: "localhost:8003"},
		{Id: 4, Address: "localhost:8004"},
		{Id: 0, Address: "localhost:8000"}}}

//********************************************************************************************
// Generates a mock cluster of 5 raft nodes
func makeMockRafts() ([]RaftNode, *mock.MockCluster){


	// init the communication layer.
	// Note, mock cluster doesn't need IP addresses. Only Ids
	clconfig := cluster.Config{Peers:[]cluster.PeerConfig{
		{Id:0}, {Id:1}, {Id:2}, {Id:3}, {Id:4}}}
	cluster, err := mock.NewCluster(clconfig)
	if err != nil {return nil, cluster}

	// init the raft node layer
	nodes := make([]RaftNode, len(clconfig.Peers))

	// Create a raft node, and give the corresponding "Server" object from the
	// cluster to help it communicate with the others.
	for id := 0; id < 5; id++ {
		mockconfig := Config{clconfig, id, "$GOPATH/src/github.com/aditya-nambiar/cs733/assignment3/", 500, 50}
		raftNode := NewRaftNode(mockconfig, id) //

		// Give each raftNode its own "Server" from the cluster.
		raftNode.serverMailBox = cluster.Servers[id]
		nodes[id] = raftNode
	}

	return nodes, cluster
}

func makeRafts() []RaftNode {
	var r []RaftNode
	for i := 0; i < len(configs.Peers); i++ {
		config := Config{configs, i, "$GOPATH/src/github.com/aditya-nambiar/cs733/assignment3/", 500, 50}
		r = append(r, NewRaftNode(config, i ))
		r[i].serverMailBox, _ = cluster.New(config.Id, config.cluster)

	}
	registerStructs()
	return r
}
/**************************************************************/

func main(){

	rafts := makeRafts() // array of []raft.Node
	for i:=0; i<5; i++ {
		//defer rafts[i].lg.Close()
		go rafts[i].processEvents()
	}
	time.Sleep(1*time.Second)
	ldr := getLeader(rafts)
	ldr.Append([]byte("foo"))
	fmt.Println(ldr.sm.serverID);
	time.Sleep(3*time.Second)
	for _, node := range rafts {
		select {
		case ci := <- node.CommitChannel():
			//if ci.Err != nil {fmt.Println(ci.Err)}
			if string(ci.Data.Data) != "foo" {
				fmt.Println("Got different data")
			} else{
				fmt.Println("Proper Commit")
			}
		//default: fmt.Println("Expected message on all nodes")
		}
	}

}

