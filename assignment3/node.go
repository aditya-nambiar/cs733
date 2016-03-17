package main

import "github.com/cs733-iitb/cluster"
import "time"
import "github.com/cs733-iitb/log"
import "strconv"
import "reflect"
import "github.com/syndtr/goleveldb/leveldb"
import (
	"sync"
	"encoding/gob"
"math/rand"
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
	mutex	 *sync.RWMutex // Ensure integrity of state machine fields
	logMutex *sync.RWMutex	// Ensure correctness of log
	electionTimeout time.Duration
}

func NewRaftNode(config Config) RaftNode {
	var node RaftNode
	node.cluster = config.cluster
	node.serverMailBox, _ = cluster.New(config.Id, node.cluster)
	node.LogDir = config.LogDir
	node.raft_log, _ = log.Open(node.LogDir + "/Log" + strconv.Itoa(config.Id))
	node.commitCh = make(chan CommitInfo, 100)
	node.sm = newServer(3) // change to ID
	return node
}



// Client's message to Raft node
func (node *RaftNode) Append(data []byte) {
	node.sm.clientCh <- data
}

// A channel for client to listen on. What goes into Append must come out of here at some point.
func (node *RaftNode) CommitChannel() <-chan CommitInfo {
	return node.commitCh
}

// Last known committed index in the log. This could be -1 until the system stabilizes.

func (node *RaftNode) CommittedIndex() int {
	node.mutex.RLock()
	c := node.sm.commitIndex
	node.mutex.RUnlock()
	return c
}

// Returns the data at a log index, or an error.

func (node *RaftNode) Get(index int64) ( []byte, error) {
	c, err := node.raft_log.Get(index)
	node.logMutex.RUnlock()
	return c, err
}

func (node *RaftNode) Id() {

}

// Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.

func (node *RaftNode) Shutdown() {

}

//********************************************************************************************



// Process output events such as Send, Alarm etc.
func (rn *RaftNode) process(ev  interface{}) {
	t := reflect.TypeOf(ev)
	if (t.Name() == "Alarm") {
		rn.timer.Reset(time.Duration(ev.(Alarm).time) * time.Millisecond)
	} else if (t.Name() == "LogStore") {
		obj := ev.(LogStore)
		//fmt.Println("Append ", rn.lg.GetLastIndex(), ev.Index, string(ev.Data))
		rn.logMutex.Lock()
		rn.raft_log.TruncateToEnd(int64(obj.index))
		rn.raft_log.Append(obj.data)
		rn.logMutex.Unlock()
	} else if (t.Name() == "Commit") {
		obj := ev.(Commit)
		out := CommitInfo{obj.data, int64(obj.index), obj.err}
		rn.commitCh <- out
	} else if (t.Name() == "Send") {
		//fmt.Println("Send ", ev.to, ev.eventName())

		//fmt.Println(string(b))
		// go func(){
		func(){
			rn.serverMailBox.Outbox() <- &cluster.Envelope{Pid: int(ev.(Send).peerID), Msg: ev}
		}()
	}
}

func (node *RaftNode) processEvents() {
	go node.sm.eventloop()
	//fmt.Println("Started ", rn.sm.id)
	node.timer = time.NewTimer(node.electionTimeout + time.Duration(rand.Intn(1000)))
	for {

		var actions []interface{}

		select {
		case <- node.shutdown :
			node.mutex.Unlock()
			return
		case appendMsg := <- node.sm.clientCh : // RaftNode gets an Append([]byte) message
			node.sm.clientCh <- appendMsg

		case envMsg := <- node.serverMailBox.Inbox() : // RaftNode gets a message from other nodes in the cluster
			b := envMsg.Msg.(interface{})
			node.sm.netCh <- b
		case <- node.timer.C :  // Timeout for internal server
			node.sm.netCh <- Timeout{}
		}

		// Collect all actions from the internal server
		for {
			t := <- node.sm.actionCh
			nm := reflect.TypeOf(t)
			if(nm.Name() == "Event Processed") {
				break
			}
			actions = append(actions, t)
		}

		// Process the actions obtained
		for i:=0; i< len(actions); i++ {
			node.process(actions[i])
		}
	}
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


var configs cluster.Config = cluster.Config{
	Peers: []cluster.PeerConfig{
		{Id: 0, Address: "localhost:8001"},
		{Id: 1, Address: "localhost:8002"},
		{Id: 2, Address: "localhost:8003"},
		{Id: 3, Address: "localhost:8004"},
		{Id: 4, Address: "localhost:8005"}}}

//********************************************************************************************

func makeRafts() []RaftNode {
	var r []RaftNode
	for i := 0; i < len(configs.Peers); i++ {
		config := Config{configs, i, "$GOPATH/src/github.com/aditya-nambiar/cs733/assignment3/", 500, 50}
		r = append(r, NewRaftNode(config))
	}
	return r
}
/**************************************************************/

func main(){

	gob.Register(VoteResp{}) // register a struct name by giving it a dummy object of that name.
	gob.Register(AppendEntriesReq{}) // register a struct name by giving it a dummy object of that name.
	gob.Register(AppendEntriesResp{}) // register a struct name by giving it a dummy object of that name.
	gob.Register(VoteReq{}) // register a struct name by giving it a dummy object of that name.
	//gob.Register(VoteResp{}) // register a struct name by giving it a dummy object of that name.
	//gob.Register(VoteResp{}) // register a struct name by giving it a dummy object of that name.
	rafts := makeRafts() // array of []raft.Node
	for i:=0; i<5; i++ {
		//defer rafts[i].raft_log.Close()
		go rafts[i].processEvents()
	}
	time.Sleep(1*time.Second)
	ldr := getLeader(rafts)
	ldr.Append([]byte("foo"))
	time.Sleep(3*time.Second)
	//for _, node := range rafts {
	//	select {
	//	case ci := <- node.CommitChannel():
	//		if ci.Err != nil {fmt.Println(ci.Err)}
	//		if string(ci.Data) != "foo" {
	//			fmt.Println("Got different data")
	//		} else{
	//			fmt.Println("Proper Commit")
	//		}
	//	//default: fmt.Println("Expected message on all nodes")
	//	}
	//}

}

