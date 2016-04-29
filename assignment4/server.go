package main

import (
	"bufio"
	"fmt"
	"github.com/aditya-nambiar/cs733/assignment4/fs"
	"net"
	"os"
	"os/exec"
	"strconv"
	"time"
	"sync"
	"bytes"
	"encoding/gob"
	"errors"

	"github.com/cs733-iitb/cluster"
)

type Server struct {
	sync.RWMutex
	fsconf        []FileSystemConfig
	ClientChanelMap map[int64]chan ClientResp
	rn            RaftNode
	fileMap       *fs.FS
	gversion      int
}

type FileSystemConfig struct {
	Id      int
	Addr string
	//	Port int
}

type ClientResp struct {
	Message fs.Msg
	Err     error
}

type MsgEntry struct {
	Data fs.Msg
}

var crlf = []byte{'\r', '\n'}
var MAX_CLIENTS int64 = 10000000
var num_nodes = 5


var fileServer []*exec.Cmd

func StartServer(i int, restartflag string) {
	fileServer[i-1] = exec.Command("./server", strconv.Itoa(i-1), restartflag)
	fileServer[i-1].Stdout = os.Stdout
	fileServer[i-1].Stdin = os.Stdin
	fileServer[i-1].Start()
}

func StartAllServerProcess() {
	fileServer = make([]*exec.Cmd, num_nodes)
	registerStructs()
	for i := 1; i <= len(configs_fs.Peers); i++ {
		//os.RemoveAll("myLogDir" + strconv.Itoa(i) + "/logfile")
		//os.RemoveAll("myStateDir" + strconv.Itoa(i) + "/mystate")
		StartServer(i, "false")
	}
	time.Sleep(10 * time.Second)


}

func KillServer(i int) {
	fileServer[i-1].Process.Kill()
}

func KillAllServerProcess() {
	for i := 1; i <= len(configs_fs.Peers); i++ {
		KillServer(i)
		//os.RemoveAll("myLogDir" + strconv.Itoa(i) + "/logfile")
		//os.RemoveAll("myStateDir" + strconv.Itoa(i) + "/mystate")
	}
	time.Sleep(2 * time.Second)
}


func reply(conn *net.TCPConn, msg *fs.Msg) bool {
	var err error
	//write := func(data []byte) {
	//	if err != nil {
	//		return
	//	}
	//	_, err = conn.Write(data)
	//}
	var resp string
	switch msg.Kind {
	case 'C': // read response
		resp = fmt.Sprintf("CONTENTS %d %d %d", msg.Version, msg.Numbytes, msg.Exptime)
	case 'O':
		resp = "OK "
		if msg.Version > 0 {
			resp += strconv.Itoa(msg.Version)
		}
	case 'F':
		resp = "ERR_FILE_NOT_FOUND"
	case 'V':
		resp = "ERR_VERSION " + strconv.Itoa(msg.Version)
	case 'M':
		resp = "ERR_CMD_ERR"
	case 'I':
		resp = "ERR_INTERNAL"
	case 'R':
		resp = "ERR_REDIRECT " + string(msg.Contents)
	default:
		fmt.Printf("Unknown response kind '%c'", msg.Kind)
		return false
	}
	resp += "\r\n"
	fmt.Println("Reply " + resp)
	conn.Write([]byte(resp))
	if msg.Kind == 'C' {
		conn.Write(msg.Contents)
		conn.Write(crlf)
	}
	return err == nil
}


func (server *Server) getAddress(id int) string {
	// find address of this server
	var address string
	for i := 0; i < len(server.fsconf); i++ {
		if id == server.fsconf[i].Id {
			address = server.fsconf[i].Addr
			break
		}
	}
	return address
}

func MsgtoBytes(data fs.Msg) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	me := MsgEntry{Data: data}
	err := enc.Encode(me)
	return buf.Bytes(), err
}

func BytesToMsg(dbytes []byte) (fs.Msg, error) {
	buf := bytes.NewBuffer(dbytes)
	enc := gob.NewDecoder(buf)
	var me MsgEntry
	err := enc.Decode(&me)
	return me.Data, err
}

func check(obj interface{}) {
	if obj != nil {
		fmt.Println(obj)
		os.Exit(1)
	}
}

func (server *Server) serve(clientid int, clientCommitCh chan ClientResp, conn *net.TCPConn) {

	reader := bufio.NewReader(conn)
	var res ClientResp
	var response *fs.Msg
	for {

		msg, msgerr, fatalerr := fs.GetMsg(reader)
		msg.ClientID = clientid
		if fatalerr != nil {
			reply(conn, &fs.Msg{Kind: 'M'})

			conn.Close()
			break
		}

		if msgerr != nil {
			if (!reply(conn, &fs.Msg{Kind: 'M'})) {
				conn.Close()
				break
			}
			continue
		}
		fmt.Println("Here")
		// if message not of read type
		if msg.Kind == 'w' || msg.Kind == 'c' || msg.Kind == 'd' {
			dbytes, err := MsgtoBytes(*msg)
			if err != nil {
				if (!reply(conn, &fs.Msg{Kind: 'I'})) {
					conn.Close()
					break
				}
				continue
			}
			// append the msg to the raft node log
			server.rn.Append(dbytes)

			//wait for the msg to appear on the client commit channel
			 res = <-clientCommitCh

			fmt.Println("HERE 2")
			if res.Err != nil {
				//msgContent := server.getAddress(res.Message.RedirectAddress)
				fmt.Println("$$%% "  +res.Message.RedirectAddress)

				reply(conn, &fs.Msg{Kind: 'R', Contents: []byte(res.Message.RedirectAddress)})
				conn.Close()
				break
			}
			response = &res.Message
			//		fmt.Printf("Response Message %v\n", string(response.Contents))

		} else if msg.Kind == 'r' {
			response = fs.ProcessMsg(server.fileMap, &(server.gversion), msg)
		}

		if !reply(conn, response) {
			fmt.Println("SDFgdgdsd")

			conn.Close()
			break
		}
	}
}

func (server *Server) ListenCommitChannel() {

	var prevLogIndexProcessed = 0
	for {
		//listen on commit channel of raft node
		fmt.Println("44444444###################")
		var commitval CommitInfo
		//commitval := <-server.rn.CommitChannel()
		for {
			tp := false
			select {
			case commitval = <-server.rn.CommitChannel():
				fmt.Println("##########################################")
				fmt.Println((int(commitval.Index)))
				fmt.Println(string(commitval.Err))
				tp = true

			case <- time.After(5 * time.Second):
				fmt.Println("Damn")
			}

			if(tp){
				break
			}
		}
		fmt.Println(string(commitval.Index))
		fmt.Println(string(commitval.Err))
		fmt.Println("####%%%%%%%%%%######################################")
		if int(commitval.Index) == -2 {
			//Redirect the client. Assume the server for which this server voted is the leader. So, redirect it there.
			dmsg, _ := BytesToMsg(commitval.Data.Data)
			server.Lock()
			redirct_Addr, _ := strconv.Atoi(commitval.Err)
			dmsg.RedirectAddress = server.getAddress(redirct_Addr)
			fmt.Println("$$ "  +dmsg.RedirectAddress)
			server.ClientChanelMap[int64(dmsg.ClientID)] <- ClientResp{dmsg, errors.New("ERR_REDIRECT")}

			server.Unlock()

		} else {
			//check if there are missing or duplicate commits
			if commitval.Index <= int64(prevLogIndexProcessed) {
				// already processed. So continue
				continue
			}
			// if missing get them
			fmt.Println("I am here !!!! with index => " + strconv.Itoa(int(commitval.Index)) +" @ ID " + strconv.Itoa(server.rn.sm.serverID))
			dmsg, _ := BytesToMsg(commitval.Data.Data)
			fmt.Println(string(dmsg.Contents))
			fmt.Println(string(dmsg.Filename))


			for i := prevLogIndexProcessed + 1; int64(i) < commitval.Index; i++ {
				 emsg, _ := server.rn.Get(int64(i))

				dmsg, _ := BytesToMsg(emsg)
				fmt.Println("I am here 123")
				fmt.Println(string(dmsg.Contents))
				fmt.Println(string(dmsg.Filename))


				response := fs.ProcessMsg(server.fileMap, &(server.gversion), &dmsg)
				server.Lock()
				server.ClientChanelMap[int64(dmsg.ClientID)] <- ClientResp{*response, nil}
				server.Unlock()
			}


			dmsg, err := BytesToMsg(commitval.Data.Data)
			if err != nil {
				fmt.Printf("ListenCommitChannel: Error in decoding message 3")
			}
			// process the message and send response to client
			response := fs.ProcessMsg(server.fileMap, &(server.gversion), &dmsg)
			//			fmt.Printf("Response: %v", *response)
			//server.ClientChanMap[dmsg.ClientId]<-ClientEntry{dmsg,nil}
			server.Lock()
			server.ClientChanelMap[int64(dmsg.ClientID)] <- ClientResp{*response, nil}
			server.Unlock()
			prevLogIndexProcessed = int(commitval.Index)
			fmt.Println("I am here 1212343")

		}

	}

}


func createRaftNode(i int) RaftNode{
	config := Config{configs, i, "$GOPATH/src/github.com/aditya-nambiar/cs733/assignment4/", 500, 50}
	var r = NewRaftNode(config, i)
	r.serverMailBox, _ = cluster.New(config.Id, config.cluster)
	return r
}


func serverMain(id int, restartFlag string) {
	var server Server
	gob.Register(MsgEntry{})
	var clientid int64 = int64(id)

	// make map for mapping client id with corresponding receiving clientcommitchannel
	server.ClientChanelMap = make(map[int64]chan ClientResp)


	// fsconf stores array of the id and addresses of all file servers
	server.fsconf = make([]FileSystemConfig, len(configs_fs.Peers))

	for j := 0; j < len(configs_fs.Peers); j++ {
		server.fsconf[j].Id = configs_fs.Peers[j].Id
		server.fsconf[j].Addr = configs_fs.Peers[j].fsAddr
	}

	// make a map for storing file info
	server.fileMap = &fs.FS{Dir: make(map[string]*fs.FileInfo, 10000)}

	server.gversion = 0
	// find address of this server
	address := server.getAddress(id)
	fmt.Println("Address to connect :" +  address)

	// start the file server
	tcpaddr, err := net.ResolveTCPAddr("tcp", address)
	check(err)

	tcp_acceptor, err := net.ListenTCP("tcp", tcpaddr)
	check(err)
	/*
	if restartFlag == "true" {
		server.rn = RestartNode(id, raftconf)
	} else {
		server.rn = BringNodeUp(id, raftconf)
	}
	*/
	//	fmt.Println("After")

	// start listening on raft commit channel
	go server.ListenCommitChannel()

	// start raft server to process events
	server.rn = createRaftNode(id)
	go server.rn.processEvents()

	// start accepting connection from clients
	for {

		tcp_conn, err := tcp_acceptor.AcceptTCP()
		check(err)

		fmt.Println("Accepted Connection")
		// assign id and commit chan to client
		clientid = (clientid + int64(11)) % MAX_CLIENTS
		clientCommitCh := make(chan ClientResp)
		server.Lock()
		server.ClientChanelMap[clientid] = clientCommitCh
		server.Unlock()

		// go and serve the client connection
		go server.serve(int(clientid), clientCommitCh, tcp_conn)
	}

}


func main() {
	id, _ := strconv.Atoi(os.Args[1])
	//fmt.Println("Created exec " + os.Args[1])
	serverMain(id, os.Args[2]) // Id & resetFlag
}
