package main


import (
	"bytes"
	"fmt"
	//"strconv"
	"testing"
	"time"
	"strconv"
)


func Redirect(t *testing.T, m *Msg, cl *Client) (c *Client) {
	redirectAddress := string(m.Contents)
	cl.close()
	cl = mkClient(redirectAddress)
	return cl
}
// ############################################## Basic Test #####################################################################

//This testcase test that all servers should start functioning properly. Clients try to connect to each of the servers for update
//operations. But ultimately they should be redirected to the leader. After all the updates, a client try to read the updated files
// from one of the follower. This should be successful. Finally, all the servers are killed.

func TestBasic(t *testing.T) {

	StartAllServerProcess()


	var leader_ip string
	//time.Sleep(199 * time.Second)
	// make client connection to all the servers
	nclients := 2
	clients := make([]*Client, nclients)
	addr := [5]string{"localhost:9201", "localhost:9202", "localhost:9203", "localhost:9204", "localhost:9205"}

	for i := 0; i < nclients; i++ {

		cl := mkClient(addr[i%5])
		if cl == nil {
		t.Fatalf("Unable to create client #%d", i)
		}
		defer cl.close()
		clients[i] = cl
	}

	// Try a write command at each server. All the clients except the one connected to leader should be redirected
	for i := 0; i < nclients; i++ {
		str := "Distributed System is a zoo"
		filename := fmt.Sprintf("DSsystem%v", i)
		cl := clients[i]
		m, err := cl.write(filename, str, 0)
		fmt.Println("Write Returned ")
		for err == nil && m.Kind == 'R' {
			fmt.Println("Redirecting")

			leader_ip = string(m.Contents)
			cl = Redirect(t, m, cl)
			clients[i] = cl
			m, err = cl.write(filename, str, 0)
		}
		expect(t, m, &Msg{Kind: 'O'}, "Test_Basic: Write success", err, 63)
	}

	// Sleep for some time and let the entries be replicated at followers
	time.Sleep(3 * time.Second)
	//make a client connection to follower and try to read

	for i:=0; i< len(addr); i++{
		if( leader_ip != addr[i]){
			cl := mkClient(addr[i])
			for i := 0; i < nclients; i++ {
				filename := fmt.Sprintf("DSsystem%v", i)
				m, err := cl.read(filename)
				expect(t, m, &Msg{Kind: 'C', Contents: []byte("Distributed System is a zoo")}, "TestBasic: Read fille", err, 76)
			}
			break
		}
	}


	//cl := mkClient("localhost:920"+strconv.Itoa(foll))
	//for i := 0; i < nclients; i++ {
	//filename := fmt.Sprintf("DSsystem%v", i)
	//m, err := cl.read(filename)
	//expect(t, m, &Msg{Kind: 'C', Contents: []byte("Distributed System is a zoo")}, "TestBasic: Read fille", err)
	//}

	KillAllServerProcess()

	fmt.Println("TestBasic : Pass")
}


func expect(t *testing.T, response *Msg, expected *Msg, errstr string, err error, line_num int) {
	if err != nil {
		KillAllServerProcess()
		t.Fatal("Unexpected error: " + err.Error() + " @line: " + strconv.Itoa(line_num))
	}
	ok := true
	if response.Kind != expected.Kind {
		ok = false
		errstr += fmt.Sprintf(" Got kind='%c', expected '%c'", response.Kind, expected.Kind)
	}
	if expected.Version > 0 && expected.Version != response.Version {
		ok = false
		errstr += " Version mismatch"
	}
	if response.Kind == 'C' {
		if expected.Contents != nil &&
		bytes.Compare(response.Contents, expected.Contents) != 0 {
			ok = false
		}
	}
	if !ok {
		//KillAllServerProcess()
		t.Fatal("Expected " + errstr + " @line: " + strconv.Itoa(line_num))
	}
}

