package main


import (
	"bytes"
	"fmt"
	//"strconv"
	//"sync"
	"testing"
	"time"
	"strconv"
	//"strings"
)


func Redirect(t *testing.T, m *Msg, cl *Client) (c *Client) {
	redirectAddress := string(m.Contents)
	cl.close()
	cl = mkClient(redirectAddress)
	return cl
}


func TestBasic(t *testing.T) {

	StartAllServerProcess()


	var leader_ip string
	//time.Sleep(199 * time.Second)
	// make client connection to all the servers
	nclients := 5
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
		str := "random text"
		filename := fmt.Sprintf("FILE%v", i)
		cl := clients[i]
		m, err := cl.write(filename, str, 0)
		//fmt.Println("Write Returned ")
		for err == nil && m.Kind == 'R' {
			fmt.Println("Redirecting")

			leader_ip = string(m.Contents)
			cl = Redirect(t, m, cl)
			clients[i] = cl
			m, err = cl.write(filename, str, 0)
		}
		expect(t, m, &Msg{Kind: 'O'}, "Test_Basic: Write success", err, 63)
	}
	//fmt.Println("Starting to READ ###############!@@@@@@#########%%%%%%%%%%")
	// Sleep for some time and let the entries be replicated at followers
	time.Sleep(7 * time.Second)
	//make a client connection to follower and try to read

	for i:=0; i< nclients; i++{
		if( leader_ip != addr[i]){
			cl := mkClient(addr[i])
			for i := 0; i < nclients; i++ {
				filename := fmt.Sprintf("FILE%v", i)
				m, err := cl.read(filename)
				expect(t, m, &Msg{Kind: 'C', Contents: []byte("random text")}, "TestBasic: Read fille", err, 76)
			}
			break
		}
	}

	KillAllServerProcess()

	fmt.Println("TestBasic : Pass")
}


func TestRPC_BasicSequential(t *testing.T) {
		StartAllServerProcess()

	cl := mkClient("localhost:9201")

	// Read non-existent file cs733net
	m, err := cl.read("cs733net")
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err, 95)

	// Read non-existent file cs733net
	m, err = cl.delete("cs733net")
	for err == nil && m.Kind == 'R' {
		cl = Redirect(t, m, cl)
		m, err = cl.delete("cs733net")
	}
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err, 103)

	// Write file cs733net
	data := "Cloud fun"
	m, err = cl.write("cs733net", data, 0)
	for err == nil && m.Kind == 'R' {
		cl = Redirect(t, m, cl)
		m, err = cl.delete("cs733net")

	}
	expect(t, m, &Msg{Kind: 'O'}, "write success", err, 113)

	// Expect to read it back
	m, err = cl.read("cs733net")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(data)}, "read my write", err, 117)

	// CAS in new value
	version1 := m.Version
	data2 := "Cloud fun 2"
	// Cas new value
	m, err = cl.cas("cs733net", version1, data2, 0)
	for err == nil && m.Kind == 'R' {
		cl = Redirect(t, m, cl)
		m, err = cl.cas("cs733net", version1, data2, 0)
	}
	expect(t, m, &Msg{Kind: 'O'}, "cas success", err, 128)

	// Expect to read it back
	m, err = cl.read("cs733net")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(data2)}, "read my cas", err, 132)

	// Expect Cas to fail with old version
	m, err = cl.cas("cs733net", version1, data, 0)
	for err == nil && m.Kind == 'R' {
		cl = Redirect(t, m, cl)
		m, err = cl.cas("cs733net", version1, data, 0)
	}
	expect(t, m, &Msg{Kind: 'V'}, "cas version mismatch", err, 140)

	// Expect a failed cas to not have succeeded. Read should return data2.
	m, err = cl.read("cs733net")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(data2)}, "failed cas to not have succeeded", err, 144)

	// delete
	m, err = cl.delete("cs733net")
	for err == nil && m.Kind == 'R' {
		cl = Redirect(t, m, cl)
		m, err = cl.delete("cs733net")
	}
	expect(t, m, &Msg{Kind: 'O'}, "delete success", err, 152)

	// Expect to not find the file
	m, err = cl.read("cs733net")
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err, 156)

	cl.close()

		KillAllServerProcess()

	fmt.Println("TestRPC_BasicSequential : Pass")
}

func TestRPC_Binary(t *testing.T) {
		StartAllServerProcess()
	cl := mkClient( "localhost:9203")
	defer cl.close()
	// Write binary contents
	data := "\x00\x01\r\n\x03" // some non-ascii, some crlf chars
	m, err := cl.write("binfile", data, 0)

	for err == nil && m.Kind == 'R' {
		cl = Redirect(t, m, cl)
		m, err = cl.write("binfile", data, 0)
	}
	expect(t, m, &Msg{Kind: 'O'}, "write success", err, 178)

	// Expect to read it back
	m, err = cl.read("binfile")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(data)}, "read my write", err, 182)

		KillAllServerProcess()
	fmt.Println("TestRPC_Binary : Pass")
}

func TestRPC_BasicTimer(t *testing.T) {
		StartAllServerProcess()
	cl := mkClient("localhost:9203")
	defer cl.close()

	// Write file cs733, with expiry time of 2 seconds
	str := "Cloud fun"
	m, err := cl.write("cs733", str, 2)
	for err == nil && m.Kind == 'R' {
		cl = Redirect(t, m, cl)
		m, err = cl.write("cs733", str, 2)
	}
	expect(t, m, &Msg{Kind: 'O'}, "write success", err, 200)

	// Expect to read it back immediately.
	m, err = cl.read("cs733")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(str)}, "read my cas", err, 204)

	time.Sleep(3 * time.Second)
	// Expect to not find the file after expiry
	m, err = cl.read("cs733")
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err, 209)

	// Recreate the file with expiry time of 1 second
	m, err = cl.write("cs733", str, 1)
	for err == nil && m.Kind == 'R' {
		cl = Redirect(t, m, cl)
		m, err = cl.write("cs733", str, 1)
	}
	expect(t, m, &Msg{Kind: 'O'}, "file recreated", err, 217)

	// Overwrite the file with expiry time of 4. This should be the new time.
	m, err = cl.write("cs733", str, 4)
	for err == nil && m.Kind == 'R' {
		cl = Redirect(t, m, cl)
		m, err = cl.write("cs733", str, 4)
	}
	expect(t, m, &Msg{Kind: 'O'}, "file overwriten with exptime=4", err, 225)

	// The last expiry time was 3 seconds. We should expect the file to still be around 2 seconds later
	time.Sleep(2 * time.Second)
	// Expect the file to not have expired.
	m, err = cl.read("cs733")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(str)}, "file to not expire until 4 sec", err, 231)

	time.Sleep(3 * time.Second)
	// 5 seconds since the last write. Expect the file to have expired
	m, err = cl.read("cs733")
	expect(t, m, &Msg{Kind: 'F'}, "file not found after 4 sec", err, 236)

	// Create the file with an expiry time of 10 sec. We're going to delete it
	// then immediately create it. The new file better not get deleted.
	m, err = cl.write("cs733", str, 10)
	for err == nil && m.Kind == 'R' {
		cl = Redirect(t, m, cl)
		m, err = cl.write("cs733", str, 10)
	}
	expect(t, m, &Msg{Kind: 'O'}, "file created for delete", err, 245)

	m, err = cl.delete("cs733")
	for err == nil && m.Kind == 'R' {
		cl = Redirect(t, m, cl)
		m, err = cl.delete("cs733")
	}
	expect(t, m, &Msg{Kind: 'O'}, "deleted ok", err, 252)

	m, err = cl.write("cs733", str, 0) // No expiry
	for err == nil && m.Kind == 'R' {
		cl = Redirect(t, m, cl)
		m, err = cl.write("cs733", str, 0)
	}
	expect(t, m, &Msg{Kind: 'O'}, "file recreated", err, 259)

	time.Sleep(1100 * time.Millisecond) // A little more than 1 sec
	m, err = cl.read("cs733")
	expect(t, m, &Msg{Kind: 'C'}, "file should not be deleted", err, 263)

		KillAllServerProcess()
	fmt.Println("TestRPC_BasicTimer : Pass")
}





func Test_LeaderFailure(t *testing.T) {
		StartAllServerProcess()
	nclients := 5
	var leader_ip string

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
		str := "random text"
		filename := fmt.Sprintf("FILE%v", i)
		cl := clients[i]
		m, err := cl.write(filename, str, 0)
		for err == nil && m.Kind == 'R' {
			fmt.Println("Redirecting")

			leader_ip = string(m.Contents)
			cl = Redirect(t, m, cl)
			clients[i] = cl
			m, err = cl.write(filename, str, 0)
		}
		expect(t, m, &Msg{Kind: 'O'}, "Test_Basic: Write success", err, 63)
	}
	time.Sleep(3* time.Second) // give goroutines a chance



	//As soon as all the responses are received, kill a follower Node.
	// Note : Some of the appends may not have been received at this follower
	var leader int = 0
	var foll int = 0
	for i:=1; i<= nclients; i++{
		if( leader_ip == addr[i-1]){
			leader = i
			break
		}
	}

	foll = (leader + 1)%5

	fmt.Println("Killing " + strconv.Itoa(leader) + "@ ADDRESS "+ addr[leader-1] )
	KillServer(leader)

	time.Sleep(3 * time.Second)

	// Make a client connect to the leader and create 2 new files and delete the previous file
	cl := mkClient( addr[foll])

	m, err := cl.write("cloudSuspense", "zoo", 0)
	for err == nil && m.Kind == 'R' {
		cl = Redirect(t, m, cl)
		m, err = cl.write("cloudSuspense", "zoo", 0)
	}

	data := "Cloud fun"
	m, err = cl.write("cs733net", data, 0)
	for err == nil && m.Kind == 'R' {
		cl = Redirect(t, m, cl)
		m, err = cl.write("cs733net", data, 0)

	}

	m, err = cl.delete("concWrite")
	for err == nil && m.Kind == 'R' {
		cl = Redirect(t, m, cl)
		m, err = cl.delete("concWrite")
	}

	var anoather_ip = "localhost:9203"
	if( anoather_ip == addr[leader-1]){
		anoather_ip = "localhost:9202"
	}

	//follower := "localhost:920" + strconv.Itoa(foll)
	// Make a client connection to the follower and start reading
	cl = mkClient(anoather_ip)

	m, err = cl.read("cloudSuspense")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte("zoo")}, "read my write", err, 432)

	m, err = cl.read("cs733net")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte("Cloud fun")}, "read cloud fun", err, 435)

	m, err = cl.read("concWrite")
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err, 438)

	KillAllServerProcess()
	fmt.Println("Test_FollowerFailure : Pass")

}




func Test_MajorityNodeFailure(t *testing.T) {

	StartAllServerProcess()

	var leader string = "localhost:9201"

	// All nodes are working system should work
	cl := mkClient( "localhost:9201")

	m, err := cl.write("cloudSuspense", "zoo", 0)
	for err == nil && m.Kind == 'R' {
		leader = string(m.Contents)
		cl = Redirect(t, m, cl)
		m, err = cl.write("cloudSuspense", "zoo", 0)
	}
	expect(t, m, &Msg{Kind: 'O'}, "Test_LeaderMultipleFailure: Write CloudSuspense success", err, 463)
	time.Sleep(1 * time.Second)

	// Kill 3 followers .
	leaderId := leader[len(leader)-1:]
	l, _ := strconv.Atoi(leaderId)

	for i := 1; i <= 3; i++ {
		KillServer((l+i)%5 + 1)
	}
	go func() {
		data := "Cloud fun"
		m, err = cl.write("cs733net", data, 0)
		for err == nil && m.Kind == 'R' {
			cl = Redirect(t, m, cl)
			m, err = cl.write("cs733net", data, 0)

		}
	}()

	time.Sleep(5 * time.Second) // client is stuck for this period
	cl1 := mkClient(leader)
	m, err = cl1.read("cs733net")
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err, 492)


	KillAllServerProcess()
	fmt.Println("Test_MajorityNodeFailure : Pass")
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

