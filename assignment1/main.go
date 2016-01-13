package main

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"net"
	"os"
	s "strings"
)

const (
	HOST = "localhost"
	TYPE = "tcp"
	PORT = "8080"
)

func main() {
	serverMain()
}

func serverMain() {
	// Listen for incoming connections.
	l, err := net.Listen(TYPE, HOST+":"+PORT)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// Close the listener when the application closes.
	defer l.Close()
	db, err := leveldb.OpenFile("/Users/aditya/Desktop", nil)

	defer db.Close()

	fmt.Println("Listening on " + HOST + ":" + PORT)
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		go handleRequest(conn)
	}
}

// Handles incoming requests.
func handleRequest(conn net.Conn) {

	for {
		// Make a buffer to hold incoming data.
		buf := make([]byte, 1024)

		// Read the incoming connection into the buffer.
		_, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading:", err.Error())
		}
		rec_str := string(buf[:])

		fmt.Println(rec_str)
		if s.Contains(rec_str, "close") {
			break
		}

		// Send a response back to person contacting us.
		conn.Write([]byte("Received."))
		conn.Write(buf)
	}

	conn.Close()
	// Close the connection when you're done with it.
}
