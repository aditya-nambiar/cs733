package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
)

const PORT = 8080

func hello() string {
	return "hi"
}
func main() {
	serverMain()
}

func serverMain() {
	server, err := net.Listen("tcp", ":"+strconv.Itoa(PORT))
	if server == nil {
		panic("couldn't start listening: " + err.Error())
	}
	conns := clientConns(server)
	for {
		go handleConn(<-conns)
	}
}

func clientConns(listener net.Listener) chan net.Conn {
	ch := make(chan net.Conn)
	i := 0
	go func() {
		for {
			client, err := listener.Accept()
			if client == nil {
				fmt.Printf("couldn't accept: " + err.Error())
				continue
			}
			i++
			fmt.Printf("%d: %v <-> %v\n", i, client.LocalAddr(), client.RemoteAddr())
			ch <- client
		}
	}()
	return ch
}

func handleConn(client net.Conn) {
	b := bufio.NewReader(client)
	rec_str := ""
	for {

		var command_complete bool = false
		//Taking a command -> write/ delete/ cas/ read
		line, err := b.ReadBytes('\r')
		line1, err1 := b.ReadBytes('\n')

		if err != nil || err1 != nil { // EOF, or worse
			break
		}
		rec_str = rec_str + string(line[:len(line)-1])

		if len(line1) != 1 { // something b/w \r & \n hence part of input
			rec_str = rec_str + string(line1[:len(line1)-1])
			continue
		} else {
			command_complete = true
		}

		if command_complete {
			//Identify command

			parts := strings.Split(rec_str, " ")

			switch {
			case parts[0] == "write":
				fmt.Println("Writing")
				fmt.Println(parts[1])
				fmt.Println("Numbytes")
				fmt.Println(parts[2])

				if len(parts) == 3 {

				} else { //expiry time exists

				}

			case parts[0] == "delete":
				fmt.Println("Delete")
				fmt.Println(parts[1])

			case parts[0] == "cas":
				fmt.Println("Cas")
				fmt.Println(parts[1])

			case parts[0] == "read":
				fmt.Println("Read")
				fmt.Println(parts[1])

			}

		}
		//splits = strings.Split(rec_str, " ")

	}
}
