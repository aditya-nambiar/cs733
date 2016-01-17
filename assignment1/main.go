package main

import (
	"bufio"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

const PORT = 8080

func main() {
	serverMain()
}

func serverMain() {
	server, err := net.Listen("tcp", ":"+strconv.Itoa(PORT))
	if server == nil {
		panic("couldn't start listening: " + err.Error())
	}
	go action_db()
	db, _ := leveldb.OpenFile(path.Join(os.TempDir(), "goleveldb-testdb"), nil)

	defer db.Close()
	conns := clientConns(server)
	for {
		go handleConn(<-conns, db)
	}
}

func action_db() {

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

func readNumBytes(num int, b *bufio.Reader, all_bytes []byte) {
	for i := 0; i < num; i++ {
		all_bytes[i], _ = b.ReadByte()
	}
	b.ReadByte()
	b.ReadByte()
	fmt.Println("Returning")
}

func handleConn(client net.Conn, db *leveldb.DB) {
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
			rec_str = ""

			switch {
			case parts[0] == "write":

				version, err := db.Get([]byte("Version:"+parts[1]), nil)
				ver := 100000
				if err == nil { //update
					ver, _ := strconv.Atoi(string(version))
					ver = ver + 1
				}

				if len(parts) == 3 {
					_ = db.Put([]byte("ExpTime:"+parts[1]), []byte("0"), nil)

				} else { //expiry time exists
					_ = db.Put([]byte("ExpTime:"+parts[1]), []byte(parts[3]), nil)

				}
				num, _ := strconv.Atoi(parts[2])
				all_bytes := make([]byte, num)
				readNumBytes(num, b, all_bytes[0:])
				sec := time.Now().Second()
				_ = db.Put([]byte("Name:"+parts[1]), all_bytes, nil)
				_ = db.Put([]byte("NumBytes:"+parts[1]), []byte(parts[2]), nil)
				_ = db.Put([]byte("TimeStamp:"+parts[1]), []byte(strconv.Itoa(sec)), nil)
				_ = db.Put([]byte("Version:"+parts[1]), []byte(strconv.Itoa(ver)), nil)
				client.Write([]byte("OK " + strconv.Itoa(ver) + "\r\n"))
				break

			case parts[0] == "delete":
				version, err := db.Get([]byte("Version:"+parts[1]), nil)
				if err != nil {
					client.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
					break
				}
				_ = db.Delete([]byte("Name:"+parts[1]), nil)
				_ = db.Delete([]byte("Version:"+parts[1]), nil)
				_ = db.Delete([]byte("TimeStamp:"+parts[1]), nil)
				_ = db.Delete([]byte("ExpTime:"+parts[1]), nil)
				_ = db.Delete([]byte("NumBytes:"+parts[1]), nil)
				client.Write([]byte("OK\r\n"))

				break

			case parts[0] == "cas":
				fmt.Println("Cas")
				fmt.Println(parts[1])
				break

			case parts[0] == "read":
				data, errd := db.Get([]byte("Name:"+parts[1]), nil)
				numbytes, err := db.Get([]byte("NumBytes:"+parts[1]), nil)
				version, err := db.Get([]byte("Version:"+parts[1]), nil)
				timestamp, err := db.Get([]byte("TimeStamp:"+parts[1]), nil)
				exp, err := db.Get([]byte("ExpTime:"+parts[1]), nil)
				exp_time, _ := strconv.Atoi(string(exp))
				time_sec, _ := strconv.Atoi(string(timestamp))
				sec := time.Now().Second()
				if errd != nil || sec > time_sec+exp_time {
					client.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
					break
				}
				client.Write([]byte("CONTENTS " + strconv.Itoa(version) + " " + string(numbytes) + " " + string(exp) + "\r\n"))
				client.Write(data)
				client.Write([]byte("\r\n"))
				break
			default:
				fmt.Println("Error")
				break

			}

		}
	}
}
