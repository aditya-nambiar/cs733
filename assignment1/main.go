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
	"sync"
	"time"
)

const PORT = 8080

func main() {
	serverMain()
}

var mutex = &sync.Mutex{}

func serverMain() {
	server, err := net.Listen("tcp", ":"+strconv.Itoa(PORT))
	if server == nil {
		panic("couldn't start listening: " + err.Error())
	}

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
	_, _ = b.ReadByte()
	_, _ = b.ReadByte()
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

		//fmt.Println("Command Complete := " + rec_str)

		if command_complete {
			//Identify command

			parts := strings.Split(rec_str, " ")
			rec_str = ""

			switch {
			case parts[0] == "write":

				version, err := db.Get([]byte("Version:"+parts[1]), nil)
				ver := 0
				if err == nil { //update
					ver, _ = strconv.Atoi(string(version))
					ver = ver + 1
				}

				num, err := strconv.Atoi(parts[2])
				if err != nil {
					client.Write([]byte("ERR_CMD_ERR\r\n"))
					break
				}

				all_bytes := make([]byte, num)
				readNumBytes(num, b, all_bytes[0:])
				sec := time.Now().Second()

				// Critical

				mutex.Lock()
				if len(parts) == 3 {
					_ = db.Put([]byte("ExpTime:"+parts[1]), []byte("0"), nil)

				} else { //expiry time exists
					_ = db.Put([]byte("ExpTime:"+parts[1]), []byte(parts[3]), nil)

				}
				_ = db.Put([]byte("Name:"+parts[1]), all_bytes, nil)
				_ = db.Put([]byte("NumBytes:"+parts[1]), []byte(parts[2]), nil)
				_ = db.Put([]byte("TimeStamp:"+parts[1]), []byte(strconv.Itoa(sec)), nil)
				_ = db.Put([]byte("Version:"+parts[1]), []byte(strconv.Itoa(ver)), nil)
				mutex.Unlock()

				client.Write([]byte("OK " + strconv.Itoa(ver) + "\r\n"))
				break

			case parts[0] == "delete":

				mutex.Lock()
				_, err := db.Get([]byte("Version:"+parts[1]), nil)
				mutex.Unlock()
				if err != nil {
					client.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
					break
				}

				mutex.Lock()
				_ = db.Delete([]byte("Name:"+parts[1]), nil)
				_ = db.Delete([]byte("Version:"+parts[1]), nil)
				_ = db.Delete([]byte("TimeStamp:"+parts[1]), nil)
				_ = db.Delete([]byte("ExpTime:"+parts[1]), nil)
				_ = db.Delete([]byte("NumBytes:"+parts[1]), nil)
				mutex.Unlock()
				client.Write([]byte("OK\r\n"))

				break

			case parts[0] == "cas":
				version, err := db.Get([]byte("Version:"+parts[1]), nil)
				ver, _ := strconv.Atoi(string(version))
				timestamp, _ := db.Get([]byte("TimeStamp:"+parts[1]), nil)
				exp, err := db.Get([]byte("ExpTime:"+parts[1]), nil)
				exp_time, _ := strconv.Atoi(string(exp))
				time_sec, _ := strconv.Atoi(string(timestamp))
				sec := time.Now().Second()
				if err != nil || (exp_time != 0 && sec > time_sec+exp_time) {
					client.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
					break
				}
				if string(version) != parts[2] {
					client.Write([]byte("ERR_VERSION\r\n"))
					break
				}

				num, err := strconv.Atoi(parts[3])
				if err != nil {
					client.Write([]byte("ERR_CMD_ERR\r\n"))
					break
				}

				all_bytes := make([]byte, num)
				readNumBytes(num, b, all_bytes[0:])

				mutex.Lock()
				if len(parts) == 4 {
					_ = db.Put([]byte("ExpTime:"+parts[1]), []byte("0"), nil)

				} else { //expiry time exists
					_ = db.Put([]byte("ExpTime:"+parts[1]), []byte(parts[4]), nil)

				}
				_ = db.Put([]byte("Name:"+parts[1]), all_bytes, nil)
				_ = db.Put([]byte("NumBytes:"+parts[1]), []byte(parts[2]), nil)
				_ = db.Put([]byte("TimeStamp:"+parts[1]), []byte(strconv.Itoa(sec)), nil)
				ver = ver + 1
				_ = db.Put([]byte("Version:"+parts[1]), []byte(strconv.Itoa(ver)), nil)
				mutex.Unlock()

				client.Write([]byte("OK " + strconv.Itoa(ver) + "\r\n"))

				break

			case parts[0] == "read":
				mutex.Lock()
				data, err := db.Get([]byte("Name:"+parts[1]), nil)
				numbytes, _ := db.Get([]byte("NumBytes:"+parts[1]), nil)
				version, _ := db.Get([]byte("Version:"+parts[1]), nil)
				timestamp, _ := db.Get([]byte("TimeStamp:"+parts[1]), nil)
				exp, err := db.Get([]byte("ExpTime:"+parts[1]), nil)
				mutex.Unlock()
				exp_time, _ := strconv.Atoi(string(exp))
				time_sec, _ := strconv.Atoi(string(timestamp))
				sec := time.Now().Second()

				if err != nil || (exp_time != 0 && sec > time_sec+exp_time) {
					client.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
					break
				}
				client.Write([]byte("CONTENTS " + string(version) + " " + string(numbytes) + " " + string(exp) + "\r\n"))
				temp := make([]byte, 2)
				temp[0] = '\r'
				temp[1] = '\n'
				client.Write(append(data, temp...))
				//client.Write([]byte("\r\n"))
				break
			default:
				client.Write([]byte("ERR_CMD_ERR\r\n"))
				break

			}

		}
	}
}
