package main

import (
	"bufio"
	//"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func init() {
	go serverMain()
}

// func readNumBytes(num int, b *bufio.Reader, all_bytes []byte) {
// 	for i := 0; i < num; i++ {
// 		all_bytes[i], _ = b.ReadByte()
// 	}
// 	_, _ = b.ReadByte()
// 	_, _ = b.ReadByte()
// }

func TestWrite(t *testing.T) {

	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	//checkError(err)
	reader := bufio.NewReader(conn)
	buf := make([]byte, 1024)

	for i := 0; i < 10; i++ {
		_, _ = conn.Write([]byte("write #try12&ab$@$** 23\r\n"))
		_, _ = conn.Write([]byte("234329giwe039he2~@#4%!@\r\n"))

		num, err := reader.Read(buf)
		if err != nil {
			t.Error("Error in receiving from server")
			break
		}
		if string(buf[:2]) != "OK" { //Version number starts from 0 to 9999
			t.Error(string(buf[:num]))
		}
	}

	_, _ = conn.Write([]byte("delete #try12&ab$@$**\r\n"))
	num, err := reader.Read(buf)
	if string(buf[:num]) != "OK\r\n" || err != nil {
		t.Error("Error in deletion")
	}
}

func TestWriteExpiry(t *testing.T) {
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	//checkError(err)
	reader := bufio.NewReader(conn)

	_, _ = conn.Write([]byte("write file123@#! 23 1\r\n"))
	_, _ = conn.Write([]byte("234329giwe039he2~@#4%!@\r\n"))
	line, _ := reader.ReadBytes('\r')
	line, _ = reader.ReadBytes('\n')

	time.Sleep(200 * time.Millisecond)
	_, _ = conn.Write([]byte("read file123@#!\r\n"))
	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	if string(line[:8]) != "CONTENTS" {
		t.Error(string(line))
	}
	readresp := strings.Split(string(line), " ")
	n, _ := strconv.Atoi(readresp[2])

	all_bytes := make([]byte, n)
	readNumBytes(n, reader, all_bytes)

	time.Sleep(3000 * time.Millisecond)
	_, _ = conn.Write([]byte("read file123@#!\r\n"))
	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')
	if string(line)+"\n" != "ERR_FILE_NOT_FOUND\r\n" {
		t.Error(string(line) + "\r\n")
	}

	_, _ = conn.Write([]byte("delete file123@#!\r\n"))

}

func TestPartialWrite(t *testing.T) {
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	//checkError(err)
	reader := bufio.NewReader(conn)
	buf := make([]byte, 1024)
	_, _ = conn.Write([]byte("wri"))
	time.Sleep(1000 * time.Millisecond)
	_, _ = conn.Write([]byte("te fi"))
	_, _ = conn.Write([]byte("le## 9\r\n"))
	_, _ = conn.Write([]byte("a\r\n"))
	time.Sleep(1000 * time.Millisecond)
	_, _ = conn.Write([]byte("d\n\r"))
	_, _ = conn.Write([]byte("\rdf"))
	time.Sleep(1000 * time.Millisecond)
	_, _ = conn.Write([]byte("\r\n"))
	num, err := reader.Read(buf)
	if string(buf[:num]) != "OK 0\r\n" || err != nil {
		t.Error(string(buf[:num]))
	}
	_, _ = conn.Write([]byte("delete file##\r\n"))

}

func make_client(t *testing.T, wg *sync.WaitGroup, x int) {
	defer (*wg).Done()
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	//checkError(err)
	reader := bufio.NewReader(conn)
	_, _ = conn.Write([]byte("write " + strconv.Itoa(x) + " 23\r\n"))
	_, _ = conn.Write([]byte("234329giwe039he2~@#4%!@\r\n"))
	line, _ := reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	var str_temp string = string(line)
	if len(str_temp) < 2 || str_temp[:2] != "OK" {
		t.Error(str_temp)
	}
	_, _ = conn.Write([]byte("read " + strconv.Itoa(x) + "\r\n"))
	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	str_temp = string(line)
	if len(str_temp) < 8 || str_temp[:8] != "CONTENTS" {
		t.Error(str_temp)
	}

	parts := strings.Split(string(line), " ")
	if len(parts) < 3 {
		t.Error("Incorrect Response")
		return
	}
	n, _ := strconv.Atoi(parts[2])
	all_bytes := make([]byte, n)
	readNumBytes(n, reader, all_bytes)

	_, _ = conn.Write([]byte("cas " + strconv.Itoa(x) + " 12 23\r\n"))
	_, _ = conn.Write([]byte("234329giwe039he2~@#4%!@\r\n"))
	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	str_temp = string(line)

	if len(str_temp) < 2 || !(str_temp[:2] == "OK" || str_temp[:11] == "ERR_VERSION") {
		t.Error(str_temp)
	}

}

func TestMultipleClients(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1000)

	for i := 0; i < 1000; i++ {

		go make_client(t, &wg, i)
		time.Sleep(50 * time.Millisecond)

	}

	wg.Wait()
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	//buf := make([]byte, 1024)

	// _, _ = conn.Write([]byte("delete rand_file!\r\n"))
	// num, err := conn.Read(buf)
	// if string(buf[:num]) != "OK\r\n" || err != nil {
	// 	t.Error(string(buf[:num]))
	// }

	conn.Close()

}

func TestCASErrors(t *testing.T) {
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()
	_, _ = conn.Write([]byte("write casfile! 23\r\n"))
	_, _ = conn.Write([]byte("234329giwe039he2~@#4%!@\r\n"))
	reader := bufio.NewReader(conn)

	line, _ := reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')
	parts := strings.Split(string(line[:len(line)-1]), " ")
	for i := 0; i < 1000; i++ {
		_, _ = conn.Write([]byte("cas casfile! " + parts[1] + " 1\r\n"))
		_, _ = conn.Write([]byte("x\r\n"))
		line, _ = reader.ReadBytes('\r')
		_, _ = reader.ReadBytes('\n')
		if string(line[:2]) != "OK" {
			t.Error(string(line))
		}
		parts = strings.Split(string(line[:len(line)-1]), " ")

	}

	_, _ = conn.Write([]byte("delete casfile!\r\n"))

}

func TestBigFile(t *testing.T) {
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	_, _ = conn.Write([]byte("write bigfile! 1000000\r\n"))
	for i := 0; i < 100000; i++ {
		_, _ = conn.Write([]byte("2222222222"))
	}
	buf := make([]byte, 1024)

	_, _ = conn.Write([]byte("\r\n"))

	num, err := conn.Read(buf)
	if string(buf[:2]) != "OK" || err != nil {
		t.Error(string(buf[:num]))
	}

	_, _ = conn.Write([]byte("delete bigfile!\r\n"))
}

func TestExcessBytesInWrite(t *testing.T) {
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	//checkError(err)
	reader := bufio.NewReader(conn)
	_, _ = conn.Write([]byte("write files## 5\r\n"))
	_, _ = conn.Write([]byte("1234556899\r\n"))
	line, _ := reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	if string(line)+"\n" != "OK 0\r\n" {
		t.Error(string(line) + "\n")
	}

	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	if string(line)+"\n" != "ERR_CMD_ERR\r\n" {
		t.Error(string(line) + "\n")
	}

	_, _ = conn.Write([]byte("read files##\r\n"))
	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')
	if string(line)+"\n" != "CONTENTS 0 5 0\r\n" {
		t.Error(string(line) + "\r\n")
	}

	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')
	if string(line)+"\n" != "12345\r\n" {
		t.Error(string(line) + "\r\n")
	}

	_, _ = conn.Write([]byte("delete files##\r\n"))

}
