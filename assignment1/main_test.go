package main

import (
	"bufio"
	//"fmt"
	"net"
	//"strconv"
	"sync"
	"testing"
	"time"
)

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
	line2, _ := reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')
	if string(line)+"\n"+string(line2)+"\n" != "CONTENTS 0 23 1\r\n234329giwe039he2~@#4%!@\r\n" {
		t.Error(string(line) + "\n" + string(line2) + "\n")
	}

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

func make_client(t *testing.T, wg *sync.WaitGroup) {
	defer (*wg).Done()
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	//checkError(err)
	reader := bufio.NewReader(conn)
	_, _ = conn.Write([]byte("write rand_file! 23\r\n"))
	_, _ = conn.Write([]byte("234329giwe039he2~@#4%!@\r\n"))
	line, _ := reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	var str_temp string = string(line)
	if str_temp[:2] != "OK" {
		t.Error(str_temp)
	}
	_, _ = conn.Write([]byte("read rand_file!\r\n"))
	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	str_temp = string(line)
	if str_temp[:8] != "CONTENTS" {
		t.Error(str_temp)
	}

	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	_, _ = conn.Write([]byte("cas rand_file! 12 23\r\n"))
	_, _ = conn.Write([]byte("234329giwe039he2~@#4%!@\r\n"))
	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	str_temp = string(line)

	if !(str_temp[:2] == "OK" || str_temp[:11] == "ERR_VERSION") {
		t.Error(str_temp)
	}

}

func TestMultipleClients(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(10)

	for i := 0; i < 10; i++ {

		go make_client(t, &wg)

	}

	wg.Wait()
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	buf := make([]byte, 1024)

	_, _ = conn.Write([]byte("delete rand_file!\r\n"))
	num, err := conn.Read(buf)
	if string(buf[:num]) != "OK\r\n" || err != nil {
		t.Error(string(buf[:num]))
	}

	conn.Close()

}

func TestCASErrors(t *testing.T) {
	buf := make([]byte, 1024)
	conn, _ := net.Dial("tcp", "localhost:8080")
	_, _ = conn.Write([]byte("write casfile! 23\r\n"))
	_, _ = conn.Write([]byte("234329giwe039he2~@#4%!@\r\n"))
	_, _ = conn.Read(buf)

	for i := 0; i < 10; i++ {
		_, _ = conn.Write([]byte("cas rand_file! 12 1\r\n"))
		_, _ = conn.Write([]byte("x\r\n"))
	}

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
