package main

import (
	"bufio"
	"fmt"
	"net"
	//"os"
	"testing"
)

func TestPrintSomething(t *testing.T) {
	fmt.Println("Say hi")
	t.Log("Say bye")
}

func TestHome(t *testing.T) {
	t.Logf("..sdfsfsd.")

	conn, _ := net.Dial("tcp", "localhost:8080")
	//checkError(err)
	_, _ = conn.Write([]byte("write filename 23\r\n"))

	//	_, _ = conn.Write([]byte(" 13 234\r\n"))

	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		fmt.Println(err)
		fmt.Println(line)
		if err != nil {
			conn.Close()
			break

		}
	}

}
