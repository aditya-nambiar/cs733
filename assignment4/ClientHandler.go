package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

)

var errNoConn = errors.New("Connection is closed")

type Msg struct {
		     // Kind = the first character of the command. For errors, it
		     // is the first letter after "ERR_", ('V' for ERR_VERSION, for
		     // example), except for "ERR_CMD_ERR", for which the kind is 'M'
	Kind     byte
	Filename string
	Contents []byte
	Numbytes int
	Exptime  int // expiry time in seconds
	Version  int
}

type Client struct {
	conn   *net.TCPConn
	reader *bufio.Reader // a bufio Reader wrapper over conn
}

func (cl *Client) read(filename string) (*Msg, error) {
	cmd := "read " + filename + "\r\n"
	return cl.sendRcv(cmd)
}

func (cl *Client) write(filename string, contents string, exptime int) (*Msg, error) {
	var cmd string
	if exptime == 0 {
		cmd = fmt.Sprintf("write %s %d\r\n", filename, len(contents))
	} else {
		cmd = fmt.Sprintf("write %s %d %d\r\n", filename, len(contents), exptime)
	}
	cmd += contents + "\r\n"
	return cl.sendRcv(cmd)
}

func (cl *Client) cas(filename string, version int, contents string, exptime int) (*Msg, error) {
	var cmd string
	if exptime == 0 {
		cmd = fmt.Sprintf("cas %s %d %d\r\n", filename, version, len(contents))
	} else {
		cmd = fmt.Sprintf("cas %s %d %d %d\r\n", filename, version, len(contents), exptime)
	}
	cmd += contents + "\r\n"
	return cl.sendRcv(cmd)
}

func (cl *Client) delete(filename string) (*Msg, error) {
	cmd := "delete " + filename + "\r\n"
	return cl.sendRcv(cmd)
}


func (cl *Client) send(str string) error {
	if cl.conn == nil {
		return errNoConn
	}
	//fmt.Println("COmmand sent" + str)
	_, err := cl.conn.Write([]byte(str))
	if err != nil {
		err = fmt.Errorf("Write error in SendRaw: %v", err)
		cl.conn.Close()
		cl.conn = nil
	}
	return err
}

func (cl *Client) sendRcv(str string) (msg *Msg, err error) {

	if cl.conn == nil {
		return nil, errNoConn
	}
	err = cl.send(str)
	//for i :=0; ; i++{}
	if err == nil {
		msg, err = cl.rcv()
	}
	return msg, err
}
func (cl *Client) rcv() (msg *Msg, err error) {
	// we will assume no errors in server side formatting
	//.Println("Waiting for msg from server")
	line, err := cl.reader.ReadString('\n')
	//fmt.Println("Got A MS`1G")
	//.Println(string(line))

	//fmt.Println(err)

	if err == nil {
		msg, err = parseFirst(line)
		if err != nil {
			return nil, err
		}
		if msg.Kind == 'C' {
			contents := make([]byte, msg.Numbytes)
			var c byte
			for i := 0; i < msg.Numbytes; i++ {
				if c, err = cl.reader.ReadByte(); err != nil {
					break
				}
				contents[i] = c
			}
			if err == nil {
				msg.Contents = contents
				cl.reader.ReadByte() // \r
				cl.reader.ReadByte() // \n
			}
		}
	}
	if err != nil {
		cl.close()
	}
	return msg, err
}

func parseFirst(line string) (msg *Msg, err error) {
	//	fmt.Printf("%v",line)
	fields := strings.Fields(line)
	msg = &Msg{}

	// Utility function fieldNum to int
	toInt := func(fieldNum int) int {
		var i int
		if err == nil {
			if fieldNum >= len(fields) {
				err = errors.New(fmt.Sprintf("Not enough fields. Expected field #%d in %s\n", fieldNum, line))
				return 0
			}
			i, err = strconv.Atoi(fields[fieldNum])
		}
		return i
	}

	if len(fields) == 0 {
		return nil, errors.New("Empty line. The previous command is likely at fault")
	}
	switch fields[0] {
	case "OK": // OK [version]
		msg.Kind = 'O'
		if len(fields) > 1 {
			msg.Version = toInt(1)
		}
	case "CONTENTS": // CONTENTS <version> <numbytes> <exptime> \r\n
		msg.Kind = 'C'
		msg.Version = toInt(1)
		msg.Numbytes = toInt(2)
		msg.Exptime = toInt(3)
	case "ERR_VERSION":
		msg.Kind = 'V'
		msg.Version = toInt(1)
	case "ERR_FILE_NOT_FOUND":
		msg.Kind = 'F'
	case "ERR_CMD_ERR":
		msg.Kind = 'M'
	case "ERR_INTERNAL":
		msg.Kind = 'I'
	case "ERR_REDIRECT":
		msg.Kind = 'R'
		if len(fields) < 2 {
			msg.Contents = []byte("localhost:9999")
		} else {
			msg.Contents = []byte(fields[1])
		}
	default:
		err = errors.New("Unknown response " + fields[0])
	}
	if err != nil {
		return nil, err
	} else {
		return msg, nil
	}
}

func mkClient(addr string) *Client {
	var client *Client
	raddr, err := net.ResolveTCPAddr("tcp", addr)
	if err == nil {
		conn, err := net.DialTCP("tcp", nil, raddr)
		//fmt.Println("Trying to connect " + addr)
		if err == nil {
			client = &Client{conn: conn, reader: bufio.NewReader(conn)}
			//fmt.Println("Add to connect" + addr)
		} else {
			//fmt.Printf("Error in mkclient\n")

		}
	} else {
		//fmt.Printf("Error in mkclient\n")

	}
	//fmt.Println("Add to connec345t" + addr)
	if err != nil {
		fmt.Printf("Error in mkclient\n")
		return nil
	}
	return client
}


func (cl *Client) close() {

	if cl != nil && cl.conn != nil {
		cl.conn.Close()
		cl.conn = nil
	}
}
