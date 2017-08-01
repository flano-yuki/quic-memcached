package main

import (
	"flag"
	"fmt"
	"github.com/ekr/minq"
	"net"
	"strconv"
	"strings"
	"time"
)

var addr string
var serverName string

var memcMap = map[string]Record{}
var rest []string

type Record struct {
	Key    string
	Value  string
	Flag   int
	Expire int64
	Create int64
}

type conn struct {
	conn *minq.Connection
	last time.Time
}

func (c *conn) checkTimer() {
	t := time.Now()
	if t.After(c.last.Add(time.Second)) {
		c.conn.CheckTimer()
		c.last = time.Now()
	}
}

var conns = make(map[minq.ConnectionId]*conn)

type serverHandler struct {
}

func (h *serverHandler) NewConnection(c *minq.Connection) {
	fmt.Println("New connection")
	c.SetHandler(&connHandler{})
	conns[c.Id()] = &conn{c, time.Now()}
}

type connHandler struct {
}

func (h *connHandler) StateChanged(s minq.State) {
	fmt.Println("State changed to ", s)
}

func (h *connHandler) NewStream(s *minq.Stream) {
	fmt.Println("Created new stream id=", s.Id())
}

func (h *connHandler) StreamReadable(s *minq.Stream) {
	fmt.Println("Ready to read for stream id=", s.Id())
	b := make([]byte, 1024)

	n, err := s.Read(b)
	if err != nil {
		fmt.Println("Error reading")
		return
	}
	b = b[:n]

	str := string(b)

	str = strings.Replace(str, "\n", " ", -1)
	strs := strings.Split(str, " ")
	command := strs[0]
	args := strs[1:]

	fmt.Printf("Read %v bytes from peer %x\n", n, b)

	//set test record
	test := Record{
		Key:   "key",
		Value: "value",
		Flag:  10,
	}
	memcMap["test"] = test
	if command == "get" {
		fmt.Printf("get key:%s\n", args[0])
		record, ok := memcMap[args[0]]
		output := "END\n"
		//expire
		if ok && (record.Expire > 0 && record.Create+record.Expire < time.Now().Unix()) {
			delete(memcMap, args[0])
			ok = false
		}
		if ok {
			output = "VALUE " + record.Key + " " + strconv.Itoa(record.Flag) + " " +
				strconv.Itoa(len(record.Value)) + "\n" +
				record.Value + "\n" + output
		}
		s.Write([]byte(output))
	} else if command == "set" {
		fmt.Printf("set key:%s value:%s\n", args[0], args[4])
		flag, _ := strconv.Atoi(args[1])
		expire, _ := strconv.Atoi(args[2])

		record := Record{
			Key:    args[0],
			Flag:   flag,
			Expire: int64(expire),
			Value:  args[4], //TODO join by space
			Create: time.Now().Unix(),
		}
		memcMap[args[0]] = record
		s.Write([]byte("STORED\n"))
	} else if command == "version" {
		s.Write([]byte("VERSION 0.0.0"))
	} else {
		s.Write([]byte("ERROR\n"))
	}
}

func main() {
	flag.StringVar(&addr, "addr", "localhost:4433", "[host:port]")
	flag.StringVar(&serverName, "server-name", "localhost", "[SNI]")
	flag.Parse()

	uaddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		fmt.Println("Invalid UDP addr: ", err)
		return
	}

	usock, err := net.ListenUDP("udp", uaddr)
	if err != nil {
		fmt.Println("Couldn't listen on UDP: ", err)
		return
	}

	server := minq.NewServer(minq.NewUdpTransportFactory(usock), minq.NewTlsConfig(serverName), &serverHandler{})

	for {
		b := make([]byte, 8192)

		usock.SetDeadline(time.Now().Add(time.Second))
		n, addr, err := usock.ReadFromUDP(b)
		if err != nil {
			e, o := err.(net.Error)
			if !o || !e.Timeout() {
				fmt.Println("Error reading from UDP socket: ", err)
				return
			}
			n = 0
		}

		// If we read data, process it.
		if n > 0 {
			if n == len(b) {
				fmt.Println("Underread from UDP socket")
				return
			}
			b = b[:n]

			_, err = server.Input(addr, b)
			if err != nil {
				fmt.Println("server.Input returned error: ", err)
				return
			}
		}

		// Check all the timers
		for _, c := range conns {
			c.checkTimer()
		}
	}
}
