package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
)

type debugger struct {
	enabled bool
}

func (d debugger) print(a ...interface{}) {
	if d.enabled {
		log.Print(a...)
	}
}

func (d debugger) printf(format string, a ...interface{}) {
	if d.enabled {
		log.Printf(format, a...)
	}
}

var d debugger = debugger{enabled: false}

func handleError(err error, msg string) {
	if err != nil {
		if msg != "" {
			d.printf("%s: %v", msg, err)
		} else {
			d.print(err)
		}
	}
}

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	d.enabled = true

	d.printf("Starting Afif's redis server on port %d", 6379)

	l, err := net.Listen("tcp", ":6379")
	handleError(err, "Failed to bind to port 6379")
	if err != nil {
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			handleError(err, "Error accepting connection")
			continue
		}
		d.print("Accepting connection from: ", conn.RemoteAddr())
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 1024)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			handleError(err, "Error reading from connection")
			return
		}
		msg := buf[:n]
		d.printf("Message read: %q, message length: %d", msg, len(msg))

		switch {
		case bytes.Contains(msg, []byte("PING")):
			_, err := conn.Write([]byte("+PONG\r\n"))
			if err != nil {
				handleError(err, "Error writing PONG response")
				return
			}
		case bytes.Contains(msg, []byte("ECHO")):
			str := bytes.Fields(msg)
			d.printf("Parsed strings: %q", str)
			var data []byte
			for i, v := range str {
				if bytes.Equal(v, []byte("ECHO")) && i+1 < len(str) {
					data = str[i+2]
					break
				}
			}
			resp := fmt.Sprintf("$%d\r\n%s\r\n", len(data), data)
			d.print("Writing response: ", resp)
			_, err := conn.Write([]byte(resp))
			if err != nil {
				handleError(err, "Error writing ECHO response")
				return
			}
		}
	}
}
