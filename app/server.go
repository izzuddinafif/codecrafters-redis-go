package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"
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

var dict map[string]string = make(map[string]string, 0)
var exp map[string]int64 = make(map[string]int64, 0)

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

func equal(sb []byte, s string) bool {
	return bytes.Equal(bytes.ToLower(sb), bytes.ToLower([]byte(s)))
}

func contains(sb []byte, s string) bool {
	return bytes.Contains(bytes.ToLower(sb), bytes.ToLower([]byte(s)))
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
		case contains(msg, "PING"):
			_, err := conn.Write([]byte("+PONG\r\n"))
			if err != nil {
				handleError(err, "Error writing PONG response")
				return
			}
		case contains(msg, "ECHO"):
			str := bytes.Fields(msg)
			d.printf("Parsed strings: %q", str)
			var data []byte
			for i, v := range str {
				if equal(v, "ECHO") && i+2 < len(str) {
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
		case contains(msg, "SET"):
			var t int
			var px bool
			str := bytes.Fields(msg)
			d.printf("Parsed strings: %q", str)
			var key, value string
			for i, v := range str {
				if equal(v, "SET") && i+4 < len(str) {
					key = string(str[i+2])
					value = string(str[i+4])
					if i+8 < len(str) && equal(str[i+6], "PX") {
						px = true
						t, err = strconv.Atoi(string(str[i+8]))
						if err != nil {
							handleError(err, "Error converting string to int")
							return
						}
					}
					break
				}
			}
			dict[key] = value
			if px {
				exp[key] = time.Now().UnixMilli() + int64(t)
			}
			d.printf("Value %q is written to key %q", value, key)
			if px {
				d.printf("Key expiration in %d milliseconds", t)
			}
			_, err := conn.Write([]byte("+OK\r\n"))
			if err != nil {
				handleError(err, "Invalid command")
				return
			}
		case contains(msg, "GET"):
			str := bytes.Fields(msg)
			d.printf("Parsed strings: %q", str)
			var key string
			for i, v := range str {
				if equal(v, "GET") && i+2 < len(str) {
					key = string(str[i+2])
					break
				}
			}
			if value, ok := dict[key]; !ok {
				d.printf("Key %q does not exist!", key)
				_, err := conn.Write([]byte("$-1\r\n"))
				if err != nil {
					handleError(err, "Error writing nil response")
					return
				}
			} else {
				if expiry, ok := exp[key]; ok {
					if expiry <= time.Now().UnixMilli() {
						d.printf("Key %q has expired!", key)
						delete(dict, key)
						delete(exp, key)
						_, err := conn.Write([]byte("$-1\r\n"))
						if err != nil {
							handleError(err, "Error writing nil response")
							return
						}
						return
					}
				}
				d.printf("Sending resp for %q key: %q value", key, value)
				len := len(value)
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len, value)
				_, err := conn.Write([]byte(resp))
				if err != nil {
					handleError(err, "Error writing value response")
				}
			}
		default:
			d.print("Invalid or incomplete command")
			_, err := conn.Write([]byte("-ERR invalid command\r\n"))
			if err != nil {
				handleError(err, "Error writing ERR response")
			}
			return
		}
	}
}
