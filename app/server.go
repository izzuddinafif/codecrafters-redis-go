package main

import (
	"bytes"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

var d debugger = debugger{enabled: false}

func parseReq(req []byte) redisCommand {

	var cmd string
	args := []string{}
	hasExp := false
	expTime := 0

	d.printf("request: %q", req)
	parsed := bytes.Fields(req)
	d.print("parsed request: ", parsed)
	p := make([]string, len(parsed))
	for _, b := range parsed {
		p = append(p, string(b))
	}
	reqLen, _ := strconv.Atoi(string(parsed[0][1]))
	cmd = p[2]
	switch reqLen {
	case 2:
		args = append(args, p[4])
	case 3:
		args = append(args, p[4], p[6])
	case 5:
		args = append(args, p[4], p[6], p[8], p[10])
		if contains([]byte(p[8]), "PX") {
			hasExp = true
			expTime, _ = strconv.Atoi(p[10])
		}
	}
	return redisCommand{
		command:    cmd,
		args:       args,
		hasExpiry:  hasExp,
		expiryTime: int64(expTime),
	}
}

// TODO: define encodeResp()

func main() {
	d.enabled = true

	db := newRedisDB()
	conf := config{} // Create a config instance and parse the values from command-line flags
	flag.StringVar(&conf.dir, "dir", "/tmp/redis-data", "Directory for RDB file storage")
	flag.StringVar(&conf.dbfilename, "dbfilename", "dump.rdb", "RDB file name")
	flag.Parse()

	f, err := os.Open(filepath.Join(conf.dir, conf.dbfilename))
	if err != nil {
		d.print("Failed to open file: ", err)
		f = nil
		d.print("Database is empty")
	}

	d.printf("Server started with dir=%s, dbfilename=%s\n", conf.dir, conf.dbfilename)

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
		go handleConnection(conn, db, conf, f) // TODO: synchronize goroutines when modifying db
	}
}

func handleConnection(conn net.Conn, db *RedisDB, conf config, f *os.File) {
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

		str := bytes.Fields(msg)
		d.printf("Parsed strings: %q", str)
		if len(str) < 2 {
			conn.Write([]byte("-ERR incomplete command"))
			return
		}

		switch {
		case contains(str[2], "PING"):
			conn.Write([]byte("+PONG\r\n"))
		case contains(str[2], "ECHO"):
			data := str[4]
			resp := fmt.Sprintf("$%d\r\n%s\r\n", len(data), data)
			conn.Write([]byte(resp))
		case contains(str[2], "SET"):
			if len(str) < 6 || !equal(str[2], "SET") {
				conn.Write([]byte("-ERR invalid SET command\r\n"))
				return
			}
			key, value := string(str[4]), string(str[6])
			expirymillis := int64(0)
			if len(str) > 10 && equal(str[8], "PX") {
				if t, err := strconv.Atoi(string(str[10])); err == nil {
					d.printf("Expiry time is in %d milliseconds", t)
					expirymillis = int64(t)
				} else {
					conn.Write([]byte("-ERR invalid expiration time\r\n"))
					return
				}
			}
			db.set(key, value, expirymillis)
			conn.Write([]byte("+OK\r\n"))
		case contains(str[2], "GET"):
			if len(str) < 4 {
				conn.Write([]byte("-ERR invalid GET command\r\n"))
				return
			}
			key := string(str[4])
			if value, exists := db.get(key); exists {
				d.printf("Sending resp for %q key: %q value", key, value)
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
				conn.Write([]byte(resp))
			} else {
				d.printf("Key %q does not exist!", key)
				conn.Write([]byte("$-1\r\n"))
			}
		case contains(str[2], "CONFIG"):
			if len(str) < 6 {
				conn.Write([]byte("-ERR invalid CONFIG command\r\n"))
				return
			}
			param := string(str[6])
			result := conf.configGet(param)
			resp := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(param), param, len(result), result)
			conn.Write([]byte(resp))
		case contains(str[2], "KEYS"):
			pattern := string(str[4])
			if pattern == "*" { // handle glob pattern
				pattern = strings.ReplaceAll(pattern, "*", ".*")
			}
			r := regexp.MustCompile(pattern)

			content := make([]byte, 4096)
			_, err := f.Read(content)
			handleError(err, "Failed to read file content")
			htIdx := bytes.IndexByte(content, 0xFB)
			end := bytes.IndexByte(content, 0xFF)
			start := htIdx + 3 // skipping hash table size and n of keys with expiry
			content = content[start:end]
			if content[0] == 0x00 { // skipping the first 0x00
				content = content[1:]
			}
			kvPairs := bytes.Split(content, []byte{0x00}) // assuming all the values are strings
			d.print(kvPairs)
			keys := make([][]byte, len(kvPairs))
			for _, kv := range kvPairs {
				d.print("inspecting key-value pair: ", string(kv))
				sizeEnc := kv[0]        // still assuming all the values are strings
				check := sizeEnc & 0xC0 // isolate the first 2 bits by using bitwise AND with 0b11000000 (0xC0)
				switch {
				case check == 0x00: // the size is in the lower 6 bits of the byte
					idx := int(sizeEnc) + 1
					keys = append(keys, kv[1:idx])
				case check == 0x40: // 14 bit size, 6 bits of this byte + next byte included
				case check == 0x80: // 4 byte big endian size
				case check == 0xC0: // special str enc
				}
			}
			var results [][]byte
			for _, key := range keys {
				found := r.Find(key)
				if found != nil {
					d.print("found key: ", string(found))
					results = append(results, found)
				}
			}
			resp := []byte("*" + strconv.Itoa(len(results)) + "\r\n")
			for _, result := range results {
				resp = append(resp, []byte("$"+strconv.Itoa(len(result))+"\r\n"+string(result)+"\r\n")...)
			}
			d.print("sending response: ", string(resp))
			conn.Write(resp)

		default:
			d.print("Invalid or incomplete command")
			conn.Write([]byte("-ERR invalid command\r\n"))
		}
	}
}
