package main

import (
	"bytes"
	"flag"
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

type RedisDB struct {
	dict   map[string]string
	expiry map[string]int64
}

func newRedisDB() *RedisDB {
	return &RedisDB{
		dict:   make(map[string]string),
		expiry: make(map[string]int64),
	}
}

func (db *RedisDB) set(key, value string, expiryMillis int64) {
	db.dict[key] = value
	if expiryMillis > 0 {
		db.expiry[key] = time.Now().UnixMilli() + expiryMillis
		d.printf("Key %q set with expiry in %d milliseconds", key, expiryMillis)
	} else {
		delete(db.expiry, key) // remove residual expiry key (if exists)
	}
}

func (db *RedisDB) get(key string) (string, bool) {
	if db.isExpired(key) {
		delete(db.dict, key)
		delete(db.expiry, key)
		return "", false
	}
	value, exists := db.dict[key]
	return value, exists
}

func (db *RedisDB) isExpired(key string) bool {
	expiry, exists := db.expiry[key]
	return exists && time.Now().UnixMilli() > expiry
}

type config struct {
	dir        string
	dbfilename string
}

func (conf config) configGet(param string) string {
	switch param {
	case "dir":
		return conf.dir
	case "dbfilename":
		return conf.dbfilename
	default:
		return "-ERR unknown parameter\r\n"
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

func equal(sb []byte, s string) bool {
	return bytes.Equal(bytes.ToLower(sb), bytes.ToLower([]byte(s)))
}

func contains(sb []byte, s string) bool {
	return bytes.Contains(bytes.ToLower(sb), bytes.ToLower([]byte(s)))
}

func main() {
	d.enabled = true

	db := newRedisDB()
	conf := config{} // Create a config instance and parse the values from command-line flags
	flag.StringVar(&conf.dir, "dir", "/tmp/redis-data", "Directory for RDB file storage")
	flag.StringVar(&conf.dbfilename, "dbfilename", "dump.rdb", "RDB file name")
	flag.Parse()

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
		go handleConnection(conn, db, conf) // TODO: synchronize goroutines when modifying db
	}
}

func handleConnection(conn net.Conn, db *RedisDB, conf config) {
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
		default:
			d.print("Invalid or incomplete command")
			conn.Write([]byte("-ERR invalid command\r\n"))
		}
	}
}
