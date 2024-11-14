package main

import (
	"log"
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

func handleError(err error, msg string) {
	if err != nil {
		if msg != "" {
			d.printf("%s: %v", msg, err)
		} else {
			d.print(err)
		}
	}
}

type redisCommand struct {
	command    string
	args       []string
	hasExpiry  bool
	expiryTime int64
}
