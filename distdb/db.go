package distdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"sync"
)

var ErrKeyDoesNotExist = errors.New("this key does not exist")
var ErrInvalidOperation = errors.New("invalid operation")

const (
	DISKFILENAME    = "dbdump"
	SERVER_PROTOCOL = "tcp"
	SERVER_HOST     = "localhost"
	SERVER_PORT     = "3108"
	SUCCESS         = "SUCCESS"
	FAILURE         = "FAILURE"
	GET             = "GET"
	PUT             = "PUT"
)

type Request struct {
	Key, Val []byte
	Op       string
}

type Response struct {
	Status, Error string
	Val           []byte
}

type DBEntry struct {
	Key, Val []byte
}

type DB struct {
	Entries []*DBEntry
	f       *os.File
	mu      *sync.Mutex
	config  DBConfig
}

type DBConfig struct {
	Persist bool
}

func NewDBConfig(persist bool) DBConfig {
	return DBConfig{Persist: persist}
}

func NewDB(config DBConfig) (*DB, error) {
	/* If db is not persistant */
	db := &DB{Entries: []*DBEntry{}, mu: &sync.Mutex{}, config: config}
	if !config.Persist {
		return db, nil
	}

	/* If persistant open file, get data and keep it in memory */
	f, err := os.OpenFile(DISKFILENAME, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}

	var entries []*DBEntry
	decoder := json.NewDecoder(f)
	err = decoder.Decode(&entries)
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
	}

	db.f = f
	return db, nil
}

func newDBEntry(key, val []byte) DBEntry {
	return DBEntry{Key: key, Val: val}
}

func (db *DB) Listen() error {
	server, err := net.Listen(SERVER_PROTOCOL, SERVER_HOST+":"+SERVER_PORT)
	if err != nil {
		return err
	}
	defer server.Close()

	for {
		clientConn, err := server.Accept()
		if err != nil {
			return err
		}

		go db.handleConn(clientConn)
	}
}

func (db *DB) handleConn(conn net.Conn) error {
	buffer := make([]byte, 1024)

	/* Read client request */
	clientMessageLen, err := conn.Read(buffer)
	if err != nil {
		return err
	}

	/* Unmarshal request sent as JSON */
	var clientRequest Request
	clientMessage := buffer[:clientMessageLen]
	err = json.Unmarshal(clientMessage, &clientRequest)
	if err != nil {
		return err
	}

	/* Formulate response according to the operation requested */
	var resp Response
	switch clientRequest.Op {
	case GET:
		val, err := db.Get(clientRequest.Key)
		if err != nil {
			resp.Error = err.Error()
			resp.Status = FAILURE
			break
		}
		resp.Val = val
		resp.Status = SUCCESS
	case PUT:
		err := db.Put(clientRequest.Key, clientRequest.Val)
		if err != nil {
			resp.Error = err.Error()
			resp.Status = FAILURE
			break
		}
		resp.Status = SUCCESS
	default:
		resp.Error = ErrInvalidOperation.Error()
		resp.Status = FAILURE
	}

	/* Marshal response to JSON */
	respData, err := json.Marshal(resp)
	if err != nil {
		return err
	}

	/* Send response */
	if _, err = conn.Write(respData); err != nil {
		return err
	}

	return nil
}

func (db *DB) Get(key []byte) (val []byte, err error) {
	entry, err := db.get(key)
	if err != nil {
		return nil, err
	}

	return entry.Val, nil
}

func (db *DB) get(key []byte) (entry *DBEntry, err error) {
	for _, entry := range db.Entries {
		if bytes.Equal(key, entry.Key) {
			return entry, nil
		}
	}

	return nil, ErrKeyDoesNotExist
}

func (db *DB) Put(key, val []byte) error {
	entry, err := db.get(key)
	db.mu.Lock()
	defer db.mu.Unlock()
	if err != nil {
		if errors.Is(err, ErrKeyDoesNotExist) {
			newEntry := newDBEntry(key, val)
			db.Entries = append(db.Entries, &newEntry)
			if !db.config.Persist {
				return nil
			}
			return db.writeToDisk()
		}
		return err
	}

	entry.Val = val
	if !db.config.Persist {
		return nil
	}
	return db.writeToDisk()
}

/* Call this only with db.Mutex held */
func (db *DB) writeToDisk() error {
	/* Truncate entire file */
	_, err := db.f.Seek(0, 0)
	if err != nil {
		return err
	}

	err = db.f.Truncate(0)
	if err != nil {
		return err
	}

	/* Rewrite the data */
	encoder := json.NewEncoder(db.f)
	err = encoder.Encode(db.Entries)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) Close() error {
	return db.f.Close()
}
