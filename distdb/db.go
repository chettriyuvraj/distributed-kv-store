package distdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"

	"github.com/chettriyuvraj/distributed-kv-store/protobuf/github.com/chettriyuvraj/distributed-kv-store/communication"
	"google.golang.org/protobuf/proto"
)

var ErrKeyDoesNotExist = errors.New("this key does not exist")
var ErrInvalidOperation = errors.New("invalid operation")

const (
	DISKFILENAME    = "dbdump"
	SERVER_PROTOCOL = "tcp"
	SERVER_HOST     = "localhost"
	SERVER_PORT     = "3108"
)

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
	db.Entries = append(db.Entries, entries...)
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
	fmt.Println("Listening...")
	defer server.Close()

	for {
		clientConn, err := server.Accept()
		if err != nil {
			return err
		}
		fmt.Println("Accepted Connection...")

		go db.handleConn(clientConn)
	}
}

func (db *DB) handleConn(conn net.Conn) error {
	for {
		buffer := make([]byte, 1024)

		/* Read client request */
		clientMessageLen, err := conn.Read(buffer)
		if err != nil {
			return err
		}

		/* Unmarshal request sent as JSON */
		var clientRequest communication.Request
		clientMessage := buffer[:clientMessageLen]
		err = proto.Unmarshal(clientMessage, &clientRequest)
		if err != nil {
			return err
		}

		/* Formulate response according to the operation requested */
		var resp communication.Response
		switch clientRequest.Op {
		case communication.Operation_GET:
			fmt.Println("Handling GET request...")
			val, err := db.Get(clientRequest.Key)
			if err != nil {
				resp.Error = err.Error()
				resp.Status = communication.Status_FAILURE
				break
			}
			resp.Val = val
			resp.Status = communication.Status_SUCCESS
		case communication.Operation_PUT:
			fmt.Println("Handling PUT request...")
			err := db.Put(clientRequest.Key, clientRequest.Val)
			if err != nil {
				resp.Error = err.Error()
				resp.Status = communication.Status_FAILURE
				break
			}
			resp.Status = communication.Status_SUCCESS
		default:
			resp.Error = ErrInvalidOperation.Error()
			resp.Status = communication.Status_FAILURE
		}

		/* Marshal response to JSON */
		respData, err := proto.Marshal(&resp)
		if err != nil {
			return err
		}

		/* Send response */
		if _, err = conn.Write(respData); err != nil {
			return err
		}

	}

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
