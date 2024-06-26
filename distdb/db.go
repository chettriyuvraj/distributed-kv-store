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
	"github.com/chettriyuvraj/distributed-kv-store/distdbclient"
	"google.golang.org/protobuf/proto"
)

var ErrKeyDoesNotExist = errors.New("this key does not exist")
var ErrInvalidOperation = errors.New("invalid operation")

/* Roles for the DB */
const (
	_ = iota
	LEADER
	FOLLOWER
)

type ReplicaWorker struct {
	receiver chan DBEntry
	config distdbclient.ClientConfig
	done chan struct{}
}

type DBEntry struct {
	Key, Val []byte
}

type DB struct {
	Entries []*DBEntry
	f       *os.File
	mu      *sync.Mutex
	config  DBConfig
	broadcaster chan DBEntry
	replicaWorkers []*ReplicaWorker
}

type DBConfig struct {
	Persist        bool
	Role           int
	DiskFileName   string
	ServerProtocol string
	ServerHost     string
	ServerPort     string
	ReplicaConfigs []distdbclient.ClientConfig

}

func (w *ReplicaWorker) String() string {
	return fmt.Sprintf("Worker: \n Protocol: %s; Host: %s; Port: %s", w.config.ServerProtocol, w.config.ServerHost, w.config.ServerPort)
}

func NewDB(config DBConfig) (*DB, error) {
	/* If db is not persistant */
	db := &DB{Entries: []*DBEntry{}, mu: &sync.Mutex{}, config: config}
	if !config.Persist {
		return db, nil
	}

	/* Initialize DB */

	/* If persistant open file, get data and keep it in memory */
	f, err := os.OpenFile(config.DiskFileName, os.O_RDWR|os.O_CREATE, 0777)
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

	/* Initialize replicas */
	err = initReplicas(db)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func initReplicas(db *DB) error {

	/* Initialize a worker + goroutine + client for each worker - to replicate the broadcast k-v */
	for _, replicaConfig := range db.config.ReplicaConfigs {
		worker := ReplicaWorker{receiver: make(chan DBEntry, 10), config: replicaConfig}
		db.replicaWorkers = append(db.replicaWorkers, &worker)
		client, err := distdbclient.NewClient(replicaConfig)
		if err != nil {
			return err
		}

		go replicate(&worker, client)
	}

	/* Initialize and start broadcast channel */
	db.broadcaster = make(chan DBEntry, 10)
	go broadcast(db)

	return nil
}

func broadcast(db *DB) {
	for entry := range db.broadcaster {
		for _, worker := range db.replicaWorkers {
			worker.receiver <- entry
		}
	}
}

func replicate(worker *ReplicaWorker, client *distdbclient.Client) {
	defer close(worker.done)
	for entry := range worker.receiver {
		err := client.Put(entry.Key, entry.Val)
		if err != nil {
			fmt.Printf("Error replicating Key: %s; Val: %s; to %s", entry.Key, entry.Val, worker)
		}
	}
	worker.done <- struct{}{}
}

func newDBEntry(key, val []byte) DBEntry {
	return DBEntry{Key: key, Val: val}
}

func (db *DB) Listen() error {
	server, err := net.Listen(db.config.ServerProtocol, db.config.ServerHost+":"+db.config.ServerPort)
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

		/* Send to broadcaster */
		if resp.Status == communication.Status_SUCCESS {
			go func(k, v []byte) {
				fmt.Printf("\nSending %s : %s to broadcaster", k, v)
				db.broadcaster <- DBEntry{Key: k, Val: v}
				fmt.Printf("\nSent %s : %s to broadcaster", k, v)
			}(clientRequest.Key, clientRequest.Val)
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
