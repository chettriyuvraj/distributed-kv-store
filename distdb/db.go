package distdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
)

var ErrKeyDoesNotExist = errors.New("this key does not exist")

const (
	DISKFILENAME = "dbdump"
)

type DBEntry struct {
	Key, Val []byte
}

type DB struct {
	Entries []*DBEntry
	f       *os.File
}

func NewDB() (*DB, error) {
	/* Open file, get data and keep it in memory */
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

	return &DB{f: f, Entries: entries}, nil
}

func newDBEntry(key, val []byte) DBEntry {
	return DBEntry{Key: key, Val: val}
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
	if err != nil {
		if errors.Is(err, ErrKeyDoesNotExist) {
			newEntry := newDBEntry(key, val)
			db.Entries = append(db.Entries, &newEntry)
			return db.writeToDisk()
		}
		return err
	}

	entry.Val = val
	return db.writeToDisk()
}

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
