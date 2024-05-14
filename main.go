package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"

	"github.com/chettriyuvraj/distributed-kv-store/distdb"
	"github.com/chettriyuvraj/distributed-kv-store/distdbclient"
)

const (
	SERVER                  = "server"
	CLIENT                  = "client"
	DEFAULT_SERVER_PROTOCOL = "tcp"
	DEFAULT_SERVER_HOST     = "localhost"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("usage: kv <'client'|'server'> <port> [filename]")
	}

	switch os.Args[1] {
	case SERVER:
		port, filename := os.Args[2], os.Args[3]
		runServer(port, filename)
	case CLIENT:
		port := os.Args[2]
		runClient(port)
	default:
		log.Fatalf("invalid argument")
	}

}

func runServer(port, filename string) {
	config := distdb.DBConfig{Persist: true, Role: distdb.LEADER, DiskFileName: filename, ServerPort: port, ServerProtocol: DEFAULT_SERVER_PROTOCOL, ServerHost: DEFAULT_SERVER_HOST}
	db, err := distdb.NewDB(config)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Listen()
	if err != nil {
		log.Fatal(err)
	}
}

func runClient(port string) {
	config := distdbclient.ClientConfig{ServerPort: port, ServerProtocol: DEFAULT_SERVER_PROTOCOL, ServerHost: DEFAULT_SERVER_HOST}
	client, err := distdbclient.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		op := scanner.Bytes()
		switch {
		case bytes.Equal(op, []byte("GET")):
			/* Ask for key to GET */
			fmt.Println("Enter key to GET!")
			isNext := scanner.Scan()
			if !isNext {
				fmt.Printf("error accepting key for GET %v", err)
				return
			}
			/* Get key */
			k := scanner.Bytes()
			v, err := client.Get(k)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("\nVal is %s\n", string(v))

		case bytes.Equal(op, []byte("PUT")):
			/* Ask for key and val to PUT */
			fmt.Println("Enter key and val to PUT!")
			isNext := scanner.Scan()
			if !isNext {
				fmt.Printf("error accepting key for PUT %v", err)
				return
			}
			k := scanner.Bytes()
			isNext = scanner.Scan()
			if !isNext {
				fmt.Printf("error accepting val for PUT %v", err)
				return
			}
			v := scanner.Bytes()
			/* Put key:val */
			err := client.Put(k, v)
			if err != nil {
				fmt.Printf("error putting key:val pair into DB %v", err)
				return
			}
			fmt.Println("Success!")
		default:
			fmt.Println("Invalid operation!")
		}
	}
}
