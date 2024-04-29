package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"

	"github.com/chettriyuvraj/distributed-kv-store/distdb"
)

func main() {
	db, err := distdb.NewDB()
	if err != nil {
		fmt.Printf("error initializing DB: %v", err)
		return
	}
	defer db.Close()

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
			v, err := db.Get(k)
			if err != nil {
				if errors.Is(err, distdb.ErrKeyDoesNotExist) {
					fmt.Println("Key does not exist")
					continue
				} else {
					fmt.Printf("error retreiving key:val pair from db %v", err)
					return
				}
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
			err := db.Put(k, v)
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
