package distdb

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/chettriyuvraj/distributed-kv-store/client"
	"github.com/stretchr/testify/require"
)

func TestGetPut(t *testing.T) {
	config := NewDBConfig(false)

	db, err := NewDB(config)
	require.NoError(t, err)

	/* Get a non existing key */
	keyNonExistent := []byte("kNE")
	_, err = db.Get(keyNonExistent)
	require.ErrorIs(t, err, ErrKeyDoesNotExist)

	/* Get-Put a new key-value pair */
	k1, v1 := []byte("key1"), []byte("val1")
	err = db.Put(k1, v1)
	require.NoError(t, err)
	v1FromDB, err := db.Get(k1)
	require.NoError(t, err)
	require.Equal(t, v1, v1FromDB)

	/* Overwrite an existing val */
	v2 := []byte("val2")
	err = db.Put(k1, v2)
	require.NoError(t, err)
	v2FromDB, err := db.Get(k1)
	require.NoError(t, err)
	require.Equal(t, v2, v2FromDB)
}

/* TODO: test mutex? currently not tested, also how would you test it? */
func TestHandleConn(t *testing.T) {
	config := NewDBConfig(false)

	/* Create server and start listening */
	db, err := NewDB(config)
	require.NoError(t, err)

	go db.Listen()

	/* Start a new goroutine, with a new client for each request - TODO: error cases */
	tcs := []struct {
		req      Request
		respWant Response
	}{
		{req: Request{Key: []byte("key2"), Val: []byte("val2"), Op: PUT}, respWant: Response{Status: SUCCESS, Error: "", Val: nil}},
		{req: Request{Key: []byte("key2"), Op: GET}, respWant: Response{Status: SUCCESS, Error: "", Val: []byte("val2")}},
		{req: Request{Key: []byte("key1"), Op: GET}, respWant: Response{Status: FAILURE, Error: ErrKeyDoesNotExist.Error(), Val: nil}},
	}

	/* Wait Group to ensure test completes only when each request finishes */
	var wg sync.WaitGroup

	for _, tc := range tcs {
		wg.Add(1)

		client, err := client.NewClient()
		require.NoError(t, err)

		req := tc.req

		go func(respWant Response) {
			defer wg.Done()
			client.MakeRequest(req.Key, req.Val, req.Op)
			require.NoError(t, err)

			var response Response
			respData, err := client.RcvResponse()
			require.NoError(t, err)
			err = json.Unmarshal(respData, &response)
			require.NoError(t, err)
			require.Equal(t, respWant, response)
		}(tc.respWant)
	}

	wg.Wait()
}
