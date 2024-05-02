package distdb

import (
	"sync"
	"testing"

	"github.com/chettriyuvraj/distributed-kv-store/distdbclient"
	"github.com/chettriyuvraj/distributed-kv-store/protobuf/github.com/chettriyuvraj/distributed-kv-store/communication"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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
		req      *communication.Request
		respWant *communication.Response
	}{
		{req: &communication.Request{Key: []byte("key2"), Val: []byte("val2"), Op: communication.Operation_PUT}, respWant: &communication.Response{Status: communication.Status_SUCCESS, Error: "", Val: nil}},
		{req: &communication.Request{Key: []byte("key2"), Op: communication.Operation_GET}, respWant: &communication.Response{Status: communication.Status_SUCCESS, Error: "", Val: []byte("val2")}},
		{req: &communication.Request{Key: []byte("key1"), Op: communication.Operation_GET}, respWant: &communication.Response{Status: communication.Status_FAILURE, Error: ErrKeyDoesNotExist.Error(), Val: nil}},
	}

	/* Wait Group to ensure test completes only when each request finishes */
	var wg sync.WaitGroup

	for _, tc := range tcs {
		wg.Add(1)

		req := tc.req

		go func(req *communication.Request, respWant *communication.Response) {
			defer wg.Done()

			client, err := distdbclient.NewClient()
			require.NoError(t, err)

			client.MakeRequest(&communication.Request{Key: req.Key, Val: req.Val, Op: req.Op})
			require.NoError(t, err)

			var response communication.Response
			respData, err := client.RcvResponse()

			require.NoError(t, err)
			err = proto.Unmarshal(respData, &response)
			require.NoError(t, err)

			require.Equal(t, respWant.Status, response.Status)
			require.Equal(t, respWant.Error, response.Error)
			require.Equal(t, respWant.Val, response.Val)

		}(req, tc.respWant)
	}

	wg.Wait()
}
