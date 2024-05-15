package distdb

import (
	"sync"
	"testing"

	"github.com/chettriyuvraj/distributed-kv-store/distdbclient"
	"github.com/chettriyuvraj/distributed-kv-store/protobuf/github.com/chettriyuvraj/distributed-kv-store/communication"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const (
	DEFAULT_SERVER_PROTOCOL = "tcp"
	DEFAULT_SERVER_HOST     = "localhost"
	DEFAULT_SERVER_PORT     = "3108"

	DEFAULT_REPLICA_PROTOCOL = "tcp"
	DEFAULT_REPLICA_HOST     = "localhost"
	DEFAULT_REPLICA_PORT     = "3109"
)

func TestGetPut(t *testing.T) {
	config := DBConfig{Persist: false, Role: LEADER}

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
	/* Define test case struct and create a 'verify' function */
	type testcase struct {
		req      *communication.Request
		respWant *communication.Response
	}
	verifyReqResp := func(req *communication.Request, respWant *communication.Response) {
		client, err := distdbclient.NewClient(distdbclient.ClientConfig{ServerProtocol: DEFAULT_SERVER_PROTOCOL, ServerHost: DEFAULT_SERVER_HOST, ServerPort: DEFAULT_SERVER_PORT})
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

	}

	/* Execute tests */

	/* Create server and start listening */
	dbConfig := DBConfig{Persist: false, Role: LEADER, ServerProtocol: DEFAULT_SERVER_PROTOCOL, ServerHost: DEFAULT_SERVER_HOST, ServerPort: DEFAULT_SERVER_PORT}
	db, err := NewDB(dbConfig)
	require.NoError(t, err)

	go db.Listen()

	/* Start a new goroutine, with a new client for each request - TODO: error cases */
	tcs := []testcase{
		{req: &communication.Request{Key: []byte("key2"), Val: []byte("val2"), Op: communication.Operation_PUT}, respWant: &communication.Response{Status: communication.Status_SUCCESS, Error: "", Val: nil}},
		{req: &communication.Request{Key: []byte("key1"), Op: communication.Operation_GET}, respWant: &communication.Response{Status: communication.Status_FAILURE, Error: ErrKeyDoesNotExist.Error(), Val: nil}},
	}

	/* Execute tcs parallely - Wait Group to ensure test completes only when all goroutines finish */
	var wg sync.WaitGroup
	for _, tc := range tcs {
		wg.Add(1)
		req := tc.req
		go func(req *communication.Request, respWant *communication.Response) {
			defer wg.Done()
			verifyReqResp(req, respWant)
		}(req, tc.respWant)
	}
	wg.Wait()

	/* Check if put operation is recognized by corresponding get */
	putTc := tcs[0]
	verifyReqResp(putTc.req, putTc.respWant)

}

/* Verifying if put requests to leader are replicated all the way to followers */
func TestReplicationChain(t *testing.T) {
	tcs := []struct {
		k, v []byte
	} {
		{k: []byte("k1"), v: []byte("v1")},
		{k: []byte("k2"), v: []byte("v2")},
		{k: []byte("k3"), v: []byte("v3")},
	}

	/* Intiialize db and replica(s), start listening */
	dbConfig := DBConfig{Persist: false, Role: LEADER, 
		ServerProtocol: DEFAULT_SERVER_PROTOCOL, ServerHost: DEFAULT_SERVER_HOST, ServerPort: "3110",
		ReplicaConfigs: []distdbclient.ClientConfig {
			distdbclient.ClientConfig{ServerProtocol: DEFAULT_REPLICA_PROTOCOL, ServerHost: DEFAULT_REPLICA_HOST, ServerPort: DEFAULT_REPLICA_PORT},
		},
	}
	db, err := NewDB(dbConfig)
	require.NoError(t, err)

	go func() {
		err := db.Listen()
		require.NoError(t, err)
	}()

	/* Make put requests using client */
	client, err := distdbclient.NewClient(distdbclient.ClientConfig{ServerProtocol: DEFAULT_SERVER_PROTOCOL, ServerHost: DEFAULT_SERVER_HOST, ServerPort: DEFAULT_SERVER_PORT})
	require.NoError(t, err)

	for _, tc := range tcs {
		req := &communication.Request{Key: tc.k, Val: tc.v, Op: communication.Operation_PUT}
		err := client.MakeRequest(&communication.Request{Key: req.Key, Val: req.Val, Op: req.Op})
		require.NoError(t, err)
	}
	
	/* Wait till all workers are done */
	for _, worker := range db.replicaWorkers {
		<- worker.done
	}

}


