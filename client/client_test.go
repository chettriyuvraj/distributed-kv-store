package client

import (
	"encoding/json"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClientMakeRequest(t *testing.T) {
	/* Create a dummy server to which we will send our request to */
	server, err := net.Listen(SERVER_PROTOCOL, SERVER_HOST+":"+SERVER_PORT)
	require.NoError(t, err)
	defer server.Close()

	/* Create client */
	client, err := NewClient()
	require.NoError(t, err)

	/* Accept at server */
	clientConn, err := server.Accept()
	require.NoError(t, err)

	tcs := []struct {
		req     Request
		errWant error
	}{
		{req: Request{Key: []byte("key1"), Val: []byte("val1"), Op: "GET"}, errWant: nil},
		{req: Request{Key: []byte("key2"), Val: []byte("val2"), Op: "PUT"}, errWant: nil},
		{req: Request{Key: []byte("key3"), Val: []byte("val3"), Op: "INVALIDOP"}, errWant: ErrInvalidOperation},
	}

	for _, tc := range tcs {
		/* Send request from client */
		req := tc.req

		reqAsJSON, err := json.Marshal(req)
		require.NoError(t, err)

		err = client.MakeRequest(req.Key, req.Val, req.Op)
		if tc.errWant != nil {
			require.ErrorIs(t, err, tc.errWant)
			continue
		} else {
			require.NoError(t, err)
		}

		/* Check if both match */
		buffer := make([]byte, 1024)
		clientMessageLen, err := clientConn.Read(buffer)
		require.NoError(t, err)
		clientMessage := buffer[:clientMessageLen]
		require.Equal(t, clientMessage, reqAsJSON)
	}

}
