package distdbclient

import (
	"net"
	"testing"

	"github.com/chettriyuvraj/distributed-kv-store/protobuf/github.com/chettriyuvraj/distributed-kv-store/communication"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const (
	DEFAULT_SERVER_PROTOCOL = "tcp"
	DEFAULT_SERVER_HOST     = "localhost"
	DEFAULT_SERVER_PORT     = "3108"
)

func TestClientMakeRequest(t *testing.T) {
	/* Create a dummy server to which we will send our request to */
	server, err := net.Listen(DEFAULT_SERVER_PROTOCOL, DEFAULT_SERVER_HOST+":"+DEFAULT_SERVER_PORT)
	require.NoError(t, err)
	defer server.Close()

	/* Create client */
	config := ClientConfig{ServerProtocol: DEFAULT_SERVER_PROTOCOL, ServerHost: DEFAULT_SERVER_HOST, ServerPort: DEFAULT_SERVER_PORT}
	client, err := NewClient(config)
	require.NoError(t, err)

	/* Accept at server */
	clientConn, err := server.Accept()
	require.NoError(t, err)

	tcs := []struct {
		req     *communication.Request
		errWant error
	}{
		{req: &communication.Request{Key: []byte("key1"), Val: []byte("val1"), Op: communication.Operation_GET}, errWant: nil},
		{req: &communication.Request{Key: []byte("key2"), Val: []byte("val2"), Op: communication.Operation_PUT}, errWant: nil},
	}

	for _, tc := range tcs {
		/* Send request from client */
		req := tc.req

		err = client.MakeRequest(req)
		if tc.errWant != nil {
			require.ErrorIs(t, err, tc.errWant)
			continue
		} else {
			require.NoError(t, err)
		}

		var clientRequestParsedAtServer communication.Request
		/* Receive response at server */
		buffer := make([]byte, 1024)
		clientRequestLen, err := clientConn.Read(buffer)
		require.NoError(t, err)
		clientRequestRcvdAtServer := buffer[:clientRequestLen]

		/* Unmarshal and check if all relevant fields match */
		err = proto.Unmarshal(clientRequestRcvdAtServer, &clientRequestParsedAtServer)
		require.NoError(t, err)
		require.Equal(t, req.Key, clientRequestParsedAtServer.Key)
		require.Equal(t, req.Val, clientRequestParsedAtServer.Val)
		require.Equal(t, req.Op, clientRequestParsedAtServer.Op)

	}

}

/* Note: ClientRcvResponse tested in db_test */
