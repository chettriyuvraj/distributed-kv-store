package distdbclient

import (
	"encoding/json"
	"errors"
	"net"

	"github.com/chettriyuvraj/distributed-kv-store/protobuf/github.com/chettriyuvraj/distributed-kv-store/communication"
	"google.golang.org/protobuf/proto"
)

const (
	SERVER_PROTOCOL = "tcp"
	SERVER_HOST     = "localhost"
	SERVER_PORT     = "3108"
)

var ErrInvalidOperation = errors.New("invalid operation")

type Client struct {
	serverConn net.Conn
}

func NewClient() (*Client, error) {
	serverConn, err := net.Dial(SERVER_PROTOCOL, SERVER_HOST+":"+SERVER_PORT)
	if err != nil {
		return nil, err
	}

	return &Client{serverConn: serverConn}, nil
}

func (c *Client) Get(key []byte) ([]byte, error) {
	req := communication.Request{Key: key, Op: communication.Operation_GET}
	err := c.MakeRequest(&req)
	if err != nil {
		return nil, err
	}

	respData, err := c.RcvResponse()
	if err != nil {
		return nil, err
	}

	var response communication.Response
	err = proto.Unmarshal(respData, &response)
	if err != nil {
		return nil, err
	}

	if response.Status == communication.Status_FAILURE {
		return nil, errors.New(response.Error)
	}

	return response.Val, err
}

func (c *Client) Put(key, val []byte) error {
	req := communication.Request{Key: key, Val: val, Op: communication.Operation_PUT}
	err := c.MakeRequest(&req)
	if err != nil {
		return err
	}

	respData, err := c.RcvResponse()
	if err != nil {
		return err
	}

	var response communication.Response
	err = json.Unmarshal(respData, &response)
	if err != nil {
		return err
	}

	if response.Status == communication.Status_FAILURE {
		return errors.New(response.Error)
	}

	return nil
}

func (c *Client) MakeRequest(req *communication.Request) error {
	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	return c.Send(data)
}

func (c *Client) RcvResponse() ([]byte, error) {
	buffer := make([]byte, 1024)
	respLen, err := c.serverConn.Read(buffer)
	if err != nil {
		return nil, err
	}
	response := buffer[:respLen]
	return response, nil
}

func (c *Client) Send(data []byte) error {
	_, err := c.serverConn.Write(data)
	if err != nil {
		return err
	}

	return nil
}
