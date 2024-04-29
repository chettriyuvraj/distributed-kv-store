package distdbclient

import (
	"encoding/json"
	"errors"
	"net"
)

const (
	SERVER_PROTOCOL = "tcp"
	SERVER_HOST     = "localhost"
	SERVER_PORT     = "3108"
	GET             = "GET"
	PUT             = "PUT"
	SUCCESS         = "SUCCESS"
	FAILURE         = "FAILURE"
)

var ErrInvalidOperation = errors.New("invalid operation")
var requestTypes = map[string]bool{
	GET: true,
	PUT: true,
}

type Request struct {
	Key, Val []byte
	Op       string
}

type Response struct {
	Status, Error string
	Val           []byte
}

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
	err := c.MakeRequest(key, nil, GET)
	if err != nil {
		return nil, err
	}

	respData, err := c.RcvResponse()
	if err != nil {
		return nil, err
	}

	var response Response
	err = json.Unmarshal(respData, &response)
	if err != nil {
		return nil, err
	}

	if response.Status == FAILURE {
		return nil, errors.New(response.Error)
	}

	return response.Val, err
}

func (c *Client) Put(key, val []byte) error {
	err := c.MakeRequest(key, val, PUT)
	if err != nil {
		return err
	}

	respData, err := c.RcvResponse()
	if err != nil {
		return err
	}

	var response Response
	err = json.Unmarshal(respData, &response)
	if err != nil {
		return err
	}

	if response.Status == FAILURE {
		return errors.New(response.Error)
	}

	return nil
}

func (c *Client) MakeRequest(key, val []byte, op string) error {
	req := Request{Key: key, Val: val, Op: op}

	_, exists := requestTypes[op]
	if !exists {
		return ErrInvalidOperation
	}

	data, err := json.Marshal(req)
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
