package network

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jainyk/distkv/proto"
	"google.golang.org/grpc"
)

// Client is a client for the distributed key-value store
type Client struct {
	serverAddr string
	conn       *grpc.ClientConn
	client     proto.KVStoreClient
}

// NewClient creates a new client
func NewClient(serverAddr string) (*Client, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %v", err)
	}

	client := proto.NewKVStoreClient(conn)

	return &Client{
		serverAddr: serverAddr,
		conn:       conn,
		client:     client,
	}, nil
}

// Get retrieves a value for a key
func (c *Client) Get(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.Get(ctx, &proto.GetRequest{Key: key})
	if err != nil {
		return "", fmt.Errorf("failed to get key: %v", err)
	}

	if !resp.Found {
		return "", errors.New("key not found")
	}

	return resp.Value, nil
}

// Put stores a key-value pair
func (c *Client) Put(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.Put(ctx, &proto.PutRequest{Key: key, Value: value})
	if err != nil {
		return fmt.Errorf("failed to put key: %v", err)
	}

	if !resp.Success {
		return errors.New("put operation failed")
	}

	return nil
}

// Delete removes a key-value pair
func (c *Client) Delete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.Delete(ctx, &proto.DeleteRequest{Key: key})
	if err != nil {
		return fmt.Errorf("failed to delete key: %v", err)
	}

	if !resp.Success {
		return errors.New("delete operation failed")
	}

	return nil
}

// Close closes the client connection
func (c *Client) Close() error {
	return c.conn.Close()
}
