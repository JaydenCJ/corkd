// Package client is a minimal Go client for the corkd protocol: dial the
// unix socket, exchange one JSON line per request/response, or hold the
// connection open as a watch stream. The CLI is built on it, and it is
// small enough to vendor into an agent wholesale.
package client

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/JaydenCJ/corkd/internal/proto"
)

// Client is one connection to a corkd server. Not safe for concurrent
// use; open one client per goroutine (connections are cheap — it's a
// local socket).
type Client struct {
	conn net.Conn
	r    *bufio.Reader
}

// Dial connects to the corkd server at socket path.
func Dial(socket string) (*Client, error) {
	conn, err := net.DialTimeout("unix", socket, 3*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connecting to %s: %w (is `corkd serve` running?)", socket, err)
	}
	return &Client{
		conn: conn,
		r:    bufio.NewReaderSize(conn, 64*1024),
	}, nil
}

// Close hangs up.
func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) send(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = c.conn.Write(append(b, '\n'))
	return err
}

func (c *Client) readLine(v any) error {
	line, err := c.r.ReadBytes('\n')
	if err != nil {
		return err
	}
	if len(line) > proto.MaxLineBytes {
		return errors.New("response line exceeds protocol limit")
	}
	return json.Unmarshal(line, v)
}

// Do sends one request and reads its response. A transport failure is
// returned as an error; a protocol-level failure arrives as an ok=false
// Response with a nil error, since those are ordinary outcomes (missing
// key, CAS conflict, timeout) the caller must branch on.
func (c *Client) Do(req proto.Request) (proto.Response, error) {
	if err := c.send(req); err != nil {
		return proto.Response{}, fmt.Errorf("sending request: %w", err)
	}
	var resp proto.Response
	if err := c.readLine(&resp); err != nil {
		return proto.Response{}, fmt.Errorf("reading response: %w", err)
	}
	return resp, nil
}

// StartWatch switches the connection into a watch stream; from here on
// only NextEvent may be used. prefix "" watches everything; withState
// replays current entries (then a sync event) before live events.
func (c *Client) StartWatch(prefix string, withState bool) error {
	return c.send(proto.Request{Op: proto.OpWatch, Prefix: prefix, State: withState})
}

// NextEvent reads the next event from a watch stream. It blocks until an
// event arrives or the stream ends (io.EOF once the server hangs up).
func (c *Client) NextEvent() (proto.Event, error) {
	var ev proto.Event
	if err := c.readLine(&ev); err != nil {
		return proto.Event{}, err
	}
	return ev, nil
}
