package main

import (
	"errors"
	"net"
	"sync"
	"time"
)

const (
	PingMessage                  = 9
	BinaryMessage                = 2
	maxControlFramePayloadSize   = 125
)

var (
	errBadWriteOpCode      = errors.New("websocket: bad write opcode")
	errInvalidControlFrame = errors.New("websocket: invalid control frame")
)

func isControl(messageType int) bool {
	return messageType == PingMessage
}

type Conn struct {
	conn    net.Conn
	writeMu sync.Mutex
}

func newConn(conn net.Conn, isServer bool, readBufferSize, writeBufferSize int) *Conn {
	return &Conn{
		conn: conn,
	}
}

func (c *Conn) WriteControl(messageType int, data []byte, deadline time.Time) error {
	if !isControl(messageType) {
		return errBadWriteOpCode
	}
	if len(data) > maxControlFramePayloadSize {
		return errInvalidControlFrame
	}

	if err := c.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	return c.writeFrame(messageType, true, data)
}

func (c *Conn) WriteMessage(messageType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	_, err := c.conn.Write(data)
	return err
}

func (c *Conn) writeFrame(messageType int, final bool, data []byte) error {
	_, err := c.conn.Write(data)
	return err
}
