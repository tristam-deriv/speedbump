package lib

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
    //"encoding/hex"
	"time"
	"github.com/hashicorp/go-hclog"
)

type transitBuffer struct {
	data       []byte
	delayUntil time.Time
}

type connection struct {
	srcConn, destConn io.ReadWriteCloser
	bufferSize        int
	latencyGen        LatencyGenerator
	delayQueueSrc     chan transitBuffer
	delayQueueDest    chan transitBuffer
	done              chan error
	ctx               context.Context
	log               hclog.Logger
}

func (c *connection) readFromSrc() {
	for {
		buffer := make([]byte, c.bufferSize)
		bytes, err := c.srcConn.Read(buffer)
		receivedAt := time.Now()
		if err != nil {
			c.done <- fmt.Errorf("Error reading data from client %s", err)
			return
		}
		trimmedBuffer := buffer[:bytes]
		desiredLatency := c.latencyGen.generateLatency(receivedAt)
		delayUntil := receivedAt.Add(desiredLatency)

		t := transitBuffer{
			data:       trimmedBuffer,
			delayUntil: delayUntil,
		}

		c.log.Trace("Writing to delay queue", "bytes", bytes, "delay", desiredLatency)
		//c.log.Warn("Writing to delay queue", "hex", hex.EncodeToString(buffer[0:bytes]))
		c.log.Warn("Writing to delay queue", "str", string(buffer[0:bytes]))

		c.delayQueueSrc <- t

	}
}

func (c *connection) readFromDest() {
	for {
		buffer := make([]byte, c.bufferSize)
		bytes, err := c.destConn.Read(buffer)
		receivedAt := time.Now()
		if err != nil {
			c.done <- fmt.Errorf("Error reading data from server %s", err)
			return
		}
		trimmedBuffer := buffer[:bytes]
		desiredLatency := c.latencyGen.generateLatency(receivedAt)
		delayUntil := receivedAt.Add(desiredLatency)

		t := transitBuffer{
			data:       trimmedBuffer,
			delayUntil: delayUntil,
		}

		c.log.Trace("Writing to dest delay queue", "bytes", bytes, "delay", desiredLatency)
		//c.log.Warn("Writing to delay queue", "hex", hex.EncodeToString(buffer[0:bytes]))
		c.log.Warn("Writing to dest delay queue", "str", string(buffer[0:bytes]))

		c.delayQueueDest <- t

	}
}

func (c *connection) readFromDelayQueueSrc() {
	for {
		t := <-c.delayQueueSrc

		c.log.Trace("Read from delay queue", "bytes", len(t.data))

		time.Sleep(time.Until(t.delayUntil))

		_, err := c.destConn.Write(t.data)
		if err != nil {
			c.done <- fmt.Errorf("Error writing from delay queue to proxy destination: %s", err)
			return
		}
	}
}

func (c *connection) readFromDelayQueueDest() {
	for {
		t := <-c.delayQueueDest

		c.log.Trace("Read from delay queueDest", "bytes", len(t.data))

		time.Sleep(time.Until(t.delayUntil))

		_, err := c.srcConn.Write(t.data)
		if err != nil {
			c.done <- fmt.Errorf("Error writing from delay queue to proxy Dest: %s", err)
			return
		}
	}
}

// start launches 3 goroutines responsible for handling a proxy connection
// (dest->src, src->queue, queue->dest). This operation will block until
// either an error is sent via the done channel or the context is cancelled.
func (c *connection) start() {
	c.log.Debug("Starting a new proxy connection")
	go c.readFromDest()
	go c.readFromSrc()
	go c.readFromDelayQueueSrc()
	go c.readFromDelayQueueDest()
	for {
		select {
		case err := <-c.done:
			c.handleError(err)
			return
		case <-c.ctx.Done():
			c.handleStop()
			return
		}
	}
}

func (c *connection) handleError(err error) {
	if !strings.HasSuffix(err.Error(), io.EOF.Error()) {
		c.log.Warn("Closing proxy connection due to an unexpected error", "err", err)
	} else {
		c.log.Debug("Closing proxy connection (EOF)")
	}
	c.closeProxyConnections()
}

func (c *connection) handleStop() {
	c.log.Info("Stopping proxy connection")
	c.closeProxyConnections()
}

func (c *connection) closeProxyConnections() {
	c.srcConn.Close()
	c.destConn.Close()
}

func newProxyConnection(
	ctx context.Context,
	clientConn io.ReadWriteCloser,
	srcAddr *net.TCPAddr,
	destAddr *net.TCPAddr,
	bufferSize int,
	queueSize int,
	latencyGen LatencyGenerator,
	logger hclog.Logger,
) (*connection, error) {
	destConn, err := net.DialTCP("tcp", nil, destAddr)
	if err != nil {
		return nil, fmt.Errorf("Error dialing remote address: %s", err)
	}
	c := &connection{
		srcConn:    clientConn,
		destConn:   destConn,
		bufferSize: bufferSize,
		latencyGen: latencyGen,
		delayQueueSrc: make(chan transitBuffer, queueSize),
		delayQueueDest: make(chan transitBuffer, queueSize),
		done:       make(chan error, 3),
		ctx:        ctx,
		log:        logger,
	}

	return c, nil
}
