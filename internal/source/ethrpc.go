package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
)

// rpcEnvelope is the shape common to every JSON-RPC 2.0 frame the connection
// can receive: either a response to a call we made (ID set) or a subscription
// push (Method == "eth_subscription").
type rpcEnvelope struct {
	ID     int64           `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message) }

// subscriptionNotification is params of an eth_subscription push.
type subscriptionNotification struct {
	Subscription string          `json:"subscription"`
	Result       json.RawMessage `json:"result"`
}

// rpcConn multiplexes a single JSON-RPC-over-WebSocket connection: request/
// response calls (eth_getBlockByNumber, ...) and the asynchronous
// eth_subscription push stream (newHeads) share one socket, so one reader
// goroutine demuxes both onto separate channels.
type rpcConn struct {
	conn *websocket.Conn

	writeMu sync.Mutex // WebSocket connections require a single writer

	pendingMu sync.Mutex
	pending   map[int64]chan rpcEnvelope

	notifications chan subscriptionNotification
	stop          chan struct{} // closed by the owner to unstick a blocked send
	nextID        int64
}

func newRPCConn(conn *websocket.Conn) *rpcConn {
	return &rpcConn{
		conn:          conn,
		pending:       make(map[int64]chan rpcEnvelope),
		notifications: make(chan subscriptionNotification, 64),
		stop:          make(chan struct{}),
	}
}

// readPump demuxes incoming frames until the connection errors, closes, or
// stop is closed by the owner. It must run in its own goroutine.
func (rc *rpcConn) readPump() error {
	defer close(rc.notifications)
	defer rc.failPending(errors.New("connection closed"))

	for {
		_, data, err := rc.conn.ReadMessage()
		if err != nil {
			return err
		}
		var env rpcEnvelope
		if err := json.Unmarshal(data, &env); err != nil {
			continue // ignore a frame we can't parse rather than kill the connection
		}
		if env.Method == "eth_subscription" {
			var note subscriptionNotification
			if err := json.Unmarshal(env.Params, &note); err != nil {
				continue
			}
			select {
			case rc.notifications <- note:
			case <-rc.stop:
				return nil
			}
			continue
		}
		if env.ID != 0 {
			rc.pendingMu.Lock()
			ch, ok := rc.pending[env.ID]
			delete(rc.pending, env.ID)
			rc.pendingMu.Unlock()
			if ok {
				ch <- env // buffered(1): never blocks
			}
		}
	}
}

// failPending resolves every outstanding call with err, so callers blocked in
// call() unblock instead of waiting forever once the connection is gone.
func (rc *rpcConn) failPending(err error) {
	rc.pendingMu.Lock()
	defer rc.pendingMu.Unlock()
	for id, ch := range rc.pending {
		ch <- rpcEnvelope{ID: id, Error: &rpcError{Message: err.Error()}}
		delete(rc.pending, id)
	}
}

// call sends a JSON-RPC request and waits for its matching response (or ctx
// cancellation, or the connection failing first).
func (rc *rpcConn) call(ctx context.Context, method string, params []any) (json.RawMessage, error) {
	id := atomic.AddInt64(&rc.nextID, 1)
	ch := make(chan rpcEnvelope, 1)

	rc.pendingMu.Lock()
	rc.pending[id] = ch
	rc.pendingMu.Unlock()

	body, err := json.Marshal(struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int64  `json:"id"`
		Method  string `json:"method"`
		Params  []any  `json:"params"`
	}{"2.0", id, method, params})
	if err != nil {
		rc.dropPending(id)
		return nil, fmt.Errorf("marshal %s request: %w", method, err)
	}

	rc.writeMu.Lock()
	writeErr := rc.conn.WriteMessage(websocket.TextMessage, body)
	rc.writeMu.Unlock()
	if writeErr != nil {
		rc.dropPending(id)
		return nil, fmt.Errorf("write %s request: %w", method, writeErr)
	}

	select {
	case env := <-ch:
		if env.Error != nil {
			return nil, fmt.Errorf("%s: %w", method, env.Error)
		}
		return env.Result, nil
	case <-ctx.Done():
		rc.dropPending(id)
		return nil, ctx.Err()
	}
}

func (rc *rpcConn) dropPending(id int64) {
	rc.pendingMu.Lock()
	delete(rc.pending, id)
	rc.pendingMu.Unlock()
}
