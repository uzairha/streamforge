package source

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/types/known/timestamppb"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

// backoffResetAfter is how long a connection must stay up before a
// subsequent drop is treated as a fresh failure rather than a continuation of
// the current backoff run.
const backoffResetAfter = 60 * time.Second

// Hooks are optional observability callbacks a caller wires to metrics; any
// field left nil is simply not invoked.
type Hooks struct {
	// OnReconnect fires before each reconnect attempt (not the first dial).
	OnReconnect func()
	// OnDedup fires once per event suppressed as a duplicate.
	OnDedup func()
}

// Ethereum streams ChainEvents from a live Ethereum JSON-RPC-over-WebSocket
// endpoint. It subscribes to newHeads for block events and, when configured,
// fetches each block's body to emit one event per transaction too. Connection
// drops are retried with exponential backoff and full jitter; events already
// seen — a resubscribe replaying the current head, or a duplicate push from
// the endpoint — are suppressed by a bounded dedup window.
type Ethereum struct {
	wsURL       string
	maxBackoff  time.Duration
	fetchBodies bool
	dedup       *seenSet
	hooks       Hooks
	dialer      *websocket.Dialer
}

// NewEthereum builds a live source against wsURL (e.g.
// "wss://ethereum.publicnode.com"). fetchBodies controls whether
// transaction-level events are emitted alongside block events; dedupWindow
// bounds how many recent event IDs are retained for duplicate suppression.
func NewEthereum(wsURL string, maxBackoff time.Duration, fetchBodies bool, dedupWindow int, hooks Hooks) *Ethereum {
	return &Ethereum{
		wsURL:       wsURL,
		maxBackoff:  maxBackoff,
		fetchBodies: fetchBodies,
		dedup:       newSeenSet(dedupWindow),
		hooks:       hooks,
		dialer:      websocket.DefaultDialer,
	}
}

// Name implements Source.
func (e *Ethereum) Name() string { return "ethereum" }

// Run implements Source. It never returns a non-nil error for a dropped
// connection — that is exactly the condition it reconnects through — only for
// context cancellation (nil) is expected; a persistently unreachable endpoint
// simply keeps retrying until ctx is cancelled.
func (e *Ethereum) Run(ctx context.Context, out chan<- *streamforgev1.ChainEvent) error {
	defer close(out)

	bo := newBackoff(e.maxBackoff)
	first := true

	for {
		if ctx.Err() != nil {
			return nil
		}
		if !first {
			if e.hooks.OnReconnect != nil {
				e.hooks.OnReconnect()
			}
			if !bo.sleep(ctx) {
				return nil
			}
		}
		first = false

		conn, resp, dialErr := e.dialer.DialContext(ctx, e.wsURL, nil)
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		if dialErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			continue // next loop iteration reconnects with a grown backoff
		}

		connectedAt := time.Now()
		_ = e.runConnection(ctx, conn, out)
		_ = conn.Close()

		if ctx.Err() != nil {
			return nil
		}
		if time.Since(connectedAt) >= backoffResetAfter {
			bo.reset()
		}
	}
}

// runConnection subscribes to newHeads and forwards decoded events to out
// until the connection fails or ctx is cancelled. A nil return means a clean,
// context-driven exit; any other return (including a subscribe failure) means
// the caller should reconnect.
func (e *Ethereum) runConnection(ctx context.Context, conn *websocket.Conn, out chan<- *streamforgev1.ChainEvent) error {
	rc := newRPCConn(conn)
	defer close(rc.stop)

	readErr := make(chan error, 1)
	go func() { readErr <- rc.readPump() }()

	if _, err := rc.call(ctx, "eth_subscribe", []any{"newHeads"}); err != nil {
		return fmt.Errorf("eth_subscribe: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-readErr:
			return err
		case note, ok := <-rc.notifications:
			if !ok {
				return <-readErr
			}
			if err := e.handleHead(ctx, rc, note.Result, out); err != nil {
				return err
			}
		}
	}
}

// ethHeader is the subset of an eth_subscribe("newHeads") push this source
// needs.
type ethHeader struct {
	Number    string `json:"number"`
	Hash      string `json:"hash"`
	Timestamp string `json:"timestamp"`
}

// ethBlock is the subset of an eth_getBlockByNumber(_, true) result this
// source needs.
type ethBlock struct {
	Transactions []ethTx `json:"transactions"`
}

type ethTx struct {
	Hash  string `json:"hash"`
	From  string `json:"from"`
	To    string `json:"to"` // empty for a contract-creation transaction
	Value string `json:"value"`
}

func (e *Ethereum) handleHead(ctx context.Context, rc *rpcConn, raw json.RawMessage, out chan<- *streamforgev1.ChainEvent) error {
	var h ethHeader
	if err := json.Unmarshal(raw, &h); err != nil {
		return fmt.Errorf("decode head: %w", err)
	}
	blockNum, err := hexToUint64(h.Number)
	if err != nil {
		return fmt.Errorf("decode head number %q: %w", h.Number, err)
	}

	observed := time.Now()
	blockTime := observed
	if t, terr := hexToUnixTime(h.Timestamp); terr == nil {
		blockTime = t
	}

	blockEvt := &streamforgev1.ChainEvent{
		Id:          "evt:ethereum:" + h.Hash,
		Chain:       "ethereum",
		Type:        streamforgev1.EventType_EVENT_TYPE_BLOCK,
		BlockNumber: blockNum,
		BlockHash:   h.Hash,
		ObservedAt:  timestamppb.New(observed),
		BlockTime:   timestamppb.New(blockTime),
		Attributes:  map[string]string{},
	}
	if !e.emit(ctx, blockEvt, out) {
		return ctx.Err()
	}
	if !e.fetchBodies {
		return nil
	}

	result, err := rc.call(ctx, "eth_getBlockByNumber", []any{h.Number, true})
	if err != nil {
		return fmt.Errorf("eth_getBlockByNumber(%s): %w", h.Number, err)
	}
	var block ethBlock
	if err := json.Unmarshal(result, &block); err != nil {
		return fmt.Errorf("decode block body: %w", err)
	}

	for _, tx := range block.Transactions {
		valueWei, verr := hexToDecimalString(tx.Value)
		if verr != nil {
			valueWei = "0"
		}
		txEvt := &streamforgev1.ChainEvent{
			Id:          "evt:ethereum:" + tx.Hash,
			Chain:       "ethereum",
			Type:        streamforgev1.EventType_EVENT_TYPE_TRANSACTION,
			BlockNumber: blockNum,
			BlockHash:   h.Hash,
			TxHash:      tx.Hash,
			FromAddress: tx.From,
			ToAddress:   tx.To,
			ValueWei:    valueWei,
			// GasUsed is left at 0: getting the real value needs a
			// per-transaction eth_getTransactionReceipt call, which would
			// double the RPC load against a keyless, rate-limited public
			// endpoint. Wire it up if a later milestone's aggregation needs
			// it (e.g. by batching receipt fetches).
			ObservedAt: timestamppb.New(observed),
			BlockTime:  timestamppb.New(blockTime),
			Attributes: map[string]string{},
		}
		if !e.emit(ctx, txEvt, out) {
			return ctx.Err()
		}
	}
	return nil
}

// emit applies dedup and forwards ev to out, returning false only when ctx is
// cancelled before the send completes.
func (e *Ethereum) emit(ctx context.Context, ev *streamforgev1.ChainEvent, out chan<- *streamforgev1.ChainEvent) bool {
	if e.dedup.seen(ev.GetId()) {
		if e.hooks.OnDedup != nil {
			e.hooks.OnDedup()
		}
		return true
	}
	select {
	case out <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

func hexToUint64(s string) (uint64, error) {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return 0, fmt.Errorf("empty hex value")
	}
	return strconv.ParseUint(s, 16, 64)
}

func hexToDecimalString(s string) (string, error) {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return "0", nil
	}
	n, ok := new(big.Int).SetString(s, 16)
	if !ok {
		return "", fmt.Errorf("invalid hex value %q", s)
	}
	return n.String(), nil
}

func hexToUnixTime(s string) (time.Time, error) {
	sec, err := hexToUint64(s)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(int64(sec), 0).UTC(), nil
}
