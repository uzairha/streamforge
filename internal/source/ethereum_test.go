package source

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

// fakeEthServer is a minimal Ethereum JSON-RPC-over-WebSocket endpoint for
// exercising Ethereum's subscribe/reconnect/dedup behavior without a real
// chain: it answers eth_subscribe and eth_getBlockByNumber, and lets the test
// push newHeads notifications on demand.
type fakeEthServer struct {
	upgrader websocket.Upgrader
	srv      *httptest.Server

	mu     sync.Mutex
	blocks map[string]ethBlock // hex block number -> body

	// writeMu serializes every WebSocket write this server makes: gorilla's
	// Conn requires a single writer, and reply() (from the per-connection
	// read loop) and pushHead() (called directly by a test) can otherwise
	// race on the same connection.
	writeMu sync.Mutex

	conns       int32
	onConnected func(conn *websocket.Conn, connNum int32)
}

func newFakeEthServer(t *testing.T) *fakeEthServer {
	t.Helper()
	f := &fakeEthServer{blocks: make(map[string]ethBlock)}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeEthServer) wsURL() string {
	return "ws" + strings.TrimPrefix(f.srv.URL, "http")
}

func (f *fakeEthServer) handle(w http.ResponseWriter, r *http.Request) {
	conn, err := f.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	connNum := atomic.AddInt32(&f.conns, 1)
	if f.onConnected != nil {
		f.onConnected(conn, connNum)
	}

	for {
		_, data, rerr := conn.ReadMessage()
		if rerr != nil {
			return
		}
		var req struct {
			ID     int64           `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if jerr := json.Unmarshal(data, &req); jerr != nil {
			continue
		}
		switch req.Method {
		case "eth_subscribe":
			f.reply(conn, req.ID, json.RawMessage(`"0xsub1"`))
		case "eth_getBlockByNumber":
			var params []json.RawMessage
			_ = json.Unmarshal(req.Params, &params)
			var num string
			if len(params) > 0 {
				_ = json.Unmarshal(params[0], &num)
			}
			f.mu.Lock()
			block := f.blocks[num]
			f.mu.Unlock()
			body, _ := json.Marshal(block)
			f.reply(conn, req.ID, body)
		default:
			f.reply(conn, req.ID, json.RawMessage(`null`))
		}
	}
}

func (f *fakeEthServer) reply(conn *websocket.Conn, id int64, result json.RawMessage) {
	body, _ := json.Marshal(struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      int64           `json:"id"`
		Result  json.RawMessage `json:"result"`
	}{"2.0", id, result})
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	_ = conn.WriteMessage(websocket.TextMessage, body)
}

// pushHead sends an eth_subscription newHeads notification on conn.
func (f *fakeEthServer) pushHead(conn *websocket.Conn, h ethHeader) {
	hdr, _ := json.Marshal(h)
	note, _ := json.Marshal(struct {
		Subscription string          `json:"subscription"`
		Result       json.RawMessage `json:"result"`
	}{"0xsub1", hdr})
	body, _ := json.Marshal(struct {
		JSONRPC string          `json:"jsonrpc"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
	}{"2.0", "eth_subscription", note})
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	_ = conn.WriteMessage(websocket.TextMessage, body)
}

func recvEvent(t *testing.T, out <-chan *streamforgev1.ChainEvent) *streamforgev1.ChainEvent {
	t.Helper()
	select {
	case ev := <-out:
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
		return nil
	}
}

func TestEthereum_Name(t *testing.T) {
	e := NewEthereum("ws://example.invalid", time.Second, true, 64, Hooks{})
	if e.Name() != "ethereum" {
		t.Fatalf("Name() = %q, want ethereum", e.Name())
	}
}

func TestEthereum_EmitsBlockAndTransactionEvents(t *testing.T) {
	f := newFakeEthServer(t)
	f.blocks["0x1"] = ethBlock{Transactions: []ethTx{
		{Hash: "0xtx1", From: "0xfrom", To: "0xto", Value: "0xde0b6b3a7640000"}, // 1e18 wei
	}}
	connected := make(chan *websocket.Conn, 1)
	f.onConnected = func(conn *websocket.Conn, _ int32) { connected <- conn }

	e := NewEthereum(f.wsURL(), time.Second, true, 64, Hooks{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan *streamforgev1.ChainEvent, 16)
	errc := make(chan error, 1)
	go func() { errc <- e.Run(ctx, out) }()

	conn := <-connected
	f.pushHead(conn, ethHeader{Number: "0x1", Hash: "0xblockhash1", Timestamp: "0x64000000"})

	blockEvt := recvEvent(t, out)
	if blockEvt.GetType() != streamforgev1.EventType_EVENT_TYPE_BLOCK {
		t.Fatalf("first event type = %v, want BLOCK", blockEvt.GetType())
	}
	if blockEvt.GetBlockHash() != "0xblockhash1" || blockEvt.GetBlockNumber() != 1 {
		t.Fatalf("block event = %+v", blockEvt)
	}

	txEvt := recvEvent(t, out)
	if txEvt.GetType() != streamforgev1.EventType_EVENT_TYPE_TRANSACTION {
		t.Fatalf("second event type = %v, want TRANSACTION", txEvt.GetType())
	}
	if txEvt.GetTxHash() != "0xtx1" || txEvt.GetFromAddress() != "0xfrom" || txEvt.GetToAddress() != "0xto" {
		t.Fatalf("tx event = %+v", txEvt)
	}
	if txEvt.GetValueWei() != "1000000000000000000" {
		t.Fatalf("ValueWei = %q, want 1000000000000000000", txEvt.GetValueWei())
	}

	cancel()
	if err := <-errc; err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
}

func TestEthereum_SkipsBodyFetchWhenDisabled(t *testing.T) {
	f := newFakeEthServer(t)
	f.blocks["0x1"] = ethBlock{Transactions: []ethTx{{Hash: "0xtx1", From: "0xfrom", To: "0xto", Value: "0x1"}}}
	connected := make(chan *websocket.Conn, 1)
	f.onConnected = func(conn *websocket.Conn, _ int32) { connected <- conn }

	e := NewEthereum(f.wsURL(), time.Second, false, 64, Hooks{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan *streamforgev1.ChainEvent, 16)
	errc := make(chan error, 1)
	go func() { errc <- e.Run(ctx, out) }()

	conn := <-connected
	f.pushHead(conn, ethHeader{Number: "0x1", Hash: "0xblockhash1", Timestamp: "0x64000000"})

	ev := recvEvent(t, out)
	if ev.GetType() != streamforgev1.EventType_EVENT_TYPE_BLOCK {
		t.Fatalf("event type = %v, want BLOCK", ev.GetType())
	}
	select {
	case extra := <-out:
		t.Fatalf("got unexpected extra event with body fetch disabled: %+v", extra)
	case <-time.After(150 * time.Millisecond):
	}

	cancel()
	<-errc
}

func TestEthereum_DedupsRepeatedHead(t *testing.T) {
	f := newFakeEthServer(t)
	f.blocks["0x1"] = ethBlock{}
	connected := make(chan *websocket.Conn, 1)
	f.onConnected = func(conn *websocket.Conn, _ int32) { connected <- conn }

	var deduped int32
	e := NewEthereum(f.wsURL(), time.Second, true, 64, Hooks{OnDedup: func() { atomic.AddInt32(&deduped, 1) }})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan *streamforgev1.ChainEvent, 16)
	errc := make(chan error, 1)
	go func() { errc <- e.Run(ctx, out) }()

	conn := <-connected
	head := ethHeader{Number: "0x1", Hash: "0xblockhash1", Timestamp: "0x64000000"}
	f.pushHead(conn, head)
	f.pushHead(conn, head) // duplicate

	first := recvEvent(t, out)
	if first.GetBlockHash() != "0xblockhash1" {
		t.Fatalf("first event = %+v", first)
	}
	select {
	case ev := <-out:
		t.Fatalf("got a second event for a duplicate head: %+v", ev)
	case <-time.After(200 * time.Millisecond):
	}
	if got := atomic.LoadInt32(&deduped); got != 1 {
		t.Fatalf("OnDedup fired %d times, want 1", got)
	}

	cancel()
	<-errc
}

func TestEthereum_ReconnectsAfterDrop(t *testing.T) {
	f := newFakeEthServer(t)
	f.blocks["0x1"] = ethBlock{}
	connected := make(chan *websocket.Conn, 4)
	f.onConnected = func(conn *websocket.Conn, _ int32) { connected <- conn }

	var reconnects int32
	e := NewEthereum(f.wsURL(), 50*time.Millisecond, true, 64, Hooks{OnReconnect: func() { atomic.AddInt32(&reconnects, 1) }})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan *streamforgev1.ChainEvent, 16)
	errc := make(chan error, 1)
	go func() { errc <- e.Run(ctx, out) }()

	conn1 := <-connected
	f.pushHead(conn1, ethHeader{Number: "0x1", Hash: "0xblockhash1", Timestamp: "0x64000000"})
	recvEvent(t, out)
	conn1.Close() // simulate a drop

	conn2 := <-connected
	f.pushHead(conn2, ethHeader{Number: "0x1", Hash: "0xblockhash2", Timestamp: "0x64000001"})
	ev := recvEvent(t, out)
	if ev.GetBlockHash() != "0xblockhash2" {
		t.Fatalf("event after reconnect = %+v", ev)
	}

	cancel()
	<-errc

	if atomic.LoadInt32(&reconnects) == 0 {
		t.Fatal("OnReconnect never fired across a reconnect")
	}
}

func TestEthereum_ClosesChannelOnCancelEvenWhenUnreachable(t *testing.T) {
	// Port 1 on loopback is reserved/unlisted, so the dial fails immediately
	// without a DNS round trip, keeping this deterministic and fast.
	e := NewEthereum("ws://127.0.0.1:1", time.Millisecond, true, 64, Hooks{})
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan *streamforgev1.ChainEvent)
	errc := make(chan error, 1)
	go func() { errc <- e.Run(ctx, out) }()

	time.Sleep(20 * time.Millisecond) // let it fail-retry a couple times
	cancel()

	done := make(chan struct{})
	go func() {
		for range out { //nolint:revive // intentional drain
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("channel not closed within 2s of cancel")
	}
	if err := <-errc; err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
}

func TestHexToUint64(t *testing.T) {
	for in, want := range map[string]uint64{"0x0": 0, "0x1": 1, "0xff": 255} {
		got, err := hexToUint64(in)
		if err != nil {
			t.Fatalf("hexToUint64(%q): %v", in, err)
		}
		if got != want {
			t.Errorf("hexToUint64(%q) = %d, want %d", in, got, want)
		}
	}
	if _, err := hexToUint64(""); err == nil {
		t.Error("hexToUint64(\"\") succeeded, want error")
	}
	if _, err := hexToUint64("0xzz"); err == nil {
		t.Error("hexToUint64(\"0xzz\") succeeded, want error")
	}
}

func TestHexToDecimalString(t *testing.T) {
	got, err := hexToDecimalString("0xde0b6b3a7640000")
	if err != nil {
		t.Fatalf("hexToDecimalString: %v", err)
	}
	if got != "1000000000000000000" {
		t.Errorf("got %q, want 1000000000000000000", got)
	}
	if got, err := hexToDecimalString(""); err != nil || got != "0" {
		t.Errorf("hexToDecimalString(\"\") = (%q, %v), want (\"0\", nil)", got, err)
	}
	if _, err := hexToDecimalString("0xzz"); err == nil {
		t.Error("hexToDecimalString(\"0xzz\") succeeded, want error")
	}
}

func TestHexToUnixTime(t *testing.T) {
	got, err := hexToUnixTime("0x64000000")
	if err != nil {
		t.Fatalf("hexToUnixTime: %v", err)
	}
	want := time.Unix(0x64000000, 0).UTC()
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
