package normalize

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

func validBlock() *streamforgev1.ChainEvent {
	return &streamforgev1.ChainEvent{
		Id:          "evt:ethereum:0xblock",
		Chain:       "ethereum",
		Type:        streamforgev1.EventType_EVENT_TYPE_BLOCK,
		BlockNumber: 100,
		BlockHash:   "0xblock",
		BlockTime:   timestamppb.New(time.Unix(1_700_000_000, 0)),
	}
}

func validTx() *streamforgev1.ChainEvent {
	ev := validBlock()
	ev.Id = "evt:ethereum:0xtx"
	ev.Type = streamforgev1.EventType_EVENT_TYPE_TRANSACTION
	ev.TxHash = "0xtx"
	ev.FromAddress = "0xFROM"
	ev.ToAddress = "0xTO"
	return ev
}

func TestValidate_ClassifiableViaErrorsIs(t *testing.T) {
	cases := []struct {
		name string
		ev   *streamforgev1.ChainEvent
		want error
	}{
		{"missing id", func() *streamforgev1.ChainEvent { e := validBlock(); e.Id = ""; return e }(), ErrMissingID},
		{"missing chain", func() *streamforgev1.ChainEvent { e := validBlock(); e.Chain = ""; return e }(), ErrMissingChain},
		{"unspecified type", func() *streamforgev1.ChainEvent {
			e := validBlock()
			e.Type = streamforgev1.EventType_EVENT_TYPE_UNSPECIFIED
			return e
		}(), ErrUnspecifiedType},
		{"missing block_time", func() *streamforgev1.ChainEvent { e := validBlock(); e.BlockTime = nil; return e }(), ErrMissingBlockTime},
		{"transaction missing tx_hash", func() *streamforgev1.ChainEvent { e := validTx(); e.TxHash = ""; return e }(), ErrMissingTxHash},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.ev)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Validate() = %v, want errors.Is(_, %v)", err, tc.want)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		ev      *streamforgev1.ChainEvent
		wantErr bool
	}{
		{"valid block", validBlock(), false},
		{"valid transaction", validTx(), false},
		{"missing id", func() *streamforgev1.ChainEvent { e := validBlock(); e.Id = ""; return e }(), true},
		{"missing chain", func() *streamforgev1.ChainEvent { e := validBlock(); e.Chain = ""; return e }(), true},
		{"unspecified type", func() *streamforgev1.ChainEvent {
			e := validBlock()
			e.Type = streamforgev1.EventType_EVENT_TYPE_UNSPECIFIED
			return e
		}(), true},
		{"missing block_time", func() *streamforgev1.ChainEvent { e := validBlock(); e.BlockTime = nil; return e }(), true},
		{"transaction missing tx_hash", func() *streamforgev1.ChainEvent { e := validTx(); e.TxHash = ""; return e }(), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.ev)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestEnrich_LowercasesAddressesWithoutMutatingInput(t *testing.T) {
	ev := validTx()
	out := Enrich(ev)

	if ev.FromAddress != "0xFROM" || ev.ToAddress != "0xTO" {
		t.Fatalf("Enrich mutated its input: from=%q to=%q", ev.FromAddress, ev.ToAddress)
	}
	if out.FromAddress != "0xfrom" || out.ToAddress != "0xto" {
		t.Fatalf("Enrich() addresses = %q/%q, want lowercase", out.FromAddress, out.ToAddress)
	}
}

func TestEnrich_LabelsKnownContractAsContractCall(t *testing.T) {
	ev := validTx()
	ev.ToAddress = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48" // USDC, mixed case

	out := Enrich(ev)

	if out.Attributes["contract_label"] != "USDC" {
		t.Errorf("contract_label = %q, want USDC", out.Attributes["contract_label"])
	}
	if out.Attributes["tx_kind"] != "contract_call" {
		t.Errorf("tx_kind = %q, want contract_call", out.Attributes["tx_kind"])
	}
}

func TestEnrich_UnknownAddressIsPlainTransfer(t *testing.T) {
	ev := validTx()
	ev.ToAddress = "0x000000000000000000000000000000deadbeef"

	out := Enrich(ev)

	if _, ok := out.Attributes["contract_label"]; ok {
		t.Errorf("contract_label unexpectedly set: %v", out.Attributes)
	}
	if out.Attributes["tx_kind"] != "transfer" {
		t.Errorf("tx_kind = %q, want transfer", out.Attributes["tx_kind"])
	}
}

func TestEnrich_BlockEventGetsNoTxKind(t *testing.T) {
	out := Enrich(validBlock())
	if _, ok := out.Attributes["tx_kind"]; ok {
		t.Errorf("tx_kind unexpectedly set on a block event: %v", out.Attributes)
	}
}
