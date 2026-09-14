// Package normalize turns a raw, decoded ChainEvent into one safe to publish
// downstream: Validate rejects malformed events before anything else touches
// them, and Enrich derives attributes (contract labels, transaction kind)
// that the aggregator and API can rely on being present.
package normalize

import (
	"errors"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

// Sentinel reasons a Validate call can fail for, so callers can classify a
// failure (e.g. into a low-cardinality metric label) with errors.Is instead
// of matching on the error's text.
var (
	ErrMissingID        = errors.New("normalize: missing id")
	ErrMissingChain     = errors.New("normalize: missing chain")
	ErrUnspecifiedType  = errors.New("normalize: unspecified event type")
	ErrMissingBlockTime = errors.New("normalize: missing block_time")
	ErrMissingTxHash    = errors.New("normalize: transaction event missing tx_hash")
)

// knownContracts maps well-known Ethereum mainnet contract addresses
// (lowercase) to a human label, used to enrich a matching to_address.
var knownContracts = map[string]string{
	"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "USDC",
	"0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "WETH",
	"0xdac17f958d2ee523a2206206994597c13d831ec7": "USDT",
	"0x6b175474e89094c44da98b954eedeac495271d0f": "DAI",
	"0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": "WBTC",
}

// Validate reports whether ev is well-formed enough to enrich and publish. It
// never mutates ev. A non-nil error should be counted and the event dropped
// rather than retried — retrying can't fix a malformed record.
func Validate(ev *streamforgev1.ChainEvent) error {
	if ev.GetId() == "" {
		return ErrMissingID
	}
	if ev.GetChain() == "" {
		return fmt.Errorf("%w (id=%s)", ErrMissingChain, ev.GetId())
	}
	if ev.GetType() == streamforgev1.EventType_EVENT_TYPE_UNSPECIFIED {
		return fmt.Errorf("%w (id=%s)", ErrUnspecifiedType, ev.GetId())
	}
	if ev.GetBlockTime() == nil {
		return fmt.Errorf("%w (id=%s)", ErrMissingBlockTime, ev.GetId())
	}
	if ev.GetType() == streamforgev1.EventType_EVENT_TYPE_TRANSACTION && ev.GetTxHash() == "" {
		return fmt.Errorf("%w (id=%s)", ErrMissingTxHash, ev.GetId())
	}
	return nil
}

// Enrich returns a normalized copy of ev; the input is never mutated.
// Addresses are lowercased for consistent joins/aggregation, and attributes
// gains a contract_label when to_address matches a known contract, plus a
// tx_kind ("contract_call" or "transfer") for transaction events.
func Enrich(ev *streamforgev1.ChainEvent) *streamforgev1.ChainEvent {
	out, _ := proto.Clone(ev).(*streamforgev1.ChainEvent)

	out.FromAddress = strings.ToLower(out.FromAddress)
	out.ToAddress = strings.ToLower(out.ToAddress)

	label, isContract := knownContracts[out.ToAddress]

	if label != "" || out.Type == streamforgev1.EventType_EVENT_TYPE_TRANSACTION {
		if out.Attributes == nil {
			out.Attributes = make(map[string]string, 2)
		}
	}
	if label != "" {
		out.Attributes["contract_label"] = label
	}
	if out.Type == streamforgev1.EventType_EVENT_TYPE_TRANSACTION && out.ToAddress != "" {
		if isContract {
			out.Attributes["tx_kind"] = "contract_call"
		} else {
			out.Attributes["tx_kind"] = "transfer"
		}
	}
	return out
}
