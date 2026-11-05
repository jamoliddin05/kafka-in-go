package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	eventType   = "OrderPlaced"
	contentType = "application/json"
)

type Status string

const (
	StatusPending  Status = "pending"
	StatusPaid     Status = "paid"
	StatusRefunded Status = "refunded" // since v2
)

// Known reports whether this release knows what to do with s. Unknown
// values are kept as they came, so they can be logged and counted.
func (s Status) Known() bool {
	switch s {
	case StatusPending, StatusPaid, StatusRefunded:
		return true
	}
	return false
}

// Order is the model the code works with. It is not a wire format: every
// version on the wire is converted into it at the edge.
type Order struct {
	ID          string
	AmountMinor int64 // cents
	Currency    string
	Status      Status
}

// Envelope is the metadata in the record headers. The payload alone can't
// tell you what it is or which writer produced it.
type Envelope struct {
	EventType   string
	ContentType string
	// SchemaVersion is 0 when the header is missing.
	SchemaVersion int
}

func (e Envelope) headers() []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: "event-type", Value: []byte(e.EventType)},
		{Key: "content-type", Value: []byte(e.ContentType)},
		{Key: "schema-version", Value: []byte(strconv.Itoa(e.SchemaVersion))},
	}
}

func envelopeOf(hs []kgo.RecordHeader) Envelope {
	var e Envelope
	for _, h := range hs {
		switch h.Key {
		case "event-type":
			e.EventType = string(h.Value)
		case "content-type":
			e.ContentType = string(h.Value)
		case "schema-version":
			e.SchemaVersion, _ = strconv.Atoi(string(h.Value))
		}
	}
	return e
}

// release is what one deployed version of the service writes and reads.
type release struct {
	schemaVersion int
	write         func(Order) ([]byte, error)
	read          func(env Envelope, value []byte, strict bool) (Order, error)
}

var releases = map[string]release{
	"v1": {schemaVersion: 1, write: encodeV1, read: decodeV1},
	"v2": {schemaVersion: 2, write: encodeV2, read: decodeCurrent},
	"v3": {schemaVersion: 3, write: encodeV3, read: decodeCurrent},
}

// ---------- release v1: frozen. These bytes and this reader exist in prod. ----------

type orderV1 struct {
	ID     string   `json:"id"`
	Amount float64  `json:"amount"` // dollars as a float: the reason v2 exists
	Status statusV1 `json:"status"`
}

// statusV1 validates the enum the way a careful v1 developer did: reject
// what you don't know. That's what breaks when v2 adds a value.
type statusV1 Status

func (s *statusV1) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch Status(v) {
	case StatusPending, StatusPaid:
		*s = statusV1(v)
		return nil
	}
	return fmt.Errorf("unknown status %q", v)
}

func encodeV1(o Order) ([]byte, error) {
	return json.Marshal(orderV1{ID: o.ID, Amount: float64(o.AmountMinor) / 100, Status: statusV1(o.Status)})
}

// decodeV1 ignores the headers: v1 assumed every record is a v1 OrderPlaced.
func decodeV1(_ Envelope, value []byte, strict bool) (Order, error) {
	var w orderV1
	if err := unmarshal(value, &w, strict); err != nil {
		return Order{}, err
	}
	return Order{ID: w.ID, AmountMinor: int64(math.Round(w.Amount * 100)), Currency: "USD", Status: Status(w.Status)}, nil
}

// ---------- release v2: expand. New fields next to the old one. ----------

type orderV2 struct {
	ID          string  `json:"id"`
	Amount      float64 `json:"amount"` // still written: v1 readers need it
	AmountMinor int64   `json:"amount_minor"`
	Currency    string  `json:"currency"`
	Status      Status  `json:"status"`
}

func encodeV2(o Order) ([]byte, error) {
	return json.Marshal(orderV2{
		ID:          o.ID,
		Amount:      float64(o.AmountMinor) / 100,
		AmountMinor: o.AmountMinor,
		Currency:    o.Currency,
		Status:      o.Status,
	})
}

// ---------- release v3: contract. amount is gone. ----------

type orderV3 struct {
	ID          string `json:"id"`
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
	Status      Status `json:"status"`
}

func encodeV3(o Order) ([]byte, error) {
	return json.Marshal(orderV3{ID: o.ID, AmountMinor: o.AmountMinor, Currency: o.Currency, Status: o.Status})
}

// ---------- the current reader: every version that can still be in the topic ----------

// orderWire is the union of every field any version ever wrote. Pointers
// tell "absent" from zero, which is the whole difference between upcasting
// an old record and silently reading 0.
type orderWire struct {
	ID          string   `json:"id"`
	Amount      *float64 `json:"amount"`       // v1, v2
	AmountMinor *int64   `json:"amount_minor"` // v2+
	Currency    *string  `json:"currency"`     // v2+
	Status      Status   `json:"status"`
}

// decodeCurrent is a tolerant reader: unknown fields and enum values pass,
// old shapes are upcast, and a record without any amount is an error.
func decodeCurrent(env Envelope, value []byte, strict bool) (Order, error) {
	if env.ContentType != "" && env.ContentType != contentType {
		return Order{}, fmt.Errorf("content-type %q: this reader only speaks %s", env.ContentType, contentType)
	}
	if env.EventType != "" && env.EventType != eventType {
		return Order{}, fmt.Errorf("event-type %q is not %s", env.EventType, eventType)
	}
	var w orderWire
	if err := unmarshal(value, &w, strict); err != nil {
		return Order{}, err
	}

	o := Order{ID: w.ID, Currency: "USD", Status: w.Status}
	switch {
	case w.AmountMinor != nil:
		o.AmountMinor = *w.AmountMinor
	case w.Amount != nil:
		o.AmountMinor = int64(math.Round(*w.Amount * 100))
	default:
		return Order{}, errors.New("neither amount_minor nor amount present")
	}
	if w.Currency != nil {
		o.Currency = *w.Currency
	}
	return o, nil
}

// unmarshal with strict=true rejects fields the struct doesn't know, which
// makes every additive change from a newer writer a decode error.
func unmarshal(value []byte, v any, strict bool) error {
	if !strict {
		return json.Unmarshal(value, v)
	}
	dec := json.NewDecoder(bytes.NewReader(value))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}
