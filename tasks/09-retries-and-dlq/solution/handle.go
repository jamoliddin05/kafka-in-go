package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	errGatewayUnavailable = errors.New("payment gateway: 503 service unavailable")
	errInvalidOrder       = errors.New("invalid order")
)

type errClass int

const (
	transient errClass = iota // may succeed if you try again later
	permanent                 // will fail the same way every time: retrying only burns time
)

func (c errClass) String() string {
	if c == permanent {
		return "permanent"
	}
	return "transient"
}

// classify decides where a failed record goes next. Unknown errors count as
// transient: the attempt limit still ends them in the DLQ, just later.
func classify(err error) errClass {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	switch {
	case errors.As(err, &syntaxErr), errors.As(err, &typeErr), errors.Is(err, errInvalidOrder):
		return permanent
	default:
		return transient
	}
}

// process is the business logic: decode, validate, charge.
func (w *worker) process(ctx context.Context, rec *kgo.Record, attempt int) error {
	o, err := decodeOrder(rec.Value, w.cfg.lenient)
	if err != nil {
		return err
	}
	if o.Amount <= 0 {
		return fmt.Errorf("%w: amount_cents %d", errInvalidOrder, o.Amount)
	}
	if err := chargeGateway(ctx, o); err != nil {
		return fmt.Errorf("charge: %w", err)
	}
	w.app.Log.InfoContext(ctx, "order paid", "order_id", o.ID, "key", string(rec.Key),
		"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "attempt", attempt)
	return nil
}

func decodeOrder(b []byte, lenient bool) (Order, error) {
	var o Order
	err := json.Unmarshal(b, &o)
	if err == nil {
		return o, nil
	}
	// The hotfix for v2 producers. Unmarshal keeps filling the other fields
	// after a type error, so only the amount is missing here.
	if typeErr, ok := errors.AsType[*json.UnmarshalTypeError](err); ok && lenient && typeErr.Field == "amount_cents" {
		var v2 orderV2
		if json.Unmarshal(b, &v2) == nil {
			if f, perr := strconv.ParseFloat(v2.Amount, 64); perr == nil {
				o.Amount = int64(math.Round(f * 100))
				return o, nil
			}
		}
	}
	return Order{}, fmt.Errorf("decode order: %w", err)
}

// chargeGateway stands in for the payment provider: for orders the producer
// marked, it fails until gateway_down_until, however often you call it.
func chargeGateway(ctx context.Context, o Order) error {
	if time.Now().Before(o.GatewayDownUntil) {
		return errGatewayUnavailable
	}
	return sleep(ctx, 2*time.Millisecond)
}
