package main

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// -update rewrites testdata from the writers. Only ever for a version that
// has not shipped: the bytes of a shipped version are already in the topic.
var update = flag.Bool("update", false, "rewrite testdata/<version>.json from the writers")

// samples are the orders behind testdata/<version>.json, one per release.
// Each uses what that release introduced.
var samples = map[string]Order{
	"v1": {ID: "o-1", AmountMinor: 1999, Currency: "USD", Status: StatusPaid},
	"v2": {ID: "o-2", AmountMinor: 1250, Currency: "EUR", Status: StatusRefunded},
	"v3": {ID: "o-3", AmountMinor: 4200, Currency: "USD", Status: StatusPaid},
}

// deployedReaders are the releases whose readers can still meet any record
// in the topic: running consumers, plus anything you would roll back to.
// A writer change ships only if every one of them passes the contract.
// TODO(14.4): this is the state during the v2 rollout. Update it only when
// prod really looks like that (README).
var deployedReaders = []string{"v1", "v2"}

func golden(t *testing.T, version string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", version+".json"))
	if err != nil {
		t.Fatalf("golden payload for %s: %v", version, err)
	}
	return b
}

func envFor(version string) Envelope {
	return Envelope{EventType: eventType, ContentType: contentType, SchemaVersion: releases[version].schemaVersion}
}

// TestWritersMatchGolden is the producer side: a release must keep writing
// exactly the shape its consumers were promised.
func TestWritersMatchGolden(t *testing.T) {
	for _, v := range []string{"v1", "v2", "v3"} {
		t.Run(v, func(t *testing.T) {
			got, err := releases[v].write(samples[v])
			if err != nil {
				t.Fatalf("write: %v", err)
			}
			path := filepath.Join("testdata", v+".json")
			if *update {
				if err := os.WriteFile(path, append(got, '\n'), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			var gotFields, wantFields map[string]any
			if err := json.Unmarshal(got, &gotFields); err != nil {
				t.Fatalf("writer output is not JSON: %v", err)
			}
			if err := json.Unmarshal(golden(t, v), &wantFields); err != nil {
				t.Fatalf("%s is not JSON: %v", path, err)
			}
			if !reflect.DeepEqual(gotFields, wantFields) {
				t.Errorf("%s writes\n  %s\nbut %s says\n  %s", v, got, path, golden(t, v))
			}
		})
	}
}

// TestContract is the consumer-driven part: every deployed reader decodes
// every payload version that can be in the topic, without errors and
// without losing meaning. An unknown status is fine (the reader must not
// guess); a wrong amount or currency is not.
func TestContract(t *testing.T) {
	for _, reader := range deployedReaders {
		for _, payload := range []string{"v1", "v2", "v3"} {
			t.Run(reader+"_reads_"+payload, func(t *testing.T) {
				got, err := releases[reader].read(envFor(payload), golden(t, payload), false)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				want := samples[payload]
				if got.ID != want.ID || got.AmountMinor != want.AmountMinor || got.Currency != want.Currency {
					t.Errorf("got %+v, want %+v", got, want)
				}
				if got.Status != want.Status && got.Status.Known() {
					t.Errorf("status %q, want %q (or one this reader admits it doesn't know)", got.Status, want.Status)
				}
			})
		}
	}
}

// TestCurrentReaderEdges covers records that are not golden files but do
// show up: no headers at all, fields from a future writer, broken input.
func TestCurrentReaderEdges(t *testing.T) {
	read := releases["v3"].read
	cases := []struct {
		name    string
		env     Envelope
		value   string
		want    Order
		wantErr bool
	}{
		{"no headers (pre-envelope record)", Envelope{}, `{"id":"o-9","amount":0.29,"status":"paid"}`,
			Order{ID: "o-9", AmountMinor: 29, Currency: "USD", Status: StatusPaid}, false},
		{"future fields and status", Envelope{SchemaVersion: 4}, `{"id":"o-9","amount_minor":5,"currency":"JPY","status":"chargeback","channel":"pos"}`,
			Order{ID: "o-9", AmountMinor: 5, Currency: "JPY", Status: "chargeback"}, false},
		{"no amount at all", envFor("v3"), `{"id":"o-9","currency":"USD","status":"paid"}`, Order{}, true},
		{"other event type", Envelope{EventType: "OrderShipped"}, `{"id":"o-9"}`, Order{}, true},
		{"not json", envFor("v3"), `id=o-9`, Order{}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := read(c.env, []byte(c.value), false)
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, c.wantErr)
			}
			if !c.wantErr && got != c.want {
				t.Errorf("got %+v, want %+v", got, c.want)
			}
		})
	}
}

// TestWhyV1IsRetired pins down what the frozen v1 reader does with later
// payloads, which is why it is not in deployedReaders.
func TestWhyV1IsRetired(t *testing.T) {
	v2USD := []byte(`{"id":"o-5","amount":3.5,"amount_minor":350,"currency":"USD","status":"paid"}`)
	if o, err := decodeV1(envFor("v2"), v2USD, false); err != nil || o.AmountMinor != 350 {
		t.Errorf("additive change: v1 should read a v2 USD order, got %+v, %v", o, err)
	}
	if _, err := decodeV1(envFor("v2"), v2USD, true); err == nil {
		t.Error("strict v1 reader accepted unknown fields")
	}
	if _, err := decodeV1(envFor("v2"), golden(t, "v2"), false); err == nil {
		t.Error("v1 reader accepted a status it does not know")
	}
	if o, err := decodeV1(envFor("v3"), golden(t, "v3"), false); err != nil || o.AmountMinor != 0 {
		t.Errorf("v1 reader on v3: want a silent zero amount, got %+v, %v", o, err)
	}
}
