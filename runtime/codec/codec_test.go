package codec_test

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"agent-runtime/codec"
	"agent-runtime/domain"
)

func TestBusinessValuesRoundTrip(t *testing.T) {
	values := map[string]any{
		"large": int64(9007199254740993), "unsigned": uint64(math.MaxUint64),
		"integer": int(-42), "float32": float32(1.25), "float64": 1.2345678901234567,
		"decimal":  json.Number("12345678901234567890.12345678901234567890"),
		"exponent": json.Number("1.2300e+9999"), "negative_zero": json.Number("-0"),
		"null": nil, "nil_map": map[string]any(nil), "nil_slice": []any(nil),
		"empty_map": map[string]any{}, "empty_slice": []any{},
		"nested": []any{map[string]any{"text": "中文", "bool": true}, nil},
	}
	if err := codec.ValidateData(values); err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.Encode(values)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := codec.Decode(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{
		"large": "9007199254740993", "unsigned": "18446744073709551615",
		"integer": "-42", "float32": "1.25", "float64": "1.2345678901234567",
		"decimal": "12345678901234567890.12345678901234567890", "exponent": "1.2300e+9999", "negative_zero": "-0",
	} {
		if got, ok := decoded[key].(json.Number); !ok || string(got) != want {
			t.Errorf("%s = %v (%T), want json.Number(%s)", key, decoded[key], decoded[key], want)
		}
	}
	for _, key := range []string{"null", "nil_map", "nil_slice"} {
		if decoded[key] != nil {
			t.Errorf("%s = %#v, want nil", key, decoded[key])
		}
	}
	if got, ok := decoded["empty_map"].(map[string]any); !ok || got == nil || len(got) != 0 {
		t.Fatalf("empty map lost: %#v", decoded["empty_map"])
	}
	if got, ok := decoded["empty_slice"].([]any); !ok || got == nil || len(got) != 0 {
		t.Fatalf("empty slice lost: %#v", decoded["empty_slice"])
	}
	if got := decoded["nested"].([]any)[0].(map[string]any); got["text"] != "中文" || got["bool"] != true {
		t.Fatalf("nested values changed: %#v", got)
	}
	again, err := codec.Encode(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, again) {
		t.Fatalf("round trip changed JSON:\n%s\n%s", encoded, again)
	}
}

type customString string
type customMap map[string]any
type customSlice []any
type marshaled struct{ Value any }

func (marshaled) MarshalJSON() ([]byte, error) { return []byte(`"hidden"`), nil }

type unmarshaled struct{}

func (*unmarshaled) UnmarshalJSON([]byte) error { panic("unmarshal hook must not run") }

func TestUnsupportedBusinessValuesRejected(t *testing.T) {
	integer := 1
	cases := map[string]any{
		"pointer": &integer, "struct": struct{ X int }{1}, "typed_map": map[string]int{"x": 1},
		"typed_slice": []string{"x"}, "custom_string": customString("x"),
		"custom_map": customMap{}, "custom_slice": customSlice{}, "time": time.Now(),
		"marshal_hook": marshaled{Value: make(chan int)}, "channel": make(chan int), "function": func() {},
		"complex": complex(1, 2), "nan": math.NaN(), "infinity": math.Inf(1), "float32_infinity": float32(math.Inf(-1)),
		"invalid_utf8": string([]byte{0xff}), "invalid_key": map[string]any{string([]byte{0xff}): 1},
		"empty_number": json.Number(""), "invalid_number": json.Number("01"), "non_number": json.Number("true"),
		"number_space": json.Number(" 1"), "number_trailing": json.Number("1 2"), "number_nan": json.Number("NaN"),
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			if err := codec.ValidateData(value); err == nil {
				t.Errorf("ValidateData accepted %T", value)
			}
			if _, err := codec.Encode(map[string]any{"value": value}); err == nil {
				t.Errorf("Encode accepted %T in business payload", value)
			}
			if _, err := codec.Encode(struct{ Value any }{value}); err == nil {
				t.Errorf("Encode accepted %T in interface field", value)
			}
		})
	}
}

func TestCyclesRejectedAndSharedValuesAllowed(t *testing.T) {
	mapping := map[string]any{}
	mapping["self"] = mapping
	slice := make([]any, 1)
	slice[0] = slice
	for name, value := range map[string]any{"map": mapping, "slice": slice} {
		t.Run(name, func(t *testing.T) {
			if err := codec.ValidateData(value); err == nil || !strings.Contains(err.Error(), "循环引用") {
				t.Fatalf("want cycle error, got %v", err)
			}
			if _, err := codec.Encode(value); err == nil {
				t.Fatal("Encode accepted cycle")
			}
		})
	}
	type node struct {
		Next *node `json:"next"`
	}
	n := new(node)
	n.Next = n
	if _, err := codec.Encode(n); err == nil || !strings.Contains(err.Error(), "循环引用") {
		t.Fatalf("record cycle: %v", err)
	}
	shared := map[string]any{"value": []any{1, "x"}}
	if _, err := codec.Encode([]any{shared, shared}); err != nil {
		t.Fatalf("shared map is not a cycle: %v", err)
	}
	sharedNode := &node{}
	if _, err := codec.Encode([]*node{sharedNode, sharedNode}); err != nil {
		t.Fatalf("shared pointer is not a cycle: %v", err)
	}
	// 同一底层数组的空切片不引用元素，不能误报为循环。
	emptyPrefix := make([]any, 1)
	emptyPrefix[0] = emptyPrefix[:0]
	if _, err := codec.Encode(emptyPrefix); err != nil {
		t.Fatalf("empty prefix is not a cycle: %v", err)
	}
	// 首个字段的地址可以等于外层结构体地址，但两种指针没有形成循环。
	type inner struct{ N int }
	type outer struct {
		First     inner
		Reference *inner
	}
	record := &outer{First: inner{N: 7}}
	record.Reference = &record.First
	if _, err := codec.Encode(record); err != nil {
		t.Fatalf("shared first-field address is not a cycle: %v", err)
	}
}

func TestRecordIdentityAndUTCTimes(t *testing.T) {
	local := time.Date(2026, 9, 20, 12, 34, 56, 123456789, time.FixedZone("CST", 8*60*60))
	execution := domain.NewExecution(domain.ID("agent-1"), domain.ID("event-1"))
	execution.ID = domain.ID("execution-1")
	execution.CreatedAt, execution.StartedAt, execution.FinishedAt = local, &local, &local
	execution.Status = domain.ExecutionStatusCompleted
	encoded, err := codec.Encode(execution)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("+08:00")) || !bytes.Contains(encoded, []byte("04:34:56.123456789Z")) {
		t.Fatalf("time not encoded as UTC: %s", encoded)
	}
	if execution.CreatedAt.Location() == time.UTC || execution.StartedAt.Location() == time.UTC {
		t.Fatal("Encode mutated input times")
	}
	var decoded domain.Execution
	if err := codec.Decode(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ID != execution.ID || decoded.AgentID != execution.AgentID || decoded.EventID != execution.EventID || decoded.Status != execution.Status {
		t.Fatalf("record identifiers changed: %#v", decoded)
	}
	for _, got := range []time.Time{decoded.CreatedAt, *decoded.StartedAt, *decoded.FinishedAt} {
		if !got.Equal(local) || got.Location() != time.UTC {
			t.Fatalf("time changed or not UTC: %v", got)
		}
	}
	var offset struct {
		At      time.Time  `json:"at"`
		Pointer *time.Time `json:"pointer"`
	}
	if err := codec.Decode([]byte(`{"at":"2026-09-20T12:34:56+08:00","pointer":"2026-09-20T12:34:56+08:00"}`), &offset); err != nil {
		t.Fatal(err)
	}
	if offset.At.Location() != time.UTC || offset.Pointer.Location() != time.UTC || offset.At.Hour() != 4 {
		t.Fatalf("decode did not normalize UTC: %#v", offset)
	}
}

func TestDecodeStrictAndAtomic(t *testing.T) {
	type record struct {
		Name  string         `json:"name"`
		Data  map[string]any `json:"data"`
		Count int            `json:"count"`
	}
	for name, raw := range map[string]string{
		"unknown_field": `{"name":"new","unknown":1}`, "trailing_value": `{"name":"new"} {}`,
		"trailing_garbage": `{"name":"new"} garbage`, "invalid_type": `{"name":"new","count":"wrong"}`,
		"truncated": `{"name":"new",`, "invalid_utf8": "{\"name\":\"\xff\"}", "empty": "",
	} {
		t.Run(name, func(t *testing.T) {
			original := record{Name: "kept", Data: map[string]any{"kept": true}, Count: 9}
			got := original
			if err := codec.Decode([]byte(raw), &got); err == nil {
				t.Fatal("invalid JSON accepted")
			}
			if !reflect.DeepEqual(got, original) {
				t.Fatalf("failed decode polluted destination: %#v", got)
			}
		})
	}
	for _, dst := range []any{nil, (*record)(nil), record{}} {
		if err := codec.Decode([]byte(`{}`), dst); err == nil {
			t.Fatalf("invalid destination accepted: %T", dst)
		}
	}
	var decoded any
	if err := codec.Decode([]byte(" 12345678901234567890 \n\t"), &decoded); err != nil {
		t.Fatal(err)
	}
	if got, ok := decoded.(json.Number); !ok || got != "12345678901234567890" {
		t.Fatalf("wrong scalar decode: %#v", decoded)
	}
}

func TestRecordCodecHooksRejected(t *testing.T) {
	if _, err := codec.Encode(marshaled{}); err == nil {
		t.Fatal("custom marshal hook accepted")
	}
	var destination unmarshaled
	if err := codec.Decode([]byte(`{}`), &destination); err == nil {
		t.Fatal("custom unmarshal hook accepted")
	}
}

func TestInvalidUTCTimeRejectedWithoutChangingDestination(t *testing.T) {
	if _, err := codec.Encode(time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("out-of-range time accepted")
	}
	original := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	got := original
	// 本地年份虽然合法，归一化 UTC 后已经超出 JSON 时间范围。
	if err := codec.Decode([]byte(`"0000-01-01T00:00:00+08:00"`), &got); err == nil {
		t.Fatal("time outside UTC encoding range accepted")
	}
	if got != original {
		t.Fatal("failed UTC validation polluted destination")
	}
}

func TestNilAndEmptyRecordContainers(t *testing.T) {
	type record struct {
		Map   map[string]any  `json:"map"`
		Slice []domain.Action `json:"slice"`
	}
	for _, original := range []record{{}, {Map: map[string]any{}, Slice: []domain.Action{}}} {
		encoded, err := codec.Encode(original)
		if err != nil {
			t.Fatal(err)
		}
		var decoded record
		if err := codec.Decode(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(original, decoded) {
			t.Fatalf("nil/empty changed: %#v -> %#v", original, decoded)
		}
	}
}
