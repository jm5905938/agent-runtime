package core

import (
	"encoding/json"
	"math"
	"testing"
)

func TestSameJSONValue(t *testing.T) {
	tests := []struct {
		name string
		a, b any
		want bool
	}{
		{"integer and decimal", 12, json.Number("12.000e0"), true},
		{"float and integer", float64(12), uint64(12), true},
		{"float32 JSON representation", float32(0.1), json.Number("0.1"), true},
		{"negative zero", json.Number("-0.00e-999999999999999999999"), 0, true},
		{"exact large integer", uint64(math.MaxUint64), json.Number("18446744073709551615.0"), true},
		{"distinct large integers", json.Number("9007199254740992"), json.Number("9007199254740993"), false},
		{"distinct precise decimals", json.Number("0.10000000000000000001"), json.Number("0.1"), false},
		{"large exponents", json.Number("1e999999999999999999999"), json.Number("10e999999999999999999998"), true},
		{"small exponents", json.Number("-123e-999999999999999999999"), json.Number("-1.23e-999999999999999999997"), true},
		{"map null", map[string]any(nil), nil, true},
		{"array null", []any(nil), nil, true},
		{"empty object differs from null", map[string]any{}, nil, false},
		{"empty array differs from null", []any{}, nil, false},
		{"array order", []any{1, 2}, []any{2, 1}, false},
		{"nested numbers", map[string]any{"b": []any{int8(2)}, "a": true}, map[string]any{"a": true, "b": []any{json.Number("2")}}, true},
		{"null differs from missing", map[string]any{"a": nil}, map[string]any{"b": nil}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := sameJSONValue(test.a, test.b)
			if err != nil || got != test.want {
				t.Fatalf("sameJSONValue(%v, %v) = %v, %v; want %v", test.a, test.b, got, err, test.want)
			}
		})
	}
	if _, err := sameJSONValue(math.Inf(1), 1); err == nil {
		t.Fatal("non-JSON value accepted")
	}
}
