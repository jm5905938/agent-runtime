package core

import (
	"bytes"
	"encoding/json"
	"math/big"
	"strings"

	"agent-runtime/codec"
	"agent-runtime/domain"
)

func sameEventContent(a, b domain.Event) (bool, error) {
	if a.ID != b.ID || a.Type != b.Type {
		return false, nil
	}
	return sameJSONValue(a.Payload, b.Payload)
}

//按json比较，保留大整数精度
func sameJSONValue(a, b any) (bool, error) {
	decode := func(value any) (any, error) {
		encoded, err := codec.Encode(value)
		if err != nil {
			return nil, err
		}
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.UseNumber()
		var result any
		err = decoder.Decode(&result)
		return result, err
	}
	left, err := decode(a)
	if err != nil {
		return false, err
	}
	right, err := decode(b)
	if err != nil {
		return false, err
	}
	return equalDecodedJSON(left, right), nil
}

func equalDecodedJSON(a, b any) bool {
	switch a := a.(type) {
	case nil:
		return b == nil
	case bool:
		other, ok := b.(bool)
		return ok && a == other
	case string:
		other, ok := b.(string)
		return ok && a == other
	case json.Number:
		other, ok := b.(json.Number)
		if !ok {
			return false
		}
		ad, ae := normalizedNumber(a)
		bd, be := normalizedNumber(other)
		return ad == bd && ae == be
	case []any:
		other, ok := b.([]any)
		if !ok || len(a) != len(other) {
			return false
		}
		for i := range a {
			if !equalDecodedJSON(a[i], other[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		other, ok := b.(map[string]any)
		if !ok || len(a) != len(other) {
			return false
		}
		for key, value := range a {
			item, exists := other[key]
			if !exists || !equalDecodedJSON(value, item) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

//分开保存系数和指数，避免展开巨大数值
func normalizedNumber(number json.Number) (string, string) {
	mantissa, exponent, found := strings.Cut(strings.ToLower(string(number)), "e")
	var power big.Int
	if found {
		power.SetString(exponent, 10)
	}
	negative := strings.HasPrefix(mantissa, "-")
	mantissa = strings.TrimPrefix(mantissa, "-")
	if whole, fraction, dot := strings.Cut(mantissa, "."); dot {
		mantissa = whole + fraction
		power.Sub(&power, big.NewInt(int64(len(fraction))))
	}
	mantissa = strings.TrimLeft(mantissa, "0")
	if mantissa == "" {
		return "0", "0"
	}
	trimmed := strings.TrimRight(mantissa, "0")
	power.Add(&power, big.NewInt(int64(len(mantissa)-len(trimmed))))
	if negative {
		trimmed = "-" + trimmed
	}
	return trimmed, power.String()
}
