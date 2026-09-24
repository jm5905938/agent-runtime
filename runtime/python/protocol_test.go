package python

import (
	"strings"
	"testing"
)

func TestRejectAmbiguousJSON(t *testing.T) {
	valid := `{"version":1,"id":"attempt","result":{"state_update":{},"actions":[]}}`
	for _, data := range []string{
		strings.Replace(valid, `"version":1`, `"version":2,"version":1`, 1),
		strings.Replace(valid, `"version"`, `"Version"`, 1),
		strings.Replace(valid, `"version":1`, `"version":true`, 1),
		strings.Replace(valid, `"version":1`, `"version":1.0`, 1),
		strings.Replace(valid, `"version":1`, `"version":{}`, 1),
		strings.Replace(valid, `"id"`, `"ID"`, 1),
		strings.Replace(valid, `"state_update":{}`, `"State_update":{}`, 1),
		strings.Replace(valid, `"state_update":{}`, `"state_update":{"x":1,"x":2}`, 1),
		strings.Replace(valid, `"state_update":{}`, `"state_update":{"a":1,"\u0061":2}`, 1),
		strings.Replace(valid, `"state_update":{}`, `"state_update":{"x":"\ud800"}`, 1),
		strings.Replace(valid, `"state_update":{}`, `"state_update":{"\udc00":"x"}`, 1),
		strings.Replace(valid, `"state_update":{}`, `"state_update":{"x":"\ud800\u1234"}`, 1),
		strings.Replace(valid, `"state_update":{}`, `"state_update":{"x":"\ud800\\udc00"}`, 1),
		strings.Replace(valid, `"actions":[]`, `"actions":[{"ID":"a","type":"echo","payload":{}}]`, 1),
		strings.Replace(valid, `"actions":[]`, `"actions":[{"id":"a","type":"echo","payload":{},"execution_id":null}]`, 1),
		`{"version":1,"id":"attempt","error":{"Kind":"runtime","message":"oops"}}`,
	} {
		t.Run(data, func(t *testing.T) {
			_, err, ok := decodeResponse([]byte(data), input("ok", "attempt"))
			if err == nil || ok {
				t.Fatalf("ambiguous response accepted: %s", data)
			}
		})
	}
}

func TestValidUnicodeAndAction(t *testing.T) {
	data := []byte(`{"version":1,"id":"attempt","result":{"state_update":{"emoji":"\ud83d\ude00","literal":"\\ud800","quoted":"\"","nested":[{},["中文"]]},"actions":[{"id":"a","type":"echo","payload":{},"execution_id":"execution"}]}}`)
	result, err, valid := decodeResponse(data, input("ok", "attempt"))
	if err != nil || !valid {
		t.Fatalf("valid response rejected: %v", err)
	}
	if result.StateUpdate["emoji"] != "😀" || result.StateUpdate["literal"] != `\ud800` ||
		len(result.Actions) != 1 || result.Actions[0].ExecutionID == nil || *result.Actions[0].ExecutionID != "execution" {
		t.Fatalf("unexpected decode: %#v", result)
	}
}
