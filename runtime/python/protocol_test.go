package python

import (
	"encoding/json"
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

func TestOptionalSubagentResult(t *testing.T) {
	for _, task := range []string{
		``,
		`,"task_result":{"status":"succeeded","output":{"message":"完成","number":900719925474099312345}}`,
		`,"task_result":{"status":"failed","output":{},"error":{"kind":"business","message":"任务失败"}}`,
		`,"task_result":{"status":"cancelled","output":{}}`,
		`,"task_result":{"status":"cancelled","output":{},"error":{"kind":"interrupted","message":"任务取消"}}`,
	} {
		t.Run(task, func(t *testing.T) {
			data := `{"version":1,"id":"attempt","result":{"state_update":{},"actions":[]` + task + `}}`
			result, err, valid := decodeResponse([]byte(data), input("ok", "attempt"))
			if err != nil || !valid {
				t.Fatalf("task_result响应被拒绝: %v", err)
			}
			if task == "" {
				if result.TaskResult != nil {
					t.Fatalf("旧响应新增了task_result: %+v", result.TaskResult)
				}
				return
			}
			if result.TaskResult == nil {
				t.Fatal("task_result未解码")
			}
			if strings.Contains(task, `"number"`) && result.TaskResult.Output["number"] != json.Number("900719925474099312345") {
				t.Fatalf("任务输出数字失真: %+v", result.TaskResult.Output)
			}
		})
	}
}

func TestRejectInvalidSubagentResult(t *testing.T) {
	for _, task := range []string{
		`null`, `[]`, `{}`, `{"status":"running","output":{}}`,
		`{"status":true,"output":{}}`, `{"status":"succeeded","output":null}`,
		`{"status":"succeeded","output":[],"extra":true}`,
		`{"status":"succeeded","output":{},"error":{"kind":"business","message":"错误"}}`,
		`{"status":"failed","output":{}}`,
		`{"status":"failed","output":{},"error":null}`,
		`{"status":"failed","output":{},"error":{"kind":"other","message":"错误"}}`,
		`{"status":"failed","output":{},"error":{"kind":"business","message":" "}}`,
		`{"status":"failed","output":{},"error":{"kind":"business","message":3}}`,
		`{"status":"failed","output":{},"error":{"kind":"business","message":"错误","extra":true}}`,
		`{"status":"succeeded","status":"failed","output":{}}`,
		`{"status":"succeeded","output":{"number":NaN}}`,
	} {
		t.Run(task, func(t *testing.T) {
			data := `{"version":1,"id":"attempt","result":{"state_update":{},"actions":[],"task_result":` + task + `}}`
			result, err, valid := decodeResponse([]byte(data), input("ok", "attempt"))
			if err == nil || valid || result.TaskResult != nil || result.StateUpdate != nil || result.Actions != nil {
				t.Fatalf("非法task_result发布了结果: valid=%v result=%+v err=%v", valid, result, err)
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
