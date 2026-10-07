package envconfig

import "testing"

func TestParseConfigQuotedEscapes(t *testing.T) {
	_, value, err := parseConfigLine(`LLM_MODEL="model\"name\\path\nnext" #模型`)
	if err != nil || value != "model\"name\\path\nnext" {
		t.Fatal("双引号转义解析不符")
	}
}
