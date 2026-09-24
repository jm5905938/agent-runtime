package python

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"unicode/utf8"
)

//encoding/json会接受重复键和孤立的utf-16代理码位，此处拒绝两者，保证两种语言解释一致
func validateJSON(data []byte) error {
	if !utf8.Valid(data) || !json.Valid(data) {
		return errors.New("json或utf-8无效")
	}
	if err := validateSurrogates(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := uniqueJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("应只有一个json值")
	}
	return nil
}

func uniqueJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	switch token {
	case json.Delim('{'):
		keys := make(map[string]bool)
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || keys[name] {
				return errors.New("json对象键重复")
			}
			keys[name] = true
			if err := uniqueJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
	case json.Delim('['):
		for decoder.More() {
			if err := uniqueJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
	}
	return err
}

func validateSurrogates(data []byte) error {
	quoted := false
	for i := 0; i < len(data); i++ {
		if data[i] == '"' {
			quoted = !quoted
			continue
		}
		if !quoted || data[i] != '\\' {
			continue
		}
		i++
		if data[i] != 'u' {
			continue
		}
		value, _ := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
		i += 4
		if value >= 0xdc00 && value <= 0xdfff {
			return errors.New("utf-16低代理码位未配对")
		}
		if value < 0xd800 || value > 0xdbff {
			continue
		}
		if len(data) < i+7 || data[i+1] != '\\' || data[i+2] != 'u' {
			return errors.New("utf-16高代理码位未配对")
		}
		low, err := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return errors.New("utf-16代理码位配对无效")
		}
		i += 6
	}
	return nil
}
