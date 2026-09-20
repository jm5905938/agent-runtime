// 固定持久化边界
package codec

import (
	"bytes"
	"encoding"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"time"
	"unicode/utf8"
)

var (
	dataMapType = reflect.TypeFor[map[string]any]()
	timeType    = reflect.TypeFor[time.Time]()
	numberType  = reflect.TypeFor[json.Number]()
	hookTypes   = []reflect.Type{
		reflect.TypeFor[json.Marshaler](), reflect.TypeFor[json.Unmarshaler](),
		reflect.TypeFor[encoding.TextMarshaler](), reflect.TypeFor[encoding.TextUnmarshaler](),
	}
)

// visit只记录当前递归路径，允许多个字段共享同一个非循环对象
type visit struct {
	kind   reflect.Kind
	typ    reflect.Type
	ptr    uintptr
	length int
}

// ValidateData 检查 State、Payload等能否表达为 JSON。
func ValidateData(value any) error {
	return validateData(value, "$", make(map[visit]bool))
}

func validateData(value any, path string, active map[visit]bool) error {
	switch value := value.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr:
		return nil
	case string:
		if !utf8.ValidString(value) {
			return fmt.Errorf("%s: 非法UTF-8编码", path)
		}
	case float32:
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return fmt.Errorf("%s: 非法浮点数", path)
		}
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("%s: 非法浮点数", path)
		}
	case json.Number:
		decoder := json.NewDecoder(bytes.NewBufferString(string(value)))
		decoder.UseNumber()
		var decoded any
		if err := decoder.Decode(&decoded); err != nil {
			return fmt.Errorf("%s: 非法JSON数字 %q", path, value)
		}
		if number, ok := decoded.(json.Number); !ok || string(number) != string(value) {
			return fmt.Errorf("%s: 非法JSON数字 %q", path, value)
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return fmt.Errorf("%s: 非法JSON数字 %q", path, value)
		}
	case map[string]any:
		v := reflect.ValueOf(value)
		leave, err := enter(v, path, active)
		if err != nil {
			return err
		}
		defer leave()
		for key, item := range value {
			if !utf8.ValidString(key) {
				return fmt.Errorf("%s: 对象键非法UTF-8编码", path)
			}
			if err := validateData(item, fmt.Sprintf("%s[%q]", path, key), active); err != nil {
				return err
			}
		}
	case []any:
		leave, err := enter(reflect.ValueOf(value), path, active)
		if err != nil {
			return err
		}
		defer leave()
		for i, item := range value {
			if err := validateData(item, fmt.Sprintf("%s[%d]", path, i), active); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("%s: 类型非法 %T", path, value)
	}
	return nil
}

// encode时间为UTC
func Encode(value any) ([]byte, error) {
	if value == nil {
		return []byte("null"), nil
	}
	v := reflect.ValueOf(value)
	if err := validateType(v.Type(), make(map[reflect.Type]bool)); err != nil {
		return nil, err
	}
	normalized, err := normalize(v, "$", make(map[visit]bool))
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(normalized.Interface())
	if err != nil {
		return nil, fmt.Errorf("JSON 编码失败: %w", err)
	}
	return data, nil
}

func Decode(data []byte, dst any) error {
	v := reflect.ValueOf(dst)
	if !v.IsValid() || v.Kind() != reflect.Pointer || v.IsNil() {
		return fmt.Errorf("解码目标必须是非空指针")
	}
	if !utf8.Valid(data) {
		return fmt.Errorf("JSON 数据包含非法 UTF-8 编码")
	}
	if err := validateType(v.Type().Elem(), make(map[reflect.Type]bool)); err != nil {
		return err
	}
	temporary := reflect.New(v.Type().Elem())
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(temporary.Interface()); err != nil {
		return fmt.Errorf("JSON 解码失败: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err == nil {
			return fmt.Errorf("只允许包含一个 JSON 值")
		}
		return fmt.Errorf("JSON 值后存在非法数据: %w", err)
	}
	normalized, err := normalize(temporary.Elem(), "$", make(map[visit]bool))
	if err != nil {
		return err
	}
	v.Elem().Set(normalized)
	return nil
}

// validateType在编解码前拒绝自定义钩子(time.Time除外)
func validateType(t reflect.Type, seen map[reflect.Type]bool) error {
	if t == timeType || t == reflect.PointerTo(timeType) || t == numberType {
		return nil
	}
	if seen[t] {
		return nil
	}
	seen[t] = true
	for _, hook := range hookTypes {
		if t.Implements(hook) || (t.Kind() != reflect.Pointer && reflect.PointerTo(t).Implements(hook)) {
			return fmt.Errorf("类型 %s 不支持自定义 JSON 或文本编解码方法", t)
		}
	}
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice:
		return validateType(t.Elem(), seen)
	case reflect.Interface:
		if t.NumMethod() == 0 {
			return nil
		}
	case reflect.Map:
		if t == dataMapType {
			return nil
		}
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if field.Tag.Get("json") == "-" {
				continue
			}
			if field.PkgPath != "" {
				if field.Anonymous {
					return fmt.Errorf("%s.%s: 不支持未导出的嵌入字段", t, field.Name)
				}
				continue
			}
			if err := validateType(field.Type, seen); err != nil {
				return fmt.Errorf("%s.%s: %w", t, field.Name, err)
			}
		}
		return nil
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		return nil
	}
	return fmt.Errorf("不支持的记录类型 %s", t)
}

func normalize(v reflect.Value, path string, active map[visit]bool) (reflect.Value, error) {
	if v.Type() == timeType {
		utc := v.Interface().(time.Time).UTC()
		if _, err := utc.MarshalJSON(); err != nil {
			return reflect.Value{}, fmt.Errorf("%s: UTC 时间无效: %w", path, err)
		}
		return reflect.ValueOf(utc), nil
	}
	if v.Type() == numberType {
		return v, validateData(v.Interface(), path, active)
	}
	if v.Kind() == reflect.Interface || v.Type() == dataMapType {
		return v, validateData(v.Interface(), path, active)
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Slice:
		if v.IsNil() {
			return reflect.Zero(v.Type()), nil
		}
		leave, err := enter(v, path, active)
		if err != nil {
			return reflect.Value{}, err
		}
		defer leave()
		if v.Kind() == reflect.Pointer {
			inner, err := normalize(v.Elem(), path, active)
			if err != nil {
				return reflect.Value{}, err
			}
			result := reflect.New(v.Type().Elem())
			result.Elem().Set(inner)
			return result, nil
		}
		result := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := 0; i < v.Len(); i++ {
			item, err := normalize(v.Index(i), fmt.Sprintf("%s[%d]", path, i), active)
			if err != nil {
				return reflect.Value{}, err
			}
			result.Index(i).Set(item)
		}
		return result, nil
	case reflect.Struct:
		result := reflect.New(v.Type()).Elem()
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			if field.PkgPath != "" || field.Tag.Get("json") == "-" {
				continue
			}
			item, err := normalize(v.Field(i), path+"."+field.Name, active)
			if err != nil {
				return reflect.Value{}, err
			}
			result.Field(i).Set(item)
		}
		return result, nil
	case reflect.String:
		if !utf8.ValidString(v.String()) {
			return reflect.Value{}, fmt.Errorf("%s: 字符串非合法UTF-8", path)
		}
	case reflect.Float32, reflect.Float64:
		if math.IsNaN(v.Float()) || math.IsInf(v.Float(), 0) {
			return reflect.Value{}, fmt.Errorf("%s: 非法数字", path)
		}
	}
	return v, nil
}

func enter(v reflect.Value, path string, active map[visit]bool) (func(), error) {
	key := visit{kind: v.Kind(), typ: v.Type(), ptr: v.Pointer()}
	if v.Kind() == reflect.Slice {
		key.length = v.Len()
	}
	if active[key] {
		return nil, fmt.Errorf("%s: 存在循环引用", path)
	}
	active[key] = true
	return func() { delete(active, key) }, nil
}
