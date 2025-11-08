package jsonfast

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	t.Run("with positive capacity", func(t *testing.T) {
		b := New(512)
		if b == nil {
			t.Fatal("New() returned nil")
		}
		if cap(b.buf) < 512 {
			t.Errorf("Expected capacity >= 512, got %d", cap(b.buf))
		}
	})

	t.Run("with zero capacity", func(t *testing.T) {
		b := New(0)
		if b == nil {
			t.Fatal("New() returned nil")
		}
		if cap(b.buf) < 256 {
			t.Errorf("Expected default capacity >= 256, got %d", cap(b.buf))
		}
	})

	t.Run("with negative capacity", func(t *testing.T) {
		b := New(-10)
		if b == nil {
			t.Fatal("New() returned nil")
		}
		if cap(b.buf) < 256 {
			t.Errorf("Expected default capacity >= 256, got %d", cap(b.buf))
		}
	})
}

func TestReset(t *testing.T) {
	b := New(256)
	b.BeginObject()
	b.AddStringField("test", "value")
	b.EndObject()

	if len(b.Bytes()) == 0 {
		t.Error("Expected non-empty buffer before reset")
	}

	b.Reset()

	if len(b.Bytes()) != 0 {
		t.Errorf("Expected empty buffer after reset, got length %d", len(b.Bytes()))
	}
	if b.opened {
		t.Error("Expected opened=false after reset")
	}
	if !b.first {
		t.Error("Expected first=true after reset")
	}
}

func TestAddStringField(t *testing.T) {
	tests := getStringFieldTestCases()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runStringFieldTest(t, tt)
		})
	}
}

func getStringFieldTestCases() []stringFieldTest {
	return []stringFieldTest{
		{name: "simple string", key: "message", value: "hello world", expected: `{"message":"hello world"}`},
		{name: "empty string", key: "empty", value: "", expected: `{"empty":""}`},
		{name: "string with quotes", key: "quoted", value: `she said "hello"`, expected: `{"quoted":"she said \"hello\""}`},
		{name: "string with backslash", key: "path", value: `C:\Users\Test`, expected: `{"path":"C:\\Users\\Test"}`},
		{name: "string with newline", key: "multiline", value: "line1\nline2", expected: `{"multiline":"line1\nline2"}`},
		{name: "string with tab", key: "tabbed", value: "col1\tcol2", expected: `{"tabbed":"col1\tcol2"}`},
	}
}

type stringFieldTest struct {
	name     string
	key      string
	value    string
	expected string
}

func runStringFieldTest(t *testing.T, tt stringFieldTest) {
	t.Helper()
	b := New(256)
	b.BeginObject()
	b.AddStringField(tt.key, tt.value)
	b.EndObject()

	result := string(b.Bytes())
	if result != tt.expected {
		t.Errorf("Expected %s, got %s", tt.expected, result)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(b.Bytes(), &parsed); err != nil {
		t.Errorf("Generated invalid JSON: %v", err)
	}
}

func TestAddRawJSONField(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		rawJSON  []byte
		expected string
	}{
		{
			name:     "simple object",
			key:      "data",
			rawJSON:  []byte(`{"nested":"value"}`),
			expected: `{"data":{"nested":"value"}}`,
		},
		{
			name:     "array",
			key:      "items",
			rawJSON:  []byte(`[1,2,3]`),
			expected: `{"items":[1,2,3]}`,
		},
		{
			name:     "complex nested",
			key:      "complex",
			rawJSON:  []byte(`{"a":{"b":{"c":"deep"}}}`),
			expected: `{"complex":{"a":{"b":{"c":"deep"}}}}`,
		},
		{
			name:     "number",
			key:      "count",
			rawJSON:  []byte(`42`),
			expected: `{"count":42}`,
		},
		{
			name:     "boolean",
			key:      "flag",
			rawJSON:  []byte(`true`),
			expected: `{"flag":true}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := New(256)
			b.BeginObject()
			b.AddRawJSONField(tt.key, tt.rawJSON)
			b.EndObject()

			result := string(b.Bytes())
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}

			// Verify it's valid JSON
			var parsed map[string]interface{}
			if err := json.Unmarshal(b.Bytes(), &parsed); err != nil {
				t.Errorf("Generated invalid JSON: %v", err)
			}
		})
	}
}

func TestAddIntField(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    int
		expected string
	}{
		{
			name:     "positive int",
			key:      "count",
			value:    42,
			expected: `{"count":42}`,
		},
		{
			name:     "zero",
			key:      "zero",
			value:    0,
			expected: `{"zero":0}`,
		},
		{
			name:     "negative int",
			key:      "negative",
			value:    -123,
			expected: `{"negative":-123}`,
		},
		{
			name:     "large number",
			key:      "large",
			value:    999999,
			expected: `{"large":999999}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := New(256)
			b.BeginObject()
			b.AddIntField(tt.key, tt.value)
			b.EndObject()

			result := string(b.Bytes())
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}

			// Verify it's valid JSON
			var parsed map[string]interface{}
			if err := json.Unmarshal(b.Bytes(), &parsed); err != nil {
				t.Errorf("Generated invalid JSON: %v", err)
			}
		})
	}
}

func TestMultipleFields(t *testing.T) {
	b := New(256)
	b.BeginObject()
	b.AddStringField("name", "John")
	b.AddIntField("age", 30)
	b.AddStringField("city", "New York")
	b.AddRawJSONField("tags", []byte(`["developer","golang"]`))
	b.EndObject()

	expected := `{"name":"John","age":30,"city":"New York","tags":["developer","golang"]}`
	result := string(b.Bytes())

	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}

	// Verify it's valid JSON and has correct values
	var parsed map[string]interface{}
	if err := json.Unmarshal(b.Bytes(), &parsed); err != nil {
		t.Fatalf("Generated invalid JSON: %v", err)
	}

	if parsed["name"] != "John" {
		t.Errorf("Expected name=John, got %v", parsed["name"])
	}
	if parsed["age"] != float64(30) {
		t.Errorf("Expected age=30, got %v", parsed["age"])
	}
}

func TestAddTimeRFC3339Field(t *testing.T) {
	// Test with a specific time
	testTime := time.Date(2025, 11, 8, 10, 30, 45, 0, time.UTC)

	b := New(256)
	b.BeginObject()
	b.AddTimeRFC3339Field("timestamp", testTime)
	b.EndObject()

	expected := `{"timestamp":"2025-11-08T10:30:45Z"}`
	result := string(b.Bytes())

	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}

	// Verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal(b.Bytes(), &parsed); err != nil {
		t.Fatalf("Generated invalid JSON: %v", err)
	}

	// Verify the timestamp can be parsed back
	timestampStr, ok := parsed["timestamp"].(string)
	if !ok {
		t.Fatal("timestamp is not a string")
	}

	parsedTime, err := time.Parse(time.RFC3339, timestampStr)
	if err != nil {
		t.Fatalf("Failed to parse timestamp: %v", err)
	}

	if !parsedTime.Equal(testTime) {
		t.Errorf("Expected time %v, got %v", testTime, parsedTime)
	}
}

func TestAddNestedStringMapField(t *testing.T) {
	t.Run("simple nested map", func(t *testing.T) {
		testNestedMapWithData(t)
	})
	t.Run("empty map", func(t *testing.T) {
		testNestedMapEmpty(t, "empty", map[string]map[string]string{})
	})
	t.Run("nil map", func(t *testing.T) {
		testNestedMapEmpty(t, "nil", nil)
	})
}

func testNestedMapWithData(t *testing.T) {
	t.Helper()
	data := map[string]map[string]string{
		"user1": {"name": "John", "role": "admin"},
	}
	b := New(256)
	b.BeginObject()
	b.AddNestedStringMapField("data", data)
	b.EndObject()

	var parsed map[string]interface{}
	if err := json.Unmarshal(b.Bytes(), &parsed); err != nil {
		t.Fatalf("Generated invalid JSON: %v", err)
	}

	verifyNestedMapStructure(t, parsed, "data", data)
}

func testNestedMapEmpty(t *testing.T, key string, data map[string]map[string]string) {
	t.Helper()
	b := New(256)
	b.BeginObject()
	b.AddNestedStringMapField(key, data)
	b.EndObject()

	result := string(b.Bytes())
	expected := `{}`
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func verifyNestedMapStructure(
	t *testing.T,
	parsed map[string]interface{},
	key string,
	data map[string]map[string]string,
) {
	t.Helper()
	dataField, ok := parsed[key].(map[string]interface{})
	if !ok {
		t.Fatal("Expected nested map structure")
	}

	for outerKey, innerMap := range data {
		innerParsed, ok := dataField[outerKey].(map[string]interface{})
		if !ok {
			t.Errorf("Expected inner map for key %s", outerKey)
			continue
		}
		verifyInnerMapValues(t, innerParsed, innerMap)
	}
}

func verifyInnerMapValues(t *testing.T, parsed map[string]interface{}, expected map[string]string) {
	t.Helper()
	for k, v := range expected {
		if parsed[k] != v {
			t.Errorf("Expected %s=%s, got %v", k, v, parsed[k])
		}
	}
}

func TestEscapeString(t *testing.T) {
	tests := getEscapeStringTestCases()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testEscapeStringCase(t, tt)
		})
	}
}

func getEscapeStringTestCases() []escapeStringTest {
	return []escapeStringTest{
		{name: "no escape needed", input: "hello world", expected: "hello world"},
		{name: "quote", input: `say "hi"`, expected: `say \"hi\"`},
		{name: "backslash", input: `path\to\file`, expected: `path\\to\\file`},
		{name: "newline", input: "line1\nline2", expected: `line1\nline2`},
		{name: "tab", input: "col1\tcol2", expected: `col1\tcol2`},
		{name: "carriage return", input: "line1\rline2", expected: `line1\rline2`},
		{name: "backspace", input: "text\bback", expected: `text\bback`},
		{name: "form feed", input: "page\fbreak", expected: `page\fbreak`},
	}
}

type escapeStringTest struct {
	name     string
	input    string
	expected string
}

func testEscapeStringCase(t *testing.T, tt escapeStringTest) {
	t.Helper()
	b := New(256)
	b.buf = append(b.buf, '"')
	b.escapeString(tt.input)
	b.buf = append(b.buf, '"')

	result := string(b.buf[1 : len(b.buf)-1])
	if result != tt.expected {
		t.Errorf("Expected %q, got %q", tt.expected, result)
	}
}

func TestComplexJSON(t *testing.T) {
	// Build a complex nested structure
	b := New(512)
	b.BeginObject()
	b.AddStringField("source", "10.0.0.1")
	b.AddStringField("timestamp", "1234567890")
	b.AddRawJSONField("object", []byte(`{"message":"test","severity":5,"nested":{"key":"value"}}`))
	b.AddStringField("raw", "<189>1 test syslog message")
	b.EndObject()

	result := b.Bytes()

	// Verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("Generated invalid JSON: %v", err)
	}

	// Verify all fields are present
	if parsed["source"] != "10.0.0.1" {
		t.Errorf("Expected source=10.0.0.1, got %v", parsed["source"])
	}

	// Verify object was included as JSON, not string
	objectField, ok := parsed["object"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected object to be a map, got %T", parsed["object"])
	}

	if objectField["message"] != "test" {
		t.Errorf("Expected object.message=test, got %v", objectField["message"])
	}

	if objectField["severity"] != float64(5) {
		t.Errorf("Expected object.severity=5, got %v", objectField["severity"])
	}
}

func BenchmarkBuilder(b *testing.B) {
	b.Run("AddStringField", func(b *testing.B) {
		builder := New(256)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			builder.Reset()
			builder.BeginObject()
			builder.AddStringField("key1", "value1")
			builder.AddStringField("key2", "value2")
			builder.AddStringField("key3", "value3")
			builder.EndObject()
			_ = builder.Bytes()
		}
	})

	b.Run("AddRawJSONField", func(b *testing.B) {
		builder := New(512)
		rawJSON := []byte(`{"nested":"value","count":42}`)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			builder.Reset()
			builder.BeginObject()
			builder.AddStringField("source", "10.0.0.1")
			builder.AddRawJSONField("object", rawJSON)
			builder.AddStringField("raw", "test data")
			builder.EndObject()
			_ = builder.Bytes()
		}
	})

	b.Run("vs json.Marshal", func(b *testing.B) {
		type TestStruct struct {
			Source string                 `json:"source"`
			Object map[string]interface{} `json:"object"`
			Raw    string                 `json:"raw"`
		}

		data := TestStruct{
			Source: "10.0.0.1",
			Object: map[string]interface{}{
				"nested": "value",
				"count":  42,
			},
			Raw: "test data",
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = json.Marshal(data)
		}
	})
}
