package json

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBase64Decode(t *testing.T) {
	assert.Equal(
		t,
		`"abcd!!!"`,
		base64Decode(`"abcd!!!"`, ""),
	)
	assert.Equal(
		t,
		"abcd",
		base64Decode("abcd", ""),
	)
	assert.Equal(
		t,
		`"writeKey:"`,
		base64Decode(`"d3JpdGVLZXk6Cg=="`, ""),
	)
	assert.Equal(
		t,
		`"value1"`,
		base64Decode(`"eyJrZXkxIjoidmFsdWUxIn0K"`, "key1"),
	)
	assert.Equal(
		t,
		`"__missing__"`,
		base64Decode(`"eyJrZXkxIjoidmFsdWUxIn0K"`, "missing_key"),
	)
}

func TestTrim(t *testing.T) {
	assert.Equal(
		t,
		`"abcd"`,
		trimString(`"abcdefghij"`, "4"),
	)
	assert.Equal(
		t,
		`"abcdefghij"`,
		trimString(`"abcdefghij"`, "20"),
	)
	assert.Equal(
		t,
		`"abcdefghij"`,
		trimString(`"abcdefghij"`, "x"),
	)
	assert.Equal(
		t,
		`["abcdefghij"]`,
		trimString(`["abcdefghij"]`, "5"),
	)
}

func TestGJsonPathValues(t *testing.T) {
	assert.Equal(
		t,
		[]string{"value1"},
		GJsonPathValues(
			[]byte(`{"key1": "value1"}`),
			[][]string{{"key1"}},
		),
	)
	assert.Equal(
		t,
		[]string{},
		GJsonPathValues(
			[]byte(`{"key1": "value1"}`),
			[][]string{{"non-matching-key"}},
		),
	)
	assert.Equal(
		t,
		[]string{"value1", "value2", "value3"},
		GJsonPathValues(
			[]byte(`{"key1": ["value1", "value2"], "key2": "value3"}`),
			[][]string{{"key1", "key2"}},
		),
	)
	assert.Equal(
		t,
		[]string{"value1∪∪value2"},
		GJsonPathValues(
			[]byte(`{"key1": "value1", "key2": "value2"}`),
			[][]string{{"key1"}, {"key2", "key3"}},
		),
	)
	assert.Equal(
		t,
		[]string{"value1∪∪__missing__"},
		GJsonPathValues(
			[]byte(`{"key1": "value1", "key2": "value2"}`),
			[][]string{{"key1"}, {"non-matching-key", "key3"}},
		),
	)
	assert.Equal(
		t,
		[]string{"__missing__∪∪__missing__"},
		GJsonPathValues(
			[]byte(`{"key1": "value1", "key2": "value2"}`),
			[][]string{{"non-matching-key"}, {"non-matching-key"}},
		),
	)
}
