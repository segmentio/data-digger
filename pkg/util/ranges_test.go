package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRanges(t *testing.T) {
	result, err := ParseRangeStr("1,2,4-6")
	assert.NoError(t, err)
	assert.Equal(
		t,
		result,
		map[int]struct{}{
			1: {},
			2: {},
			4: {},
			5: {},
			6: {},
		},
	)

	_, err = ParseRangeStr("not-parseable")
	assert.Error(t, err)

	_, err = ParseRangeStr("1,2,3-1")
	assert.Error(t, err)
}
