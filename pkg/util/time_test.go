package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseTimeOrDuration(t *testing.T) {
	now := time.Date(2020, 10, 28, 20, 30, 5, 0, time.UTC)

	result, err := ParseTimeOrDuration("", now)
	assert.NoError(t, err)
	assert.True(t, result.IsZero())

	result, err = ParseTimeOrDuration("-25m", now)
	assert.NoError(t, err)
	assert.Equal(t, now.Add(-25*time.Minute), result)

	result, err = ParseTimeOrDuration("2020-10-29T10:12:44Z", now)
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2020, 10, 29, 10, 12, 44, 0, time.UTC), result)

	result, err = ParseTimeOrDuration("bad time", now)
	assert.Error(t, err)
	assert.True(t, result.IsZero())
}
