package util

import (
	"fmt"
	"time"
)

// ParseTimeOrDuration converts an input string into a time. It first tries parsing it
// as an RFC3339 timestamp and then if that fails as a duration (relative to now).
func ParseTimeOrDuration(input string, now time.Time) (time.Time, error) {
	if input == "" {
		return time.Time{}, nil
	}

	timestamp, err := time.Parse(time.RFC3339, input)
	if err == nil {
		return timestamp, nil
	}

	duration, err := time.ParseDuration(input)
	if err != nil {
		return time.Time{}, fmt.Errorf(
			"Could not parse %s as either timestamp or duration",
			input,
		)
	}
	return now.Add(duration), nil
}
