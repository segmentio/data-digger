package util

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var rangeRegexp = regexp.MustCompile(`^([0-9]+)(-([0-9]+))?$`)

// ParseRangeStr parses range strings like "1-2" or "3,5-6".
func ParseRangeStr(rangeStr string) (map[int]struct{}, error) {
	if rangeStr == "" {
		return nil, nil
	}

	valuesMap := map[int]struct{}{}

	components := strings.Split(rangeStr, ",")

	for _, component := range components {
		groups := rangeRegexp.FindStringSubmatch(component)

		if len(groups) != 4 {
			return nil, fmt.Errorf("Invalid range string: %s", rangeStr)
		}

		if groups[2] == "" {
			// Single number
			value, err := strconv.Atoi(groups[1])
			if err != nil {
				return nil, err
			}
			valuesMap[value] = struct{}{}
		} else {
			// Number range
			startValue, err := strconv.Atoi(groups[1])
			if err != nil {
				return nil, err
			}
			endValue, err := strconv.Atoi(groups[3])
			if err != nil {
				return nil, err
			}

			if startValue > endValue {
				return nil, fmt.Errorf("Range start cannot be greater than end")
			}

			for i := startValue; i <= endValue; i++ {
				valuesMap[i] = struct{}{}
			}
		}
	}

	return valuesMap, nil
}
