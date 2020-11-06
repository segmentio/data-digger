package json

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/segmentio/data-digger/pkg/stats"
	sjson "github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

func init() {
	gjson.AddModifier("base64d", base64Decode)
	gjson.AddModifier("trim", trimString)
}

func base64Decode(json, arg string) string {
	if len(json) < 3 {
		return json
	}

	var strValue string
	if err := sjson.Unmarshal([]byte(json), &strValue); err != nil {
		log.Debugf("Error unmarshalling string: %+v", err)
		return json
	}

	decodedBytes, err := base64.StdEncoding.DecodeString(strValue)
	if err != nil {
		log.Debugf("Error base64 decoding string: %+v", err)
		return json
	}

	if arg == "" {
		// Just get the entire result as a json string
		result, err := sjson.Marshal(strings.TrimSpace(string(decodedBytes)))
		if err != nil {
			log.Debugf("Error marshalling string: %+v", err)
			return json
		}

		return string(result)
	}

	gjsonResult := gjson.GetBytes(decodedBytes, arg)
	if !gjsonResult.Exists() {
		return fmt.Sprintf(`"%s"`, stats.MissingValue)
	}
	result, err := sjson.Marshal(gjsonResult.String())
	if err != nil {
		return json
	}

	return string(result)
}

func trimString(json, arg string) string {
	maxLen, err := strconv.Atoi(arg)
	if err != nil {
		log.Debugf("Could not convert %s to int", arg)
		return json
	}

	if len(json) <= maxLen+2 {
		return json
	}

	var strValue string
	if err := sjson.Unmarshal([]byte(json), &strValue); err != nil {
		log.Debugf("Error unmarshalling string: %+v", err)
		return json
	}

	return fmt.Sprintf(`"%s"`, strValue[0:maxLen])
}

// GJsonPathValues returns the values associated with one or more gjson-formatted paths in
// the argument JSON blob.
func GJsonPathValues(contents []byte, pathGroups [][]string) []string {
	valueGroups := [][]string{}

	for _, pathGroup := range pathGroups {
		valueGroup := []string{}

		for _, path := range pathGroup {
			if path == "" {
				valueGroup = append(valueGroup, stats.AllValue)
				continue
			}

			result := gjson.GetBytes(contents, path)

			if result.Exists() {
				if result.IsArray() {
					for _, subResult := range result.Array() {
						value := subResult.String()
						valueGroup = append(valueGroup, value)
					}
				} else {
					value := result.String()
					valueGroup = append(valueGroup, value)
				}
			}
		}

		valueGroups = append(valueGroups, valueGroup)
	}

	if len(valueGroups) == 0 {
		return nil
	} else if len(valueGroups) == 1 {
		return valueGroups[0]
	}

	values := []string{}

	for _, valueGroup := range valueGroups {
		if len(valueGroup) == 0 {
			values = append(values, stats.MissingValue)
		} else if len(valueGroup) == 1 {
			values = append(values, valueGroup[0])
		} else {
			// TODO: Evaluate full cross-product?
			log.Debugf(
				"Found more than one value for multi-dimensional path query; dropping extra values",
			)
			values = append(values, valueGroup[0])
		}
	}

	// TODO: Keep sub-values instead of using a string to join them
	return []string{strings.Join(values, stats.DimSeparator)}
}
