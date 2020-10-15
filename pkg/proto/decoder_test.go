package proto

import (
	"testing"

	proto "github.com/gogo/protobuf/proto"
	pb "github.com/segmentio/data-digger/pkg/proto/protobuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder(t *testing.T) {
	type testCase struct {
		input          []byte
		expectedOutput []byte
		expectedErr    bool
	}

	testCases := []testCase{
		{
			input:          []byte(`{"key":"value"}`),
			expectedOutput: []byte(`{"key":"value"}`),
		},
		{
			input:          []byte(`bad json`),
			expectedOutput: []byte(`bad json`),
			expectedErr:    true,
		},
		{
			input:          protoToBytes(t, &pb.TestMessage1{Key1: "value1"}),
			expectedOutput: []byte(`{"key1":"value1"}`),
		},
	}

	decoder, err := NewDecoder(
		[]string{
			"TestMessage1",
			"TestMessage2",
		},
	)
	require.NoError(t, err)

	for _, testCase := range testCases {
		result, err := decoder.ToJSON(testCase.input)
		if testCase.expectedErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
		assert.Equal(t, testCase.expectedOutput, result)
	}
}

func protoToBytes(t *testing.T, msg proto.Message) []byte {
	contents, err := proto.Marshal(msg)
	require.NoError(t, err)
	return contents
}
