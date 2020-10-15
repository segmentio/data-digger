package proto

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/gogo/protobuf/jsonpb"
	proto "github.com/gogo/protobuf/proto"
	"github.com/segmentio/encoding/json"
)

// Decoder decodes protobuf messages to JSON.
type Decoder struct {
	typeInstances []proto.Message
}

// NewDecoder creates a new Decoder instance for the argument proto type names.
func NewDecoder(protoTypeNames []string) (*Decoder, error) {
	decoder := &Decoder{
		typeInstances: []proto.Message{},
	}

	for _, typeName := range protoTypeNames {
		if typeName == "" {
			continue
		}

		messageType := proto.MessageType(typeName)
		if messageType == nil {
			return nil, fmt.Errorf("Could not find message for type %s", typeName)
		}
		protoType := messageType.Elem()
		intPtr := reflect.New(protoType)
		instance, ok := intPtr.Interface().(proto.Message)
		if !ok {
			return nil, fmt.Errorf("Could not convert type to proto.Message: %+v", protoType)
		}

		decoder.typeInstances = append(decoder.typeInstances, instance)
	}

	return decoder, nil
}

// ToJSON converts the argument bytes to JSON bytes. If the former is already valid JSON, it's
// returned unchanged. Otherwise, it tries to decode the input using the proto types registered in
// this Decoder instance.
func (d *Decoder) ToJSON(contents []byte) ([]byte, error) {
	if json.Valid(contents) {
		return contents, nil
	}

	message, err := d.protoMessageFromBytes(contents)
	if err != nil || message == nil {
		return contents,
			fmt.Errorf("Message is neither JSON nor a recognized proto type")
	}

	m := &jsonpb.Marshaler{}
	buf := &bytes.Buffer{}
	err = m.Marshal(buf, message)
	if err != nil {
		return contents, err
	}

	return buf.Bytes(), nil
}

func (d *Decoder) protoMessageFromBytes(
	contents []byte,
) (proto.Message, error) {
	var err error

	for _, instance := range d.typeInstances {
		err = proto.Unmarshal(contents, instance)
		if err == nil {
			return instance, nil
		}
	}

	return nil, err
}
