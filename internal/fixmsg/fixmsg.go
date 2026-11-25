// Package fixmsg is a minimal FIX 4.2 tag=value codec: parse, encode, checksum and stream framing.
package fixmsg

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

const (
	Delimiter   = '\x01'
	BeginString = "FIX.4.2"
)

// Field is one tag=value pair.
type Field struct {
	Tag   int
	Value string
}

// Message keeps fields in wire order. Tags 8, 9 and 10 are not stored: Encode computes them.
type Message struct {
	Fields []Field
}

// Get returns the value of the first field with the tag.
func (message *Message) Get(tag int) (string, bool) {
	for _, field := range message.Fields {
		if field.Tag == tag {
			return field.Value, true
		}
	}
	return "", false
}

// GetOrEmpty returns the value of the tag, or an empty string.
func (message *Message) GetOrEmpty(tag int) string {
	value, _ := message.Get(tag)
	return value
}

// Set replaces the first field with the tag, or appends a new one.
func (message *Message) Set(tag int, value string) {
	for index := range message.Fields {
		if message.Fields[index].Tag == tag {
			message.Fields[index].Value = value
			return
		}
	}
	message.Fields = append(message.Fields, Field{Tag: tag, Value: value})
}

// Type returns tag 35, MsgType.
func (message *Message) Type() string {
	return message.GetOrEmpty(TagMsgType)
}

// Checksum is the sum of all bytes modulo 256.
func Checksum(data []byte) int {
	sum := 0
	for _, value := range data {
		sum += int(value)
	}
	return sum % 256
}

// Encode produces a wire message with tag 35 first, then the other fields in order,
// with computed BodyLength (9) and CheckSum (10).
func (message *Message) Encode() []byte {
	var body bytes.Buffer
	writeField(&body, TagMsgType, message.Type())
	for _, field := range message.Fields {
		switch field.Tag {
		case TagBeginString, TagBodyLength, TagCheckSum, TagMsgType:
			continue
		}
		writeField(&body, field.Tag, field.Value)
	}
	var wire bytes.Buffer
	writeField(&wire, TagBeginString, BeginString)
	writeField(&wire, TagBodyLength, strconv.Itoa(body.Len()))
	wire.Write(body.Bytes())
	writeField(&wire, TagCheckSum, fmt.Sprintf("%03d", Checksum(wire.Bytes())))
	return wire.Bytes()
}

func writeField(buffer *bytes.Buffer, tag int, value string) {
	buffer.WriteString(strconv.Itoa(tag))
	buffer.WriteByte('=')
	buffer.WriteString(value)
	buffer.WriteByte(Delimiter)
}

// Parse decodes a wire message. With validate, BodyLength and CheckSum must be correct.
// Messages from masked samples are parsed with validate false.
func Parse(raw []byte, validate bool) (Message, error) {
	var message Message
	parts := bytes.Split(raw, []byte{Delimiter})
	bodyStart, bodyEnd := -1, -1
	offset := 0
	declaredLength, declaredChecksum := -1, -1
	for _, part := range parts {
		partStart := offset
		offset += len(part) + 1
		if len(part) == 0 {
			continue
		}
		separator := bytes.IndexByte(part, '=')
		if separator <= 0 {
			return message, fmt.Errorf("malformed field %q", part)
		}
		tag, err := strconv.Atoi(string(part[:separator]))
		if err != nil {
			return message, fmt.Errorf("bad tag in %q", part)
		}
		value := string(part[separator+1:])
		switch tag {
		case TagBeginString:
			if value != BeginString {
				return message, fmt.Errorf("unsupported BeginString %q", value)
			}
		case TagBodyLength:
			declaredLength, _ = strconv.Atoi(value)
			bodyStart = offset
		case TagCheckSum:
			declaredChecksum, _ = strconv.Atoi(value)
			bodyEnd = partStart
		default:
			message.Fields = append(message.Fields, Field{Tag: tag, Value: value})
		}
	}
	if message.Type() == "" {
		return message, errors.New("missing MsgType (35)")
	}
	if validate {
		if bodyStart < 0 || bodyEnd < 0 {
			return message, errors.New("missing BodyLength (9) or CheckSum (10)")
		}
		if bodyEnd-bodyStart != declaredLength {
			return message, fmt.Errorf("body length %d, declared %d", bodyEnd-bodyStart, declaredLength)
		}
		if got := Checksum(raw[:bodyEnd]); got != declaredChecksum {
			return message, fmt.Errorf("checksum %d, declared %d", got, declaredChecksum)
		}
	}
	return message, nil
}

// ReadMessage reads one framed message: it ends at the CheckSum (10) field.
func ReadMessage(reader *bufio.Reader) ([]byte, error) {
	var raw []byte
	for {
		field, err := reader.ReadBytes(Delimiter)
		raw = append(raw, field...)
		if err != nil {
			return nil, err
		}
		if bytes.HasPrefix(field, []byte("10=")) {
			return raw, nil
		}
	}
}
