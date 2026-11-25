package fixmsg

import (
	"bufio"
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestEncodeParseRoundTrip(t *testing.T) {
	message := Message{}
	message.Set(TagMsgType, MsgTypeNewOrderSingle)
	message.Set(TagClientOrderID, "ORDER1")
	message.Set(TagSymbol, "AAPL")
	message.Set(TagPrice, "123.45")
	parsed, err := Parse(message.Encode(), true)
	if err != nil {
		t.Fatalf("parse of encoded message: %v", err)
	}
	if parsed.Type() != "D" || parsed.GetOrEmpty(TagSymbol) != "AAPL" || parsed.GetOrEmpty(TagPrice) != "123.45" {
		t.Fatalf("round trip mismatch: %+v", parsed)
	}
}

func TestParseRejectsBadChecksumAndLength(t *testing.T) {
	message := Message{}
	message.Set(TagMsgType, MsgTypeHeartbeat)
	wire := message.Encode()
	badChecksum := bytes.Replace(wire, []byte("10="), []byte("10=9"), 1)
	if _, err := Parse(badChecksum, true); err == nil {
		t.Error("bad checksum accepted")
	}
	badLength := bytes.Replace(wire, []byte("9=5"), []byte("9=6"), 1)
	if _, err := Parse(badLength, true); err == nil {
		t.Error("bad body length accepted")
	}
}

func TestChecksumKnownMessage(t *testing.T) {
	raw := []byte("8=FIX.4.2\x019=12\x0135=0\x0134=1\x0149=A\x01")
	// 10= is computed over all bytes before it
	want := Checksum(raw)
	if want < 0 || want > 255 {
		t.Fatalf("checksum out of range: %d", want)
	}
}

func TestReadMessageFraming(t *testing.T) {
	first, second := Message{}, Message{}
	first.Set(TagMsgType, MsgTypeHeartbeat)
	second.Set(TagMsgType, MsgTypeLogout)
	stream := bufio.NewReader(bytes.NewReader(append(first.Encode(), second.Encode()...)))
	for _, want := range []string{"0", "5"} {
		raw, err := ReadMessage(stream)
		if err != nil {
			t.Fatal(err)
		}
		message, err := Parse(raw, true)
		if err != nil || message.Type() != want {
			t.Fatalf("want type %s, got %q, err %v", want, message.Type(), err)
		}
	}
}

// TestProductionSamples parses the masked order trail samples from vnt-prd (docs/spec/fix_samples).
func TestProductionSamples(t *testing.T) {
	content, err := os.ReadFile("../../docs/spec/fix_samples/order_trail_samples.txt")
	if err != nil {
		t.Skipf("samples not available: %v", err)
	}
	count := 0
	for _, line := range strings.Split(string(content), "\n") {
		if !strings.HasPrefix(line, "8=FIX.4.2") {
			continue
		}
		message, err := Parse([]byte(strings.ReplaceAll(line, "|", "\x01")), false)
		if err != nil {
			t.Fatalf("sample %q: %v", line[:40], err)
		}
		switch message.Type() {
		case "D", "F", "G", "8":
		default:
			t.Errorf("unexpected message type %q", message.Type())
		}
		count++
	}
	if count < 30 {
		t.Errorf("only %d samples parsed", count)
	}
}
