package gateway

import (
	"bufio"
	"net"
	"strconv"
	"testing"
	"time"

	. "github.com/larytet/market_simulation/internal/fixmsg"
)

// testClient is a minimal FIX initiator.
type testClient struct {
	t        *testing.T
	conn     net.Conn
	reader   *bufio.Reader
	sequence int
}

func startGateway(t *testing.T) *testClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(Config{SenderCompID: "SIM"})
	go server.Serve(listener)
	t.Cleanup(func() { listener.Close(); server.Close() })
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	client := &testClient{t: t, conn: conn, reader: bufio.NewReader(conn), sequence: 1}
	logon := Message{}
	logon.Set(TagMsgType, MsgTypeLogon)
	logon.Set(TagEncryptMethod, "0")
	logon.Set(TagHeartBtInt, "30")
	client.send(logon)
	if reply := client.receive(); reply.Type() != MsgTypeLogon {
		t.Fatalf("expected Logon, got %q", reply.Type())
	}
	return client
}

func (client *testClient) send(message Message) {
	message.Set(TagSenderCompID, "ALGO")
	message.Set(TagTargetCompID, "SIM")
	message.Set(TagMsgSeqNum, strconv.Itoa(client.sequence))
	message.Set(TagSendingTime, "20261125-10:00:00.000")
	client.sequence++
	client.conn.Write(message.Encode())
}

func (client *testClient) receive() Message {
	client.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	raw, err := ReadMessage(client.reader)
	if err != nil {
		client.t.Fatalf("receive: %v", err)
	}
	message, err := Parse(raw, true)
	if err != nil {
		client.t.Fatalf("parse: %v", err)
	}
	return message
}

func newOrder(clientOrderID, price string) Message {
	order := Message{}
	order.Set(TagMsgType, MsgTypeNewOrderSingle)
	order.Set(TagClientOrderID, clientOrderID)
	order.Set(TagSymbol, "AAPL")
	order.Set(TagSide, "1")
	order.Set(TagOrderQty, "100")
	order.Set(TagOrderType, "2")
	order.Set(TagTimeInForce, "0")
	if price != "" {
		order.Set(TagPrice, price)
	}
	return order
}

func TestNewOrderAcknowledged(t *testing.T) {
	client := startGateway(t)
	client.send(newOrder("O1", "150.25"))
	report := client.receive()
	want := map[int]string{
		TagMsgType: "8", TagExecType: "0", TagOrdStatus: "0", TagClientOrderID: "O1",
		TagSymbol: "AAPL", TagSide: "1", TagOrderQty: "100", TagPrice: "150.25", TagLeavesQty: "100", TagCumQty: "0",
	}
	for tag, value := range want {
		if got := report.GetOrEmpty(tag); got != value {
			t.Errorf("tag %d: got %q want %q", tag, got, value)
		}
	}
	if report.GetOrEmpty(TagExecID) == "" || report.GetOrEmpty(TagTransactTime) == "" {
		t.Error("ExecID or TransactTime missing")
	}
}

func TestCancelAndReplaceAndUnknown(t *testing.T) {
	client := startGateway(t)
	client.send(newOrder("O1", "150.25"))
	client.receive()

	replace := Message{}
	replace.Set(TagMsgType, MsgTypeOrderCancelReplace)
	replace.Set(TagClientOrderID, "O2")
	replace.Set(TagOrigClientOrdID, "O1")
	replace.Set(TagOrderQty, "50")
	replace.Set(TagPrice, "150.30")
	client.send(replace)
	report := client.receive()
	if report.GetOrEmpty(TagExecType) != "5" || report.GetOrEmpty(TagOrigClientOrdID) != "O1" || report.GetOrEmpty(TagOrderQty) != "50" {
		t.Fatalf("replace report: %+v", report)
	}

	cancel := Message{}
	cancel.Set(TagMsgType, MsgTypeOrderCancelRequest)
	cancel.Set(TagClientOrderID, "C1")
	cancel.Set(TagOrigClientOrdID, "O2")
	client.send(cancel)
	if report := client.receive(); report.GetOrEmpty(TagExecType) != "4" || report.GetOrEmpty(TagOrigClientOrdID) != "O2" {
		t.Fatalf("cancel report: %+v", report)
	}

	cancel.Set(TagClientOrderID, "C2")
	cancel.Set(TagOrigClientOrdID, "NOPE")
	client.send(cancel)
	if reject := client.receive(); reject.Type() != MsgTypeOrderCancelReject || reject.GetOrEmpty(TagCxlRejResponseTo) != "1" {
		t.Fatalf("expected OrderCancelReject, got %+v", reject)
	}
}

func TestNewOrderRejections(t *testing.T) {
	client := startGateway(t)
	client.send(newOrder("O1", ""))
	if report := client.receive(); report.GetOrEmpty(TagExecType) != "8" {
		t.Errorf("limit order without price must be rejected: %+v", report)
	}
	other := newOrder("O2", "10")
	other.Set(TagSecurityExchange, "XNYS")
	client.send(other)
	if report := client.receive(); report.GetOrEmpty(TagExecType) != "8" || report.GetOrEmpty(TagText) != "venue not supported" {
		t.Errorf("other venue must be rejected: %+v", report)
	}
	nasdaq := newOrder("O3", "10")
	nasdaq.Set(TagSecurityExchange, "XNAS")
	client.send(nasdaq)
	if report := client.receive(); report.GetOrEmpty(TagExecType) != "0" {
		t.Errorf("Nasdaq venue must be accepted: %+v", report)
	}
}

func TestDuplicateClientOrderIDRejected(t *testing.T) {
	client := startGateway(t)
	client.send(newOrder("O1", "10"))
	client.receive()
	client.send(newOrder("O1", "10"))
	if report := client.receive(); report.GetOrEmpty(TagExecType) != "8" {
		t.Errorf("duplicate ClOrdID must be rejected: %+v", report)
	}
}

func TestTestRequestAnsweredWithHeartbeat(t *testing.T) {
	client := startGateway(t)
	request := Message{}
	request.Set(TagMsgType, MsgTypeTestRequest)
	request.Set(TagTestReqID, "PING")
	client.send(request)
	reply := client.receive()
	if reply.Type() != MsgTypeHeartbeat || reply.GetOrEmpty(TagTestReqID) != "PING" {
		t.Fatalf("expected Heartbeat PING, got %+v", reply)
	}
}

func TestSequenceNumberTooLowLogsOut(t *testing.T) {
	client := startGateway(t)
	client.sequence = 1 // repeat a used sequence number
	client.send(newOrder("O1", "10"))
	if reply := client.receive(); reply.Type() != MsgTypeLogout {
		t.Fatalf("expected Logout, got %+v", reply)
	}
}

func TestFirstMessageMustBeLogon(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	server := NewServer(Config{SenderCompID: "SIM"})
	go server.Serve(listener)
	defer listener.Close()
	defer server.Close()
	conn, _ := net.Dial("tcp", listener.Addr().String())
	defer conn.Close()
	order := newOrder("O1", "10")
	order.Set(TagSenderCompID, "ALGO")
	order.Set(TagTargetCompID, "SIM")
	order.Set(TagMsgSeqNum, "1")
	conn.Write(order.Encode())
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := bufio.NewReader(conn).ReadByte(); err == nil {
		t.Error("gateway answered a session that did not log on")
	}
}
