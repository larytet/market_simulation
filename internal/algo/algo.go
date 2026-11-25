// Package algo is a minimal trading algorithm that talks FIX 4.2 to the gateway like a production algorithm.
// It is used by the end to end test (docs/testplan.md, E2E-1) and shows how an algorithm connects.
package algo

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"time"

	. "github.com/larytet/market_simulation/internal/fixmsg"
)

// Client is a FIX initiator session.
type Client struct {
	connection net.Conn
	reader     *bufio.Reader
	sequence   int
	senderID   string
	targetID   string
}

// Dial connects and logs on.
func Dial(address, senderID, targetID string) (*Client, error) {
	connection, err := net.DialTimeout("tcp", address, 10*time.Second)
	if err != nil {
		return nil, err
	}
	client := &Client{connection: connection, reader: bufio.NewReader(connection), sequence: 1, senderID: senderID, targetID: targetID}
	logon := Message{}
	logon.Set(TagMsgType, MsgTypeLogon)
	logon.Set(TagEncryptMethod, "0")
	logon.Set(TagHeartBtInt, "30")
	client.Send(logon)
	reply, err := client.Receive(10 * time.Second)
	if err != nil {
		return nil, err
	}
	if reply.Type() != MsgTypeLogon {
		return nil, fmt.Errorf("expected Logon, got %q", reply.Type())
	}
	return client, nil
}

// Close drops the connection.
func (client *Client) Close() { client.connection.Close() }

// Send stamps the header and sends.
func (client *Client) Send(message Message) {
	message.Set(TagSenderCompID, client.senderID)
	message.Set(TagTargetCompID, client.targetID)
	message.Set(TagMsgSeqNum, strconv.Itoa(client.sequence))
	message.Set(TagSendingTime, time.Now().UTC().Format("20060102-15:04:05.000"))
	client.sequence++
	client.connection.Write(message.Encode())
}

// Receive waits for the next message.
func (client *Client) Receive(timeout time.Duration) (Message, error) {
	client.connection.SetReadDeadline(time.Now().Add(timeout))
	raw, err := ReadMessage(client.reader)
	if err != nil {
		return Message{}, err
	}
	return Parse(raw, true)
}

// NewLimitOrder builds a day limit order.
func NewLimitOrder(clientOrderID, symbol, side string, quantity int, price string) Message {
	order := Message{}
	order.Set(TagMsgType, MsgTypeNewOrderSingle)
	order.Set(TagClientOrderID, clientOrderID)
	order.Set(TagSymbol, symbol)
	order.Set(TagSide, side)
	order.Set(TagOrderQty, strconv.Itoa(quantity))
	order.Set(TagOrderType, "2")
	order.Set(TagPrice, price)
	order.Set(TagTimeInForce, "0")
	return order
}

// Fill is one fill seen by the algorithm.
type Fill struct {
	ClientOrderID string
	Quantity      int
	Price         string
	Commission    string
	Liquidity     string
}

// SampleResult is what RunSample saw.
type SampleResult struct {
	Fills    []Fill
	Canceled []string
}

// RunSample trades the sample feed of e2e/data (symbol TEST, 2026-11-20) with the exact fill model:
//
//	O1 buy 100 @ 10.00: rests behind 300 shares, fills when the queue ahead is gone and a later order trades.
//	O2 buy 50 @ 10.05: marketable against the 10.02 ask, fills at once at 10.02.
//	O3 buy 100 @ 9.99: rests; the algorithm cancels it after the acknowledgement.
//
// It returns when O1 is filled and O3 is cancelled, or fails after the timeout.
func RunSample(address, senderID, targetID string, timeout time.Duration) (SampleResult, error) {
	var result SampleResult
	client, err := Dial(address, senderID, targetID)
	if err != nil {
		return result, err
	}
	defer client.Close()
	client.Send(NewLimitOrder("O1", "TEST", "1", 100, "10.00"))
	client.Send(NewLimitOrder("O2", "TEST", "1", 50, "10.05"))
	client.Send(NewLimitOrder("O3", "TEST", "1", 100, "9.99"))
	deadline := time.Now().Add(timeout)
	o1Filled, o3Canceled, o2Filled := false, false, false
	for !(o1Filled && o2Filled && o3Canceled) {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return result, fmt.Errorf("timeout: O1 filled %v, O2 filled %v, O3 canceled %v, fills %+v", o1Filled, o2Filled, o3Canceled, result.Fills)
		}
		message, err := client.Receive(remaining)
		if err != nil {
			return result, err
		}
		if message.Type() != MsgTypeExecutionReport {
			continue
		}
		clientOrderID := message.GetOrEmpty(TagClientOrderID)
		switch message.GetOrEmpty(TagExecType) {
		case "0":
			if clientOrderID == "O3" {
				cancel := Message{}
				cancel.Set(TagMsgType, MsgTypeOrderCancelRequest)
				cancel.Set(TagClientOrderID, "C3")
				cancel.Set(TagOrigClientOrdID, "O3")
				cancel.Set(TagSymbol, "TEST")
				cancel.Set(TagSide, "1")
				client.Send(cancel)
			}
		case "1", "2":
			quantity, _ := strconv.Atoi(message.GetOrEmpty(TagLastShares))
			result.Fills = append(result.Fills, Fill{
				ClientOrderID: clientOrderID, Quantity: quantity, Price: message.GetOrEmpty(TagLastPrice),
				Commission: message.GetOrEmpty(TagCommission), Liquidity: message.GetOrEmpty(TagLiquidity),
			})
			if message.GetOrEmpty(TagExecType) == "2" {
				switch clientOrderID {
				case "O1":
					o1Filled = true
				case "O2":
					o2Filled = true
				}
			}
		case "4":
			result.Canceled = append(result.Canceled, message.GetOrEmpty(TagOrigClientOrdID))
			if message.GetOrEmpty(TagOrigClientOrdID) == "O3" {
				o3Canceled = true
			}
		case "8":
			return result, fmt.Errorf("order %s rejected: %s", clientOrderID, message.GetOrEmpty(TagText))
		}
	}
	return result, nil
}

// CheckSample compares the result of RunSample with the hand computed expectation of the exact model.
func CheckSample(result SampleResult) error {
	want := []Fill{
		{ClientOrderID: "O2", Quantity: 50, Price: "10.02", Commission: "1.0000", Liquidity: "R"},
		{ClientOrderID: "O1", Quantity: 100, Price: "10", Commission: "1.0000", Liquidity: "A"},
	}
	if len(result.Fills) != len(want) {
		return fmt.Errorf("want %d fills, got %+v", len(want), result.Fills)
	}
	for index := range want {
		if result.Fills[index] != want[index] {
			return fmt.Errorf("fill %d: want %+v, got %+v", index, want[index], result.Fills[index])
		}
	}
	if len(result.Canceled) != 1 || result.Canceled[0] != "O3" {
		return fmt.Errorf("want O3 canceled, got %v", result.Canceled)
	}
	return nil
}
