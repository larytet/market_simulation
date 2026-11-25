package gateway

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/larytet/market_simulation/internal/engine"
	. "github.com/larytet/market_simulation/internal/fixmsg"
)

const timeFormat = "20060102-15:04:05.000"

// session is one algorithm connection. Inbound messages are handled on the run goroutine;
// outbound messages (from the run goroutine, the engine and the heartbeat timer) are serialized by writeMutex.
type session struct {
	server     *Server
	connection net.Conn
	reader     *bufio.Reader

	writeMutex       sync.Mutex
	outgoingSeqNum   int
	lastOutboundTime time.Time

	counterpartyID    string
	expectedSeqNum    int
	heartbeatInterval time.Duration
	loggedOn          bool
	engine            engine.Engine
	executionCounter  int
}

func newSession(server *Server, connection net.Conn) *session {
	return &session{
		server: server, connection: connection, reader: bufio.NewReader(connection),
		outgoingSeqNum: 1, expectedSeqNum: 1,
	}
}

func (current *session) name() string {
	if current.counterpartyID == "" {
		return current.connection.RemoteAddr().String()
	}
	return current.counterpartyID
}

func (current *session) run() error {
	stopHeartbeats := make(chan struct{})
	defer close(stopHeartbeats)
	for {
		if current.heartbeatInterval > 0 {
			current.connection.SetReadDeadline(time.Now().Add(current.heartbeatInterval * 5 / 2))
		}
		raw, err := ReadMessage(current.reader)
		if err != nil {
			return err
		}
		message, err := Parse(raw, true)
		if err != nil {
			continue // a garbled message is ignored, FIX 4.2 session rules
		}
		if !current.loggedOn {
			if message.Type() != MsgTypeLogon {
				return fmt.Errorf("first message is %q, not Logon", message.Type())
			}
			if err := current.handleLogon(&message, stopHeartbeats); err != nil {
				return err
			}
			continue
		}
		done, err := current.handleMessage(&message)
		if err != nil || done {
			return err
		}
	}
}

func (current *session) handleLogon(message *Message, stopHeartbeats chan struct{}) error {
	counterparty := message.GetOrEmpty(TagSenderCompID)
	if message.GetOrEmpty(TagTargetCompID) != current.server.config.SenderCompID {
		return fmt.Errorf("logon for TargetCompID %q", message.GetOrEmpty(TagTargetCompID))
	}
	if allowed := current.server.config.AllowedCounterpartyIDs; len(allowed) > 0 {
		found := false
		for _, id := range allowed {
			found = found || id == counterparty
		}
		if !found {
			return fmt.Errorf("counterparty %q not allowed", counterparty)
		}
	}
	current.counterpartyID = counterparty
	current.heartbeatInterval = current.server.config.DefaultHeartbeatInterval
	if value, err := strconv.Atoi(message.GetOrEmpty(TagHeartBtInt)); err == nil && value > 0 {
		current.heartbeatInterval = time.Duration(value) * time.Second
	}
	if sequence, err := strconv.Atoi(message.GetOrEmpty(TagMsgSeqNum)); err == nil {
		current.expectedSeqNum = sequence + 1
	}
	current.engine = current.server.config.EngineFactory(counterparty, current)
	current.loggedOn = true
	reply := Message{}
	reply.Set(TagMsgType, MsgTypeLogon)
	reply.Set(TagEncryptMethod, "0")
	reply.Set(TagHeartBtInt, strconv.Itoa(int(current.heartbeatInterval/time.Second)))
	current.send(&reply)
	go current.heartbeatLoop(stopHeartbeats)
	if callback := current.server.config.OnLogon; callback != nil {
		callback(counterparty)
	}
	return nil
}

// handleMessage returns done true when the session must end.
func (current *session) handleMessage(message *Message) (done bool, err error) {
	if sequence, convertErr := strconv.Atoi(message.GetOrEmpty(TagMsgSeqNum)); convertErr == nil {
		switch {
		case sequence < current.expectedSeqNum && message.GetOrEmpty(TagPossDupFlag) != "Y":
			current.sendLogout(fmt.Sprintf("MsgSeqNum too low, expecting %d", current.expectedSeqNum))
			return true, nil
		case sequence > current.expectedSeqNum:
			request := Message{}
			request.Set(TagMsgType, MsgTypeResendRequest)
			request.Set(TagBeginSeqNo, strconv.Itoa(current.expectedSeqNum))
			request.Set(TagEndSeqNo, "0")
			current.send(&request)
		}
		if sequence >= current.expectedSeqNum {
			current.expectedSeqNum = sequence + 1
		}
	}
	switch message.Type() {
	case MsgTypeHeartbeat:
	case MsgTypeTestRequest:
		reply := Message{}
		reply.Set(TagMsgType, MsgTypeHeartbeat)
		reply.Set(TagTestReqID, message.GetOrEmpty(TagTestReqID))
		current.send(&reply)
	case MsgTypeLogout:
		current.sendLogout("")
		return true, nil
	case MsgTypeResendRequest, MsgTypeSequenceReset:
		// TODO: outbound message store and resend (the skeleton does not keep sent messages).
	case MsgTypeNewOrderSingle:
		current.handleNewOrder(message)
	case MsgTypeOrderCancelRequest:
		current.handleCancel(message)
	case MsgTypeOrderCancelReplace:
		current.handleReplace(message)
	default:
		current.sendReject(message, "unsupported message type")
	}
	return false, nil
}

func (current *session) heartbeatLoop(stop chan struct{}) {
	ticker := time.NewTicker(current.heartbeatInterval / 2)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			current.writeMutex.Lock()
			idle := time.Since(current.lastOutboundTime) >= current.heartbeatInterval
			current.writeMutex.Unlock()
			if idle {
				heartbeat := Message{}
				heartbeat.Set(TagMsgType, MsgTypeHeartbeat)
				current.send(&heartbeat)
			}
		}
	}
}

// send stamps the standard header and writes the message.
func (current *session) send(message *Message) {
	current.writeMutex.Lock()
	defer current.writeMutex.Unlock()
	message.Set(TagSenderCompID, current.server.config.SenderCompID)
	message.Set(TagTargetCompID, current.counterpartyID)
	message.Set(TagMsgSeqNum, strconv.Itoa(current.outgoingSeqNum))
	message.Set(TagSendingTime, current.server.config.Clock.Now().Format(timeFormat))
	current.outgoingSeqNum++
	current.lastOutboundTime = time.Now()
	current.connection.Write(message.Encode())
}

func (current *session) sendLogout(text string) {
	logout := Message{}
	logout.Set(TagMsgType, MsgTypeLogout)
	if text != "" {
		logout.Set(TagText, text)
	}
	current.send(&logout)
}

func (current *session) sendReject(offending *Message, text string) {
	reject := Message{}
	reject.Set(TagMsgType, MsgTypeReject)
	reject.Set(TagRefSeqNum, offending.GetOrEmpty(TagMsgSeqNum))
	reject.Set(TagText, text)
	current.send(&reject)
}
