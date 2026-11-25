package gateway

import (
	"fmt"
	"strconv"

	"github.com/larytet/market_simulation/internal/engine"
	. "github.com/larytet/market_simulation/internal/fixmsg"
)

// supportedVenues are the SecurityExchange / ExDestination values accepted in version 1 (FR-80).
var supportedVenues = map[string]bool{"": true, "NASDAQ": true, "XNAS": true, "NSDQ": true}

func (current *session) handleNewOrder(message *Message) {
	order := engine.Order{
		ClientOrderID: message.GetOrEmpty(TagClientOrderID),
		Symbol:        message.GetOrEmpty(TagSymbol),
		Side:          message.GetOrEmpty(TagSide),
		OrderType:     message.GetOrEmpty(TagOrderType),
		TimeInForce:   message.GetOrEmpty(TagTimeInForce),
		DecisionTime:  current.server.config.Clock.Now(),
	}
	if reason := current.validateNewOrder(message, &order); reason != "" {
		current.sendReport(engine.Report{ExecType: engine.ExecTypeRejected, Order: order, Text: reason, EventTime: order.DecisionTime})
		return
	}
	order.Venue = engine.DefaultVenue
	current.engine.NewOrder(order)
}

func (current *session) validateNewOrder(message *Message, order *engine.Order) string {
	for _, required := range []struct {
		tag  int
		name string
	}{{TagClientOrderID, "ClOrdID"}, {TagSymbol, "Symbol"}, {TagSide, "Side"}, {TagOrderQty, "OrderQty"}, {TagOrderType, "OrdType"}} {
		if _, found := message.Get(required.tag); !found {
			return fmt.Sprintf("missing %s (%d)", required.name, required.tag)
		}
	}
	quantity, err := strconv.ParseInt(message.GetOrEmpty(TagOrderQty), 10, 64)
	if err != nil || quantity <= 0 {
		return "bad OrderQty"
	}
	order.Quantity = quantity
	if value, found := message.Get(TagPrice); found {
		price, err := strconv.ParseFloat(value, 64)
		if err != nil || price <= 0 {
			return "bad Price"
		}
		order.Price = price
	} else if order.OrderType == "2" {
		return "limit order without Price (44)"
	}
	for _, tag := range []int{TagSecurityExchange, TagExDestination} {
		if venue := message.GetOrEmpty(tag); !supportedVenues[venue] {
			return "venue not supported"
		}
	}
	return ""
}

func (current *session) handleCancel(message *Message) {
	request := engine.CancelRequest{
		ClientOrderID:         message.GetOrEmpty(TagClientOrderID),
		OriginalClientOrderID: message.GetOrEmpty(TagOrigClientOrdID),
		DecisionTime:          current.server.config.Clock.Now(),
	}
	if request.OriginalClientOrderID == "" {
		current.sendReject(message, "missing OrigClOrdID (41)")
		return
	}
	current.engine.Cancel(request)
}

func (current *session) handleReplace(message *Message) {
	request := engine.ReplaceRequest{
		ClientOrderID:         message.GetOrEmpty(TagClientOrderID),
		OriginalClientOrderID: message.GetOrEmpty(TagOrigClientOrdID),
		DecisionTime:          current.server.config.Clock.Now(),
	}
	quantity, quantityErr := strconv.ParseInt(message.GetOrEmpty(TagOrderQty), 10, 64)
	price, priceErr := strconv.ParseFloat(message.GetOrEmpty(TagPrice), 64)
	if request.OriginalClientOrderID == "" || quantityErr != nil || quantity <= 0 || priceErr != nil || price <= 0 {
		current.sendReject(message, "replace needs OrigClOrdID (41), OrderQty (38) and Price (44)")
		return
	}
	request.Quantity, request.Price = quantity, price
	current.engine.Replace(request)
}

// Send implements engine.ReportSink: it turns an engine report into a FIX message.
func (current *session) Send(report engine.Report) { current.sendReport(report) }

func (current *session) sendReport(report engine.Report) {
	message := Message{}
	order := report.Order
	message.Set(TagClientOrderID, order.ClientOrderID)
	if report.OriginalOrderID != "" {
		message.Set(TagOrigClientOrdID, report.OriginalOrderID)
	}
	orderID := report.OrderID
	if orderID == "" {
		orderID = "NONE"
	}
	message.Set(TagOrderID, orderID)
	if report.CancelReject {
		message.Set(TagMsgType, MsgTypeOrderCancelReject)
		message.Set(TagOrdStatus, "8")
		message.Set(TagCxlRejResponseTo, report.ResponseTo)
		reason := report.RejectReason
		if reason == "" {
			reason = "1"
		}
		message.Set(TagCxlRejReason, reason)
		message.Set(TagText, report.Text)
		current.send(&message)
		return
	}
	current.executionCounter++
	message.Set(TagMsgType, MsgTypeExecutionReport)
	message.Set(TagExecID, fmt.Sprintf("E%010d", current.executionCounter))
	message.Set(TagExecType, report.ExecType)
	message.Set(TagOrdStatus, report.ExecType) // FIX 4.2: same code for the states the simulator produces
	message.Set(TagSymbol, order.Symbol)
	message.Set(TagSide, order.Side)
	message.Set(TagOrderQty, strconv.FormatInt(order.Quantity, 10))
	message.Set(TagOrderType, order.OrderType)
	if order.Price > 0 {
		message.Set(TagPrice, strconv.FormatFloat(order.Price, 'f', -1, 64))
	}
	if order.TimeInForce != "" {
		message.Set(TagTimeInForce, order.TimeInForce)
	}
	message.Set(TagCumQty, strconv.FormatInt(report.CumulativeQty, 10))
	message.Set(TagLeavesQty, strconv.FormatInt(report.LeavesQty, 10))
	message.Set(TagAvgPrice, strconv.FormatFloat(report.AveragePrice, 'f', -1, 64))
	if report.LastQuantity > 0 {
		message.Set(TagLastShares, strconv.FormatInt(report.LastQuantity, 10))
		message.Set(TagLastPrice, strconv.FormatFloat(report.LastPrice, 'f', -1, 64))
	}
	if report.Venue != "" {
		message.Set(TagLastMarket, report.Venue)
	}
	if report.ExecType == engine.ExecTypeRejected {
		message.Set(TagOrdRejReason, "0")
	}
	if report.Text != "" {
		message.Set(TagText, report.Text)
	}
	if report.LastQuantity > 0 {
		message.Set(TagCommission, strconv.FormatFloat(report.Commission, 'f', 4, 64))
		message.Set(TagCommType, "3")
		if report.Liquidity != "" {
			message.Set(TagLiquidity, report.Liquidity)
		}
	}
	if report.FillProbability > 0 {
		message.Set(TagFillProbability, strconv.FormatFloat(report.FillProbability, 'f', 4, 64))
		message.Set(TagQueueModel, report.QueueModel)
	}
	if !report.EventTime.IsZero() {
		message.Set(TagTransactTime, report.EventTime.Format("20060102-15:04:05.000000"))
	}
	current.send(&message)
}
