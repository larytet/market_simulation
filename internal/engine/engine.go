// Package engine defines what the FIX gateway needs from the order book simulation.
// The real book replay and fill model (docs/spec FR-1 to FR-25) will implement Engine.
package engine

import "time"

const DefaultVenue = "NASDAQ"

// Order is a new order from the algorithm.
type Order struct {
	ClientOrderID string
	Symbol        string
	Venue         string
	Side          string // FIX Side code: 1 buy, 2 sell, 5 sell short
	OrderType     string // FIX OrdType code: 2 limit, P pegged
	TimeInForce   string // FIX TimeInForce code
	Price         float64
	Quantity      int64
	DecisionTime  time.Time
}

// CancelRequest cancels the order OriginalClientOrderID.
type CancelRequest struct {
	ClientOrderID         string
	OriginalClientOrderID string
	DecisionTime          time.Time
}

// ReplaceRequest changes price and quantity of the order OriginalClientOrderID.
type ReplaceRequest struct {
	ClientOrderID         string
	OriginalClientOrderID string
	Price                 float64
	Quantity              int64
	DecisionTime          time.Time
}

// ExecType values (FIX tag 150) used in reports.
const (
	ExecTypeNew      = "0"
	ExecTypePartial  = "1"
	ExecTypeFill     = "2"
	ExecTypeCanceled = "4"
	ExecTypeReplaced = "5"
	ExecTypeRejected = "8"
)

// Report is an execution report, an order cancel reject or an order reject.
// The Order fields echo the state of the order the report is about.
type Report struct {
	ExecType        string
	Order           Order
	OrderID         string
	OriginalOrderID string // OrigClOrdID for cancel, replace and cancel reject
	LastQuantity    int64
	LastPrice       float64
	CumulativeQty   int64
	LeavesQty       int64
	AveragePrice    float64
	Venue           string
	Text            string
	CancelReject    bool   // true: send OrderCancelReject (35=9) instead of ExecutionReport
	ResponseTo      string // for CancelReject: 1 cancel request, 2 replace request (FIX tag 434)
	FillProbability float64
	QueueModel      string
	EventTime       time.Time
}

// ReportSink receives reports from the engine and delivers them to the algorithm session.
type ReportSink interface {
	Send(report Report)
}

// Engine is one simulated market for one algorithm session. Methods are called from the session
// goroutine in the order the algorithm sent the commands.
type Engine interface {
	NewOrder(order Order)
	Cancel(request CancelRequest)
	Replace(request ReplaceRequest)
}

// Factory creates an engine for a session, reporting to sink.
type Factory func(sessionName string, sink ReportSink) Engine
