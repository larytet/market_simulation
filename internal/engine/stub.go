package engine

import (
	"fmt"
	"sync"
)

// StubEngine accepts, cancels and replaces orders and never fills them.
// It lets the gateway be tested end to end before the book simulation exists.
type StubEngine struct {
	sink   ReportSink
	mutex  sync.Mutex
	orders map[string]Order
	nextID int
}

// NewStubFactory returns a Factory of StubEngine.
func NewStubFactory() Factory {
	return func(sessionName string, sink ReportSink) Engine {
		return &StubEngine{sink: sink, orders: map[string]Order{}}
	}
}

func (stub *StubEngine) NewOrder(order Order) {
	stub.mutex.Lock()
	defer stub.mutex.Unlock()
	if _, exists := stub.orders[order.ClientOrderID]; exists {
		stub.sink.Send(Report{ExecType: ExecTypeRejected, Order: order, Text: "duplicate ClOrdID", EventTime: order.DecisionTime})
		return
	}
	stub.nextID++
	orderID := fmt.Sprintf("S%08d", stub.nextID)
	stub.orders[order.ClientOrderID] = order
	stub.sink.Send(Report{
		ExecType: ExecTypeNew, Order: order, OrderID: orderID,
		LeavesQty: order.Quantity, Venue: DefaultVenue, EventTime: order.DecisionTime,
	})
}

func (stub *StubEngine) Cancel(request CancelRequest) {
	stub.mutex.Lock()
	defer stub.mutex.Unlock()
	order, found := stub.orders[request.OriginalClientOrderID]
	if !found {
		stub.sink.Send(Report{
			CancelReject: true, ResponseTo: "1", ExecType: ExecTypeRejected,
			Order:           Order{ClientOrderID: request.ClientOrderID},
			OriginalOrderID: request.OriginalClientOrderID,
			Text:            "unknown order", EventTime: request.DecisionTime,
		})
		return
	}
	delete(stub.orders, request.OriginalClientOrderID)
	order.ClientOrderID = request.ClientOrderID
	stub.sink.Send(Report{
		ExecType: ExecTypeCanceled, Order: order,
		OriginalOrderID: request.OriginalClientOrderID, Venue: DefaultVenue, EventTime: request.DecisionTime,
	})
}

func (stub *StubEngine) Replace(request ReplaceRequest) {
	stub.mutex.Lock()
	defer stub.mutex.Unlock()
	order, found := stub.orders[request.OriginalClientOrderID]
	if !found {
		stub.sink.Send(Report{
			CancelReject: true, ResponseTo: "2", ExecType: ExecTypeRejected,
			Order:           Order{ClientOrderID: request.ClientOrderID},
			OriginalOrderID: request.OriginalClientOrderID,
			Text:            "unknown order", EventTime: request.DecisionTime,
		})
		return
	}
	delete(stub.orders, request.OriginalClientOrderID)
	order.ClientOrderID = request.ClientOrderID
	order.Price = request.Price
	order.Quantity = request.Quantity
	stub.orders[order.ClientOrderID] = order
	stub.sink.Send(Report{
		ExecType: ExecTypeReplaced, Order: order, OriginalOrderID: request.OriginalClientOrderID,
		LeavesQty: order.Quantity, Venue: DefaultVenue, EventTime: request.DecisionTime,
	})
}
