package engine

import (
	"testing"
	"time"

	"github.com/larytet/market_simulation/internal/book"
)

type collectingSink struct{ reports []Report }

func (sink *collectingSink) Send(report Report) { sink.reports = append(sink.reports, report) }

func (sink *collectingSink) fills() []Report {
	var fills []Report
	for _, report := range sink.reports {
		if !report.CancelReject && (report.ExecType == ExecTypePartial || report.ExecType == ExecTypeFill) {
			fills = append(fills, report)
		}
	}
	return fills
}

var origin = time.Date(2026, 11, 20, 14, 30, 0, 0, time.UTC)

func at(milliseconds int) time.Time {
	return origin.Add(time.Duration(milliseconds) * time.Millisecond)
}

func event(milliseconds int, kind byte, ref uint64, side book.Side, price float64, shares int64) book.Event {
	return book.Event{Time: at(milliseconds), Kind: kind, Ref: ref, Side: side, Price: book.ToPrice(price), Shares: shares}
}

// scenario builds the market of the sample feed: bids 200+100 at 10.00, ask 500 at 10.02, bid 300 at 9.99.
func scenario(model FillModel) (*Market, *Simulator, *collectingSink) {
	market := NewMarket(Config{Model: model, Seed: 7, Fees: FeeSchedule{PerShare: 0.005, MinimumPerOrder: 1}})
	sink := &collectingSink{}
	simulator := market.Factory()("test", sink).(*Simulator)
	market.Clock().Set(at(0))
	for _, e := range []book.Event{
		event(0, 'A', 1, book.Buy, 10.00, 200), event(0, 'A', 2, book.Buy, 10.00, 100),
		event(0, 'A', 3, book.Sell, 10.02, 500), event(0, 'A', 4, book.Buy, 9.99, 300),
	} {
		market.Process("TEST", e)
	}
	return market, simulator, sink
}

func buy(clientOrderID string, quantity int64, price float64) Order {
	return Order{ClientOrderID: clientOrderID, Symbol: "TEST", Side: "1", OrderType: "2", TimeInForce: "0", Price: price, Quantity: quantity, DecisionTime: at(0)}
}

// playQueueSequence: execute 150 of ref1, cancel 50 of ref1, execute ref2 (100), add ref5 behind us, execute 120 of ref5.
func playQueueSequence(market *Market) {
	market.Process("TEST", event(100, 'E', 1, book.Buy, 10.00, 150))
	market.Process("TEST", event(200, 'X', 1, book.Buy, 0, 50))
	market.Process("TEST", event(400, 'E', 2, book.Buy, 10.00, 100))
	market.Process("TEST", event(500, 'A', 5, book.Buy, 10.00, 200))
	market.Process("TEST", event(600, 'E', 5, book.Buy, 10.00, 120))
}

func TestExactModelFillsWhenQueueAheadIsGone(t *testing.T) {
	market, simulator, sink := scenario(ModelExact)
	simulator.NewOrder(buy("O1", 100, 10.00))
	playQueueSequence(market)
	fills := sink.fills()
	if len(fills) != 1 || fills[0].LastQuantity != 100 || fills[0].ExecType != ExecTypeFill || fills[0].LastPrice != 10.00 {
		t.Fatalf("fills %+v", fills)
	}
	if !fills[0].EventTime.Equal(at(600)) || fills[0].FillProbability != 1 || fills[0].Liquidity != "A" {
		t.Fatalf("fill details %+v", fills[0])
	}
	if sink.reports[0].ExecType != ExecTypeNew || !sink.reports[0].EventTime.Equal(at(1)) {
		t.Fatalf("acknowledgement %+v", sink.reports[0])
	}
}

func TestPessimisticModelIgnoresCancelsAhead(t *testing.T) {
	market, simulator, sink := scenario(ModelPessimistic)
	simulator.NewOrder(buy("O1", 100, 10.00))
	playQueueSequence(market)
	// queue ahead 300 - 150 - 100 = 50 (the 50 cancelled shares are not credited); the 120 shares executed at 10.00 fill 70
	fills := sink.fills()
	if len(fills) != 1 || fills[0].LastQuantity != 70 || fills[0].ExecType != ExecTypePartial {
		t.Fatalf("fills %+v", fills)
	}
}

func TestProbabilisticModelBetweenPessimisticAndExact(t *testing.T) {
	market, simulator, sink := scenario(ModelProbabilistic)
	simulator.NewOrder(buy("O1", 100, 10.00))
	playQueueSequence(market)
	var total int64
	for _, fill := range sink.fills() {
		total += fill.LastQuantity
		if fill.FillProbability < 0.8 {
			t.Errorf("main stream fill below the threshold: %+v", fill)
		}
	}
	if total < 70 || total > 100 {
		t.Fatalf("high probability fill %d outside [70, 100]", total)
	}
}

func TestMarketableOrderTakesDisplayedLiquidity(t *testing.T) {
	market, simulator, sink := scenario(ModelExact)
	simulator.NewOrder(buy("O2", 50, 10.05))
	market.Process("TEST", event(100, 'A', 6, book.Sell, 10.03, 10))
	fills := sink.fills()
	if len(fills) != 1 || fills[0].LastQuantity != 50 || fills[0].LastPrice != 10.02 || fills[0].Liquidity != "R" || fills[0].Commission != 1 {
		t.Fatalf("fills %+v", fills)
	}
}

func TestIOCRemainderIsCancelled(t *testing.T) {
	market, simulator, sink := scenario(ModelExact)
	order := buy("O1", 800, 10.02)
	order.TimeInForce = "3"
	simulator.NewOrder(order)
	market.Process("TEST", event(100, 'A', 6, book.Sell, 10.03, 10))
	last := sink.reports[len(sink.reports)-1]
	if last.ExecType != ExecTypeCanceled || len(sink.fills()) != 1 || sink.fills()[0].LastQuantity != 500 {
		t.Fatalf("reports %+v", sink.reports)
	}
}

func TestTradeThroughFillsRegardlessOfQueue(t *testing.T) {
	market, simulator, sink := scenario(ModelPessimistic)
	simulator.NewOrder(buy("O1", 100, 10.00))
	market.Process("TEST", event(100, 'A', 6, book.Buy, 9.98, 10)) // activates the order
	// a sell hits a bid at 9.98, below ours: our level must have been exhausted first
	market.Process("TEST", event(200, 'E', 6, book.Buy, 9.98, 10))
	fills := sink.fills()
	if len(fills) != 1 || fills[0].LastQuantity != 100 || fills[0].LastPrice != 10.00 {
		t.Fatalf("fills %+v", fills)
	}
}

func TestCancelLatencyRace(t *testing.T) {
	market, simulator, sink := scenario(ModelExact)
	simulator.NewOrder(buy("O1", 100, 10.00))
	market.Process("TEST", event(100, 'A', 6, book.Sell, 10.03, 10))
	// the algorithm decides to cancel at 0.550 s; the cancel reaches the book at 0.5505 s, after the fill at 0.5 s
	market.Process("TEST", event(150, 'E', 1, book.Buy, 10.00, 200))
	market.Process("TEST", event(160, 'E', 2, book.Buy, 10.00, 100))
	market.Process("TEST", event(170, 'A', 7, book.Buy, 10.00, 500))
	market.Process("TEST", event(180, 'E', 7, book.Buy, 10.00, 100))
	simulator.Cancel(CancelRequest{ClientOrderID: "C1", OriginalClientOrderID: "O1", DecisionTime: at(550)})
	market.Process("TEST", event(600, 'A', 8, book.Sell, 10.04, 1))
	last := sink.reports[len(sink.reports)-1]
	if !last.CancelReject || last.RejectReason != "0" {
		t.Fatalf("cancel after fill must be rejected as too late: %+v", last)
	}
}

func TestReplaceLosesPriorityOnPriceChange(t *testing.T) {
	market, simulator, sink := scenario(ModelExact)
	simulator.NewOrder(buy("O1", 100, 10.00))
	market.Process("TEST", event(100, 'A', 6, book.Sell, 10.03, 10))
	simulator.Replace(ReplaceRequest{ClientOrderID: "R1", OriginalClientOrderID: "O1", Price: 9.99, Quantity: 100, DecisionTime: at(100)})
	market.Process("TEST", event(200, 'A', 7, book.Sell, 10.04, 10))
	var replaced bool
	for _, report := range sink.reports {
		replaced = replaced || report.ExecType == ExecTypeReplaced
	}
	if !replaced || simulator.orders["R1"] == nil || simulator.orders["R1"].paths[0].queueAhead != 300 {
		t.Fatalf("replace to 9.99 must queue behind the 300 shares there: %+v", sink.reports)
	}
}

func TestEndOfDayCancelsRestingOrders(t *testing.T) {
	market, simulator, sink := scenario(ModelExact)
	simulator.NewOrder(buy("O1", 100, 9.99))
	market.Process("TEST", event(100, 'A', 6, book.Sell, 10.03, 10))
	market.Finish()
	last := sink.reports[len(sink.reports)-1]
	if last.ExecType != ExecTypeCanceled || last.Text != "end of day" {
		t.Fatalf("last report %+v", last)
	}
}

func TestDuplicateClientOrderID(t *testing.T) {
	market, simulator, sink := scenario(ModelExact)
	simulator.NewOrder(buy("O1", 100, 9.99))
	market.Process("TEST", event(100, 'A', 6, book.Sell, 10.03, 10))
	simulator.NewOrder(buy("O1", 100, 9.99))
	if last := sink.reports[len(sink.reports)-1]; last.ExecType != ExecTypeRejected {
		t.Fatalf("duplicate not rejected: %+v", last)
	}
}
