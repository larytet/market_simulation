package engine

import (
	"math"
	"sort"
	"time"

	"github.com/larytet/market_simulation/internal/book"
)

// path is one queue-position hypothesis of a resting order. The exact and pessimistic models have a single path.
type path struct {
	queueAhead int64 // shares ahead of us at our price level
	filled     int64 // shares filled since the order (re)entered the queue
}

type ownOrder struct {
	order       Order
	orderID     string
	side        book.Side
	price       book.Price
	quantity    int64 // total quantity (FIX tag 38)
	cumulative  int64
	leaves      int64
	value       float64 // sum of fill quantity * price
	addedShares int64
	commission  float64
	resting     bool
	mark        uint64 // orders with ArrivalSeq up to mark were ahead of us when we entered
	paths       []path
	entryLeaves int64 // leaves when the order entered the queue: the fill cap of a path
	reported    int64 // shares reported on the main stream since entering the queue
	expected    int64 // shares reported on the uncertain stream since entering the queue
}

type pendingCommand struct {
	arrival  time.Time
	sequence int
	run      func(arrival time.Time)
}

// Simulator is the Engine of one FIX session. Its methods are called with the market mutex NOT held and take it.
type Simulator struct {
	market      *Market
	name        string
	sink        ReportSink
	pending     []pendingCommand
	pendingSeq  int
	orders      map[string]*ownOrder
	resting     []*ownOrder // creation order, for deterministic iteration
	finished    map[string]string
	random      randomSource
	nextOrderID int
}

type randomSource interface{ Float64() float64 }

func (simulator *Simulator) config() *Config { return &simulator.market.config }

func (simulator *Simulator) schedule(arrival time.Time, run func(time.Time)) {
	simulator.pendingSeq++
	command := pendingCommand{arrival: arrival, sequence: simulator.pendingSeq, run: run}
	index := sort.Search(len(simulator.pending), func(i int) bool { return simulator.pending[i].arrival.After(arrival) })
	simulator.pending = append(simulator.pending, pendingCommand{})
	copy(simulator.pending[index+1:], simulator.pending[index:])
	simulator.pending[index] = command
}

// activate runs the commands that reached the book strictly before now.
func (simulator *Simulator) activate(now time.Time) {
	for len(simulator.pending) > 0 && simulator.pending[0].arrival.Before(now) {
		command := simulator.pending[0]
		simulator.pending = simulator.pending[1:]
		command.run(command.arrival)
	}
}

// ---- Engine interface ----

func (simulator *Simulator) known(clientOrderID string) bool {
	if _, found := simulator.orders[clientOrderID]; found {
		return true
	}
	_, found := simulator.finished[clientOrderID]
	return found
}

func (simulator *Simulator) NewOrder(order Order) {
	simulator.market.mutex.Lock()
	defer simulator.market.mutex.Unlock()
	if simulator.known(order.ClientOrderID) {
		simulator.sink.Send(Report{ExecType: ExecTypeRejected, Order: order, Text: "duplicate ClOrdID", EventTime: order.DecisionTime})
		return
	}
	simulator.schedule(order.DecisionTime.Add(simulator.config().OrderLatency), func(arrival time.Time) {
		simulator.enterNewOrder(order, arrival)
	})
}

func (simulator *Simulator) Cancel(request CancelRequest) {
	simulator.market.mutex.Lock()
	defer simulator.market.mutex.Unlock()
	simulator.schedule(request.DecisionTime.Add(simulator.config().CancelLatency), func(arrival time.Time) {
		simulator.cancelOrder(request, arrival)
	})
}

func (simulator *Simulator) Replace(request ReplaceRequest) {
	simulator.market.mutex.Lock()
	defer simulator.market.mutex.Unlock()
	simulator.schedule(request.DecisionTime.Add(simulator.config().CancelLatency), func(arrival time.Time) {
		simulator.replaceOrder(request, arrival)
	})
}

// ---- order entry ----

func (simulator *Simulator) enterNewOrder(order Order, arrival time.Time) {
	own := &ownOrder{
		order: order, orderID: simulator.market.newOrderID(simulator), side: book.Side(sideOf(order.Side)),
		price: book.ToPrice(order.Price), quantity: order.Quantity, leaves: order.Quantity,
	}
	simulator.orders[order.ClientOrderID] = own
	simulator.report(own, ExecTypeNew, arrival, Report{})
	simulator.place(own, arrival)
}

// sideOf maps the FIX side code to the book side: 1 buy, 2 sell, 5 sell short.
func sideOf(code string) byte {
	if code == "1" {
		return byte(book.Buy)
	}
	return byte(book.Sell)
}

// place sweeps the displayed opposite side when the order is marketable, then rests the remainder.
func (simulator *Simulator) place(own *ownOrder, now time.Time) {
	current := simulator.market.bookFor(own.order.Symbol)
	isMarket := own.order.OrderType == "1"
	opposite := current.Depth(own.side.Opposite(), 64)
	var available int64
	var takeable []book.Level
	for _, level := range opposite {
		if !isMarket && !crosses(own.side, own.price, level.Price) {
			break
		}
		takeable = append(takeable, level)
		available += level.Size
	}
	if own.order.TimeInForce == "4" && available < own.leaves { // fill or kill
		simulator.cancel(own, now, "FOK could not be filled in full")
		return
	}
	for _, level := range takeable {
		if own.leaves == 0 {
			break
		}
		simulator.fill(own, minimum(own.leaves, level.Size), level.Price, "R", 1, now)
	}
	if own.leaves == 0 {
		return
	}
	if isMarket || own.order.TimeInForce == "3" { // IOC remainder
		simulator.cancel(own, now, "")
		return
	}
	simulator.rest(own, current)
}

func crosses(side book.Side, limit, opposite book.Price) bool {
	if side == book.Buy {
		return opposite <= limit
	}
	return opposite >= limit
}

func (simulator *Simulator) rest(own *ownOrder, current *book.Book) {
	own.resting = true
	own.mark = current.ArrivalCounter()
	own.entryLeaves = own.leaves
	own.reported, own.expected = 0, 0
	queue := current.SizeAt(own.side, own.price)
	pathCount := 1
	if simulator.config().Model == ModelProbabilistic {
		pathCount = simulator.config().Paths
	}
	own.paths = make([]path, pathCount)
	for index := range own.paths {
		own.paths[index].queueAhead = queue
	}
	simulator.resting = append(simulator.resting, own)
}

func (simulator *Simulator) unrest(own *ownOrder) {
	own.resting = false
	for index, candidate := range simulator.resting {
		if candidate == own {
			simulator.resting = append(simulator.resting[:index], simulator.resting[index+1:]...)
			break
		}
	}
}

func (simulator *Simulator) cancelOrder(request CancelRequest, now time.Time) {
	own := simulator.orders[request.OriginalClientOrderID]
	if own == nil {
		simulator.rejectCancel(request.ClientOrderID, request.OriginalClientOrderID, "1", now)
		return
	}
	own.order.ClientOrderID = request.ClientOrderID
	simulator.cancelWithOriginal(own, request.OriginalClientOrderID, now, "")
}

func (simulator *Simulator) rejectCancel(clientOrderID, original, responseTo string, now time.Time) {
	reason, text := "1", "unknown order"
	if state := simulator.finished[original]; state != "" {
		reason, text = "0", "too late to cancel: order "+state
	}
	simulator.sink.Send(Report{
		CancelReject: true, ResponseTo: responseTo, ExecType: ExecTypeRejected,
		Order: Order{ClientOrderID: clientOrderID}, OriginalOrderID: original,
		RejectReason: reason, Text: text, EventTime: now,
	})
}

func (simulator *Simulator) cancel(own *ownOrder, now time.Time, text string) {
	simulator.cancelWithOriginal(own, "", now, text)
}

func (simulator *Simulator) cancelWithOriginal(own *ownOrder, original string, now time.Time, text string) {
	if own.resting {
		simulator.unrest(own)
	}
	key := own.order.ClientOrderID
	if original != "" {
		delete(simulator.orders, original)
		simulator.finished[original] = "canceled"
	} else {
		delete(simulator.orders, key)
	}
	simulator.finished[key] = "canceled"
	leaves := own.leaves
	own.leaves = 0
	simulator.report(own, ExecTypeCanceled, now, Report{OriginalOrderID: original, Text: text})
	own.leaves = leaves
}

func (simulator *Simulator) replaceOrder(request ReplaceRequest, now time.Time) {
	own := simulator.orders[request.OriginalClientOrderID]
	if own == nil {
		simulator.rejectCancel(request.ClientOrderID, request.OriginalClientOrderID, "2", now)
		return
	}
	newLeaves := request.Quantity - own.cumulative
	if newLeaves <= 0 {
		simulator.sink.Send(Report{
			CancelReject: true, ResponseTo: "2", ExecType: ExecTypeRejected,
			Order: Order{ClientOrderID: request.ClientOrderID}, OriginalOrderID: request.OriginalClientOrderID,
			RejectReason: "0", Text: "new quantity not above the filled quantity", EventTime: now,
		})
		return
	}
	newPrice := book.ToPrice(request.Price)
	keepsPriority := own.resting && newPrice == own.price && newLeaves <= own.leaves
	delete(simulator.orders, request.OriginalClientOrderID)
	simulator.finished[request.OriginalClientOrderID] = "replaced"
	own.order.ClientOrderID = request.ClientOrderID
	own.order.Price, own.order.Quantity = request.Price, request.Quantity
	own.quantity, own.leaves, own.price = request.Quantity, newLeaves, newPrice
	simulator.orders[request.ClientOrderID] = own
	if keepsPriority {
		own.entryLeaves = minimum(own.entryLeaves, own.leaves)
		simulator.report(own, ExecTypeReplaced, now, Report{OriginalOrderID: request.OriginalClientOrderID})
		return
	}
	if own.resting {
		simulator.unrest(own)
	}
	simulator.report(own, ExecTypeReplaced, now, Report{OriginalOrderID: request.OriginalClientOrderID})
	simulator.place(own, now)
}

func (simulator *Simulator) endOfDay(now time.Time) {
	for len(simulator.resting) > 0 {
		simulator.cancel(simulator.resting[0], now, "end of day")
	}
}

// ---- feed effects and the fill model ----

func (simulator *Simulator) onEffects(symbol string, effects []book.Effect, now time.Time) {
	if len(simulator.resting) == 0 {
		return
	}
	for _, effect := range effects {
		// iterate over a copy: a fill can finish an order and change the slice
		for _, own := range append([]*ownOrder(nil), simulator.resting...) {
			if own.order.Symbol != symbol || !own.resting {
				continue
			}
			simulator.applyEffect(own, effect, now)
		}
	}
}

func (simulator *Simulator) applyEffect(own *ownOrder, effect book.Effect, now time.Time) {
	atOurLevel := effect.Side == own.side && effect.Price == own.price
	switch effect.Kind {
	case book.EffectExecute:
		switch {
		case atOurLevel:
			for index := range own.paths {
				executeOnPath(&own.paths[index], effect.Shares, own.entryLeaves)
			}
		case tradedThrough(own, effect):
			// the whole level was consumed before reaching a worse price: we are filled at our price
			for index := range own.paths {
				own.paths[index].filled = own.entryLeaves
				own.paths[index].queueAhead = 0
			}
		default:
			return
		}
	case book.EffectCancel, book.EffectRemove:
		if !atOurLevel || effect.ArrivalSeq > own.mark {
			return // not our level, or the order was behind us
		}
		simulator.creditCancelAhead(own, effect.Shares)
	default:
		return
	}
	simulator.reportQueueFills(own, now)
}

func tradedThrough(own *ownOrder, effect book.Effect) bool {
	if own.side == book.Buy {
		return effect.Side == book.Buy && effect.Price < own.price
	}
	return effect.Side == book.Sell && effect.Price > own.price
}

// executeOnPath: executions take shares from the front of the level. What is left after the queue ahead
// is consumed fills us.
func executeOnPath(current *path, shares, cap int64) {
	current.queueAhead -= shares
	if current.queueAhead < 0 {
		current.filled = minimum(cap, current.filled-current.queueAhead)
		current.queueAhead = 0
	}
}

// creditCancelAhead advances our position when an order ahead of us leaves the queue.
func (simulator *Simulator) creditCancelAhead(own *ownOrder, shares int64) {
	probability := 0.0
	switch simulator.config().Model {
	case ModelExact:
		probability = 1
	case ModelProbabilistic:
		probability = simulator.config().CancelAheadProbability
	}
	for index := range own.paths {
		if probability >= 1 || (probability > 0 && simulator.random.Float64() < probability) {
			own.paths[index].queueAhead = maximum(0, own.paths[index].queueAhead-shares)
		}
	}
}

// reportQueueFills turns the path state into fills on the main stream (high probability) and the uncertain stream.
func (simulator *Simulator) reportQueueFills(own *ownOrder, now time.Time) {
	paths := own.paths
	if len(paths) == 1 {
		if delta := paths[0].filled - own.reported; delta > 0 {
			own.reported = paths[0].filled
			simulator.fill(own, delta, own.price, "A", 1, now)
		}
		return
	}
	filled := make([]int64, len(paths))
	var total int64
	for index := range paths {
		filled[index] = paths[index].filled
		total += filled[index]
	}
	sort.Slice(filled, func(i, j int) bool { return filled[i] < filled[j] })
	count := len(filled)
	// largest quantity reached on at least Threshold of the paths
	needed := int(math.Ceil(simulator.config().Threshold * float64(count)))
	quantile := filled[count-needed]
	if quantile > own.reported {
		probability := float64(count-sort.Search(count, func(i int) bool { return filled[i] >= quantile })) / float64(count)
		delta := quantile - own.reported
		own.reported = quantile
		own.expected = maximum(own.expected, quantile)
		simulator.fill(own, delta, own.price, "A", probability, now)
		return
	}
	expected := int64(math.Round(float64(total) / float64(count)))
	if expected > own.expected && simulator.config().Recorder != nil {
		positive := count - sort.Search(count, func(i int) bool { return filled[i] > 0 })
		report := Report{
			ExecType: ExecTypePartial, Order: own.order, OrderID: own.orderID,
			LastQuantity: expected - own.expected, LastPrice: own.price.Dollars(), Venue: DefaultVenue,
			FillProbability: float64(positive) / float64(count), QueueModel: string(ModelProbabilistic),
			Liquidity: "A", EventTime: now,
		}
		own.expected = expected
		simulator.config().Recorder.Record(simulator.name, report, true)
	}
}

// fill books quantity at price, updates the order and reports it.
func (simulator *Simulator) fill(own *ownOrder, quantity int64, price book.Price, liquidity string, probability float64, now time.Time) {
	previousCommission := own.commission
	own.cumulative += quantity
	own.leaves -= quantity
	own.value += float64(quantity) * price.Dollars()
	if liquidity == "A" {
		own.addedShares += quantity
	}
	own.commission = simulator.config().Fees.cumulative(own.cumulative, own.value, own.addedShares)
	execType := ExecTypePartial
	if own.leaves == 0 {
		execType = ExecTypeFill
	}
	report := Report{
		LastQuantity: quantity, LastPrice: price.Dollars(), FillProbability: probability,
		QueueModel: string(simulator.config().Model), Commission: own.commission - previousCommission, Liquidity: liquidity,
	}
	simulator.report(own, execType, now, report)
	if recorder := simulator.config().Recorder; recorder != nil {
		report.ExecType, report.Order, report.OrderID, report.Venue, report.EventTime = execType, own.order, own.orderID, DefaultVenue, now
		recorder.Record(simulator.name, report, false)
	}
	if own.leaves == 0 {
		if own.resting {
			simulator.unrest(own)
		}
		delete(simulator.orders, own.order.ClientOrderID)
		simulator.finished[own.order.ClientOrderID] = "filled"
	}
}

// report sends an execution report of the order to the algorithm.
func (simulator *Simulator) report(own *ownOrder, execType string, now time.Time, extra Report) {
	extra.ExecType = execType
	extra.Order = own.order
	extra.OrderID = own.orderID
	extra.CumulativeQty = own.cumulative
	extra.LeavesQty = own.leaves
	if own.cumulative > 0 {
		extra.AveragePrice = own.value / float64(own.cumulative)
	}
	extra.Venue = DefaultVenue
	extra.EventTime = now
	simulator.sink.Send(extra)
}

func minimum(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maximum(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
