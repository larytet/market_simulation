package engine

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/larytet/market_simulation/internal/book"
)

// FillModel selects how queue position is tracked (docs/spec FR-21).
type FillModel string

const (
	// ModelPessimistic: cancels ahead of us never advance us; only executions do. A lower bound on fills.
	ModelPessimistic FillModel = "pessimistic"
	// ModelExact: the order id of every cancel and execution says whether it was ahead of us.
	ModelExact FillModel = "exact"
	// ModelProbabilistic: cancels ahead advance us with a probability, Monte Carlo over the paths.
	ModelProbabilistic FillModel = "probabilistic"
)

// Config of the simulated market.
type Config struct {
	Model                  FillModel
	OrderLatency           time.Duration // order entry: decision to arrival at the book
	CancelLatency          time.Duration // cancel and replace
	Threshold              float64       // probabilistic: minimum probability of a fill on the main stream
	CancelAheadProbability float64       // probabilistic: chance that a cancel ahead of us advances us
	Paths                  int           // probabilistic: Monte Carlo paths
	Seed                   int64
	Fees                   FeeSchedule
	Recorder               Recorder
}

// Recorder receives every fill. uncertain is true for the expected-fill stream (probabilistic model), which is
// not sent to the algorithm.
type Recorder interface {
	Record(sessionName string, report Report, uncertain bool)
}

func (config *Config) applyDefaults() {
	if config.Model == "" {
		config.Model = ModelExact
	}
	if config.OrderLatency == 0 {
		config.OrderLatency = time.Millisecond
	}
	if config.CancelLatency == 0 {
		config.CancelLatency = 500 * time.Microsecond
	}
	if config.Threshold == 0 {
		config.Threshold = 0.8
	}
	if config.CancelAheadProbability == 0 {
		config.CancelAheadProbability = 0.5
	}
	if config.Paths == 0 {
		config.Paths = 200
	}
	if config.Fees == (FeeSchedule{}) {
		config.Fees = IBKRFixed
	}
}

// SimClock is the feed time. It moves only when the market processes an event.
type SimClock struct {
	nanoseconds atomic.Int64
}

// Now returns the feed time (UTC).
func (clock *SimClock) Now() time.Time { return time.Unix(0, clock.nanoseconds.Load()).UTC() }

// Set moves the clock.
func (clock *SimClock) Set(now time.Time) { clock.nanoseconds.Store(now.UnixNano()) }

// Market owns the order books of all symbols and the simulated sessions. All access is serialized by one mutex:
// the replay goroutine calls Process, the session goroutines call the Engine methods.
type Market struct {
	mutex      sync.Mutex
	config     Config
	clock      *SimClock
	books      map[string]*book.Book
	simulators []*Simulator
}

// NewMarket creates a market.
func NewMarket(config Config) *Market {
	config.applyDefaults()
	return &Market{config: config, clock: &SimClock{}, books: map[string]*book.Book{}}
}

// Clock is the feed clock the gateway stamps messages with.
func (market *Market) Clock() *SimClock { return market.clock }

// Book returns the book of the symbol (created empty on first use). For tests and diagnostics.
func (market *Market) Book(symbol string) *book.Book {
	market.mutex.Lock()
	defer market.mutex.Unlock()
	return market.bookFor(symbol)
}

func (market *Market) bookFor(symbol string) *book.Book {
	current := market.books[symbol]
	if current == nil {
		current = book.New()
		market.books[symbol] = current
	}
	return current
}

// Factory returns the engine factory for the gateway: one Simulator per FIX session.
func (market *Market) Factory() Factory {
	return func(sessionName string, sink ReportSink) Engine {
		market.mutex.Lock()
		defer market.mutex.Unlock()
		simulator := &Simulator{
			market: market, name: sessionName, sink: sink,
			orders: map[string]*ownOrder{}, finished: map[string]string{},
			random: rand.New(rand.NewSource(market.config.Seed + int64(len(market.simulators)))),
		}
		market.simulators = append(market.simulators, simulator)
		return simulator
	}
}

// Process applies one feed event: first the commands whose arrival time is before the event, then the event.
func (market *Market) Process(symbol string, event book.Event) {
	market.mutex.Lock()
	defer market.mutex.Unlock()
	market.clock.Set(event.Time)
	for _, simulator := range market.simulators {
		simulator.activate(event.Time)
	}
	effects := market.bookFor(symbol).Apply(event)
	for _, simulator := range market.simulators {
		simulator.onEffects(symbol, effects, event.Time)
	}
}

// Finish ends the day: pending commands are applied, resting day orders are cancelled.
func (market *Market) Finish() {
	market.mutex.Lock()
	defer market.mutex.Unlock()
	end := market.clock.Now()
	for _, simulator := range market.simulators {
		simulator.activate(end.Add(24 * time.Hour))
		simulator.endOfDay(end)
	}
}

func (market *Market) newOrderID(simulator *Simulator) string {
	simulator.nextOrderID++
	return fmt.Sprintf("SIM%08d", simulator.nextOrderID)
}
