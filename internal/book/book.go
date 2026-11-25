// Package book rebuilds a limit order book from the datafeedfs `stream` messages
// (add, execute, cancel, delete, replace) and reports, for every message, which order it touched
// and where that order stood in the queue. The fill model uses that to track own orders (docs/spec FR-1 to FR-3, FR-20).
package book

import (
	"math"
	"sort"
	"time"
)

// Price is in units of 1/10000 dollar, the ITCH price precision.
type Price int64

const PriceScale = 10000

// ToPrice converts dollars to Price.
func ToPrice(dollars float64) Price { return Price(math.Round(dollars * PriceScale)) }

// Dollars converts back to dollars.
func (price Price) Dollars() float64 { return float64(price) / PriceScale }

// Side of the book.
type Side byte

const (
	Buy  Side = 'B'
	Sell Side = 'S'
)

// Opposite returns the other side.
func (side Side) Opposite() Side {
	if side == Buy {
		return Sell
	}
	return Buy
}

// Event kinds, as in the stream file.
const (
	KindAdd            = 'A'
	KindExecute        = 'E' // execute at the order's price
	KindExecuteAtPrice = 'C' // execute at another price: Price is the execution price
	KindCancel         = 'X' // partial cancel: Shares cancelled
	KindDelete         = 'D'
	KindReplace        = 'U'
	KindNonDisplayed   = 'P'
	KindCross          = 'Q'
)

// Event is one stream row.
type Event struct {
	Time   time.Time
	Kind   byte
	Seq    int64
	Ref    uint64
	NewRef uint64
	Side   Side
	Price  Price
	Shares int64
}

// Order is a resting order of the recorded feed.
type Order struct {
	Ref        uint64
	Side       Side
	Price      Price
	Shares     int64
	ArrivalSeq uint64 // position in time priority: lower is earlier, across the whole book
}

// EffectKind says what an Effect describes.
type EffectKind int

const (
	EffectAdd         EffectKind = iota // a new order joined the back of its level
	EffectExecute                       // shares of a resting order traded
	EffectCancel                        // shares of a resting order were cancelled, the order stays
	EffectRemove                        // the order left the book: delete, or the old side of a replace
	EffectHiddenTrade                   // non-displayed trade: the book did not change
)

// Effect is the change one event made to one order.
type Effect struct {
	Kind       EffectKind
	Side       Side
	Price      Price // level price of the order; for an execute the trade price
	Shares     int64 // shares executed, cancelled or removed
	ArrivalSeq uint64
	Replaced   bool // EffectRemove caused by a replace
}

type level struct {
	price Price
	total int64
}

type sideBook struct {
	levels map[Price]*level
	prices []Price // sorted ascending
}

func newSideBook() *sideBook { return &sideBook{levels: map[Price]*level{}} }

func (sb *sideBook) add(price Price, shares int64) {
	current, found := sb.levels[price]
	if !found {
		current = &level{price: price}
		sb.levels[price] = current
		index := sort.Search(len(sb.prices), func(i int) bool { return sb.prices[i] >= price })
		sb.prices = append(sb.prices, 0)
		copy(sb.prices[index+1:], sb.prices[index:])
		sb.prices[index] = price
	}
	current.total += shares
}

func (sb *sideBook) remove(price Price, shares int64) {
	current := sb.levels[price]
	if current == nil {
		return
	}
	current.total -= shares
	if current.total <= 0 {
		delete(sb.levels, price)
		index := sort.Search(len(sb.prices), func(i int) bool { return sb.prices[i] >= price })
		if index < len(sb.prices) && sb.prices[index] == price {
			sb.prices = append(sb.prices[:index], sb.prices[index+1:]...)
		}
	}
}

// Level is a price and the displayed size there.
type Level struct {
	Price Price
	Size  int64
}

// Book is the order book of one symbol.
type Book struct {
	orders  map[uint64]*Order
	buys    *sideBook
	sells   *sideBook
	arrival uint64
	effects []Effect

	// Anomalies counts events that referred to an unknown order or removed more shares than the order held.
	// A day with many anomalies has an incomplete book (docs/spec FR-4).
	Anomalies int64
	Events    int64
}

// New creates an empty book.
func New() *Book {
	return &Book{orders: map[uint64]*Order{}, buys: newSideBook(), sells: newSideBook()}
}

func (b *Book) side(side Side) *sideBook {
	if side == Buy {
		return b.buys
	}
	return b.sells
}

// ArrivalCounter is the arrival sequence of the newest order; an own order entering now has priority behind
// every order with ArrivalSeq up to this value.
func (b *Book) ArrivalCounter() uint64 { return b.arrival }

// Best returns the best level of the side, and false when the side is empty.
func (b *Book) Best(side Side) (Level, bool) {
	sb := b.side(side)
	if len(sb.prices) == 0 {
		return Level{}, false
	}
	price := sb.prices[0]
	if side == Buy {
		price = sb.prices[len(sb.prices)-1]
	}
	return Level{Price: price, Size: sb.levels[price].total}, true
}

// SizeAt is the displayed size at the price.
func (b *Book) SizeAt(side Side, price Price) int64 {
	if current := b.side(side).levels[price]; current != nil {
		return current.total
	}
	return 0
}

// Depth returns up to count levels from the best price outward.
func (b *Book) Depth(side Side, count int) []Level {
	sb := b.side(side)
	var result []Level
	for step := 0; step < len(sb.prices) && step < count; step++ {
		price := sb.prices[step]
		if side == Buy {
			price = sb.prices[len(sb.prices)-1-step]
		}
		result = append(result, Level{Price: price, Size: sb.levels[price].total})
	}
	return result
}

// Apply updates the book with the event and returns the effects. The returned slice is reused by the next call.
func (b *Book) Apply(event Event) []Effect {
	b.Events++
	b.effects = b.effects[:0]
	switch event.Kind {
	case KindAdd:
		b.add(event.Ref, event.Side, event.Price, event.Shares)
	case KindExecute, KindExecuteAtPrice:
		order := b.lookup(event.Ref)
		if order == nil {
			return b.effects
		}
		shares := event.Shares
		if shares > order.Shares {
			b.Anomalies++
			shares = order.Shares
		}
		tradePrice := order.Price
		if event.Kind == KindExecuteAtPrice && event.Price != 0 {
			tradePrice = event.Price
		}
		b.effects = append(b.effects, Effect{Kind: EffectExecute, Side: order.Side, Price: tradePrice, Shares: shares, ArrivalSeq: order.ArrivalSeq})
		b.reduce(order, shares)
	case KindCancel:
		order := b.lookup(event.Ref)
		if order == nil {
			return b.effects
		}
		shares := event.Shares
		if shares > order.Shares {
			b.Anomalies++
			shares = order.Shares
		}
		kind := EffectCancel
		if shares == order.Shares {
			kind = EffectRemove
		}
		b.effects = append(b.effects, Effect{Kind: kind, Side: order.Side, Price: order.Price, Shares: shares, ArrivalSeq: order.ArrivalSeq})
		b.reduce(order, shares)
	case KindDelete:
		order := b.lookup(event.Ref)
		if order == nil {
			return b.effects
		}
		b.effects = append(b.effects, Effect{Kind: EffectRemove, Side: order.Side, Price: order.Price, Shares: order.Shares, ArrivalSeq: order.ArrivalSeq})
		b.reduce(order, order.Shares)
	case KindReplace:
		order := b.lookup(event.Ref)
		if order == nil {
			return b.effects
		}
		side := order.Side
		b.effects = append(b.effects, Effect{Kind: EffectRemove, Side: side, Price: order.Price, Shares: order.Shares, ArrivalSeq: order.ArrivalSeq, Replaced: true})
		b.reduce(order, order.Shares)
		b.add(event.NewRef, side, event.Price, event.Shares)
	case KindNonDisplayed:
		b.effects = append(b.effects, Effect{Kind: EffectHiddenTrade, Side: event.Side, Price: event.Price, Shares: event.Shares})
	}
	return b.effects
}

func (b *Book) lookup(ref uint64) *Order {
	order := b.orders[ref]
	if order == nil {
		b.Anomalies++
	}
	return order
}

func (b *Book) add(ref uint64, side Side, price Price, shares int64) {
	if shares <= 0 {
		b.Anomalies++
		return
	}
	b.arrival++
	order := &Order{Ref: ref, Side: side, Price: price, Shares: shares, ArrivalSeq: b.arrival}
	b.orders[ref] = order
	b.side(side).add(price, shares)
	b.effects = append(b.effects, Effect{Kind: EffectAdd, Side: side, Price: price, Shares: shares, ArrivalSeq: order.ArrivalSeq})
}

func (b *Book) reduce(order *Order, shares int64) {
	order.Shares -= shares
	b.side(order.Side).remove(order.Price, shares)
	if order.Shares <= 0 {
		delete(b.orders, order.Ref)
	}
}

// IsCrossed reports whether the best bid is at or above the best ask (a sign of a broken book).
func (b *Book) IsCrossed() bool {
	bid, hasBid := b.Best(Buy)
	ask, hasAsk := b.Best(Sell)
	return hasBid && hasAsk && bid.Price >= ask.Price
}
