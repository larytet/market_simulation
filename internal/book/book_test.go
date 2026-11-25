package book

import (
	"testing"
	"time"
)

func add(ref uint64, side Side, price float64, shares int64) Event {
	return Event{Time: time.Unix(0, 0), Kind: KindAdd, Ref: ref, Side: side, Price: ToPrice(price), Shares: shares}
}

func TestBestAndDepth(t *testing.T) {
	b := New()
	b.Apply(add(1, Buy, 10.00, 100))
	b.Apply(add(2, Buy, 9.99, 200))
	b.Apply(add(3, Buy, 10.00, 50))
	b.Apply(add(4, Sell, 10.02, 300))
	bid, _ := b.Best(Buy)
	ask, _ := b.Best(Sell)
	if bid.Price != ToPrice(10.00) || bid.Size != 150 || ask.Price != ToPrice(10.02) || ask.Size != 300 {
		t.Fatalf("best bid %+v ask %+v", bid, ask)
	}
	depth := b.Depth(Buy, 5)
	if len(depth) != 2 || depth[1].Price != ToPrice(9.99) {
		t.Fatalf("depth %+v", depth)
	}
}

func TestExecuteCancelDeleteReplace(t *testing.T) {
	b := New()
	b.Apply(add(1, Buy, 10.00, 100))
	b.Apply(add(2, Buy, 10.00, 100))
	effects := b.Apply(Event{Kind: KindExecute, Ref: 1, Shares: 40})
	if len(effects) != 1 || effects[0].Kind != EffectExecute || effects[0].Shares != 40 || effects[0].ArrivalSeq != 1 {
		t.Fatalf("execute effect %+v", effects)
	}
	effects = b.Apply(Event{Kind: KindCancel, Ref: 1, Shares: 20})
	if effects[0].Kind != EffectCancel || b.SizeAt(Buy, ToPrice(10)) != 140 {
		t.Fatalf("cancel effect %+v size %d", effects, b.SizeAt(Buy, ToPrice(10)))
	}
	effects = b.Apply(Event{Kind: KindReplace, Ref: 2, NewRef: 9, Price: ToPrice(10.01), Shares: 70})
	if len(effects) != 2 || !effects[0].Replaced || effects[1].Kind != EffectAdd || effects[1].ArrivalSeq != 3 {
		t.Fatalf("replace effects %+v", effects)
	}
	if bid, _ := b.Best(Buy); bid.Price != ToPrice(10.01) || bid.Size != 70 {
		t.Fatalf("best bid after replace %+v", bid)
	}
	effects = b.Apply(Event{Kind: KindDelete, Ref: 1})
	if effects[0].Kind != EffectRemove || effects[0].Shares != 40 {
		t.Fatalf("delete effect %+v", effects)
	}
	if b.SizeAt(Buy, ToPrice(10)) != 0 {
		t.Fatal("level not removed")
	}
}

func TestUnknownReferenceIsAnAnomaly(t *testing.T) {
	b := New()
	if effects := b.Apply(Event{Kind: KindExecute, Ref: 99, Shares: 10}); len(effects) != 0 {
		t.Fatalf("effects for unknown order: %+v", effects)
	}
	if b.Anomalies != 1 {
		t.Fatalf("anomalies %d", b.Anomalies)
	}
}
