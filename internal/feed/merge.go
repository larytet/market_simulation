package feed

import (
	"container/heap"
	"io"
)

type mergeItem struct {
	event  SymbolEvent
	source int
}

type mergeHeap []mergeItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if !h[i].event.Time.Equal(h[j].event.Time) {
		return h[i].event.Time.Before(h[j].event.Time)
	}
	return h[i].source < h[j].source
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(mergeItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	*h = old[:len(old)-1]
	return item
}

// Merged merges the readers of several symbols by time (ties by reader order, which keeps each symbol's seq order).
type Merged struct {
	readers []Reader
	queue   mergeHeap
	primed  bool
	peeked  *SymbolEvent
}

// Merge creates a merged reader.
func Merge(readers ...Reader) *Merged { return &Merged{readers: readers} }

func (m *Merged) prime() error {
	m.primed = true
	for index, reader := range m.readers {
		event, err := reader.Next()
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}
		heap.Push(&m.queue, mergeItem{event: event, source: index})
	}
	return nil
}

// Peek returns the next event without consuming it.
func (m *Merged) Peek() (SymbolEvent, error) {
	if m.peeked != nil {
		return *m.peeked, nil
	}
	event, err := m.next()
	if err != nil {
		return SymbolEvent{}, err
	}
	m.peeked = &event
	return event, nil
}

// Next returns the next event in time order.
func (m *Merged) Next() (SymbolEvent, error) {
	if m.peeked != nil {
		event := *m.peeked
		m.peeked = nil
		return event, nil
	}
	return m.next()
}

func (m *Merged) next() (SymbolEvent, error) {
	if !m.primed {
		if err := m.prime(); err != nil {
			return SymbolEvent{}, err
		}
	}
	if m.queue.Len() == 0 {
		return SymbolEvent{}, io.EOF
	}
	item := heap.Pop(&m.queue).(mergeItem)
	following, err := m.readers[item.source].Next()
	if err == nil {
		heap.Push(&m.queue, mergeItem{event: following, source: item.source})
	} else if err != io.EOF {
		return SymbolEvent{}, err
	}
	return item.event, nil
}
