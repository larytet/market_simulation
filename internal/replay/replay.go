// Package replay drives the market with the recorded feed.
package replay

import (
	"context"
	"io"
	"time"

	"github.com/larytet/market_simulation/internal/engine"
	"github.com/larytet/market_simulation/internal/feed"
)

// Run feeds the events to the market in time order. speed is the feed time over wall time: 1 is real time,
// 10 ten times faster, 0 (or less) as fast as possible. At the end the market finishes the day.
func Run(ctx context.Context, source *feed.Merged, market *engine.Market, speed float64) error {
	var firstFeedTime time.Time
	startedAt := time.Now()
	for {
		event, err := source.Next()
		if err == io.EOF {
			market.Finish()
			return nil
		}
		if err != nil {
			return err
		}
		if firstFeedTime.IsZero() {
			firstFeedTime = event.Time
		}
		if speed > 0 {
			due := startedAt.Add(time.Duration(float64(event.Time.Sub(firstFeedTime)) / speed))
			if wait := time.Until(due); wait > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(wait):
				}
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		market.Process(event.Symbol, event.Event)
	}
}
