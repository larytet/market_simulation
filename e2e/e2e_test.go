// Package e2e runs the sample algorithm against the gateway and the sample feed in one process.
// The same scenario runs in containers with docker compose (e2e/run.sh, docs/testplan.md E2E-2).
package e2e

import (
	"context"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/larytet/market_simulation/internal/algo"
	"github.com/larytet/market_simulation/internal/engine"
	"github.com/larytet/market_simulation/internal/feed"
	"github.com/larytet/market_simulation/internal/gateway"
	"github.com/larytet/market_simulation/internal/replay"
)

func TestSampleAlgorithmOnSampleFeed(t *testing.T) {
	market := engine.NewMarket(engine.Config{Model: engine.ModelExact})
	reader, closer, err := feed.OpenCSV(filepath.Join("data", "stream", "TEST", "2026-11-20.csv"), "TEST")
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	source := feed.Merge(reader)
	first, err := source.Peek()
	if err != nil {
		t.Fatal(err)
	}
	market.Clock().Set(first.Time)

	var once sync.Once
	replayDone := make(chan struct{})
	server := gateway.NewServer(gateway.Config{
		SenderCompID: "SIMULATOR", Clock: market.Clock(), EngineFactory: market.Factory(),
		OnLogon: func(string) {
			go func() {
				time.Sleep(200 * time.Millisecond) // the algorithm sends its orders at feed time 0
				once.Do(func() {
					replay.Run(context.Background(), source, market, 20)
					close(replayDone)
				})
			}()
		},
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	defer server.Close()
	go server.Serve(listener)

	result, err := algo.RunSample(listener.Addr().String(), "ALGO", "SIMULATOR", 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := algo.CheckSample(result); err != nil {
		t.Fatal(err)
	}
	select {
	case <-replayDone:
	case <-time.After(10 * time.Second):
		t.Fatal("replay did not finish")
	}
	if anomalies := market.Book("TEST").Anomalies; anomalies != 0 {
		t.Errorf("book anomalies: %d", anomalies)
	}
}
