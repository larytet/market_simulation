// Command simulate starts the FIX gateway of the market simulator and replays the recorded feed.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/larytet/market_simulation/internal/engine"
	"github.com/larytet/market_simulation/internal/feed"
	"github.com/larytet/market_simulation/internal/gateway"
	"github.com/larytet/market_simulation/internal/replay"
)

func main() {
	listenAddress := flag.String("listen", "127.0.0.1:9878", "FIX acceptor address")
	senderCompID := flag.String("sender-comp-id", "SIMULATOR", "CompID of the gateway (tag 49)")
	heartbeat := flag.Duration("heartbeat", 30*time.Second, "default heartbeat interval")
	streamDirectory := flag.String("stream-dir", "", "datafeedfs stream directory: <dir>/<SYMBOL>/<day>.csv")
	day := flag.String("day", "", "trading day, YYYY-MM-DD")
	symbols := flag.String("symbols", "", "comma separated symbols")
	model := flag.String("model", "exact", "fill model: pessimistic, exact or probabilistic")
	speed := flag.Float64("speed", 1, "feed time over wall time; 0 is as fast as possible")
	startOnLogon := flag.Bool("start-on-logon", true, "start the replay after the first logon")
	startDelay := flag.Duration("start-delay", 0, "wait this long after the first logon before the replay starts")
	outputDirectory := flag.String("out", "", "directory for fills.csv (the fill stream)")
	orderLatency := flag.Duration("order-latency", time.Millisecond, "order entry latency in feed time")
	cancelLatency := flag.Duration("cancel-latency", 500*time.Microsecond, "cancel and replace latency in feed time")
	threshold := flag.Float64("threshold", 0.8, "probabilistic model: minimum fill probability on the main stream")
	seed := flag.Int64("seed", 1, "random seed of the probabilistic model")
	flag.Parse()

	config := engine.Config{
		Model: engine.FillModel(*model), OrderLatency: *orderLatency, CancelLatency: *cancelLatency,
		Threshold: *threshold, Seed: *seed,
	}
	if *outputDirectory != "" {
		if err := os.MkdirAll(*outputDirectory, 0o755); err != nil {
			log.Fatal(err)
		}
		file, err := os.Create(filepath.Join(*outputDirectory, "fills.csv"))
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		config.Recorder = engine.NewCSVRecorder(file)
	}
	market := engine.NewMarket(config)

	var readers []feed.Reader
	for _, symbol := range strings.Split(*symbols, ",") {
		if symbol == "" {
			continue
		}
		reader, closer, err := feed.OpenCSV(filepath.Join(*streamDirectory, symbol, *day+".csv"), symbol)
		if err != nil {
			log.Fatalf("open feed: %v", err)
		}
		defer closer.Close()
		readers = append(readers, reader)
	}
	source := feed.Merge(readers...)
	first, err := source.Peek()
	if err != nil {
		log.Fatalf("feed is empty or unreadable: %v", err)
	}
	market.Clock().Set(first.Time) // the clock stays here until the replay starts

	startReplay := func() {
		log.Printf("replay starts at %s, speed %v", first.Time.Format(time.RFC3339Nano), *speed)
		if err := replay.Run(context.Background(), source, market, *speed); err != nil {
			log.Printf("replay: %v", err)
			return
		}
		log.Printf("replay finished")
	}
	var once sync.Once
	if !*startOnLogon {
		go once.Do(startReplay)
	}

	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("FIX gateway %s listening on %s, model %s", *senderCompID, listener.Addr(), *model)
	server := gateway.NewServer(gateway.Config{
		SenderCompID: *senderCompID, DefaultHeartbeatInterval: *heartbeat,
		Clock: market.Clock(), EngineFactory: market.Factory(),
		OnLogon: func(counterpartyID string) {
			log.Printf("logon %s", counterpartyID)
			go func() {
				time.Sleep(*startDelay)
				once.Do(startReplay)
			}()
		},
	})
	if err := server.Serve(listener); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
