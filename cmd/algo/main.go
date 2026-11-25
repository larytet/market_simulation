// Command algo runs the sample trading algorithm against the FIX gateway (end to end test).
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/larytet/market_simulation/internal/algo"
)

func main() {
	address := flag.String("addr", "127.0.0.1:9878", "gateway address")
	sender := flag.String("sender", "ALGO", "own CompID")
	target := flag.String("target", "SIMULATOR", "gateway CompID")
	timeout := flag.Duration("timeout", 60*time.Second, "overall timeout")
	connectRetries := flag.Int("connect-retries", 30, "connect attempts, one per second")
	flag.Parse()

	var result algo.SampleResult
	var err error
	for attempt := 0; attempt < *connectRetries; attempt++ {
		result, err = algo.RunSample(*address, *sender, *target, *timeout)
		if err == nil || !isConnectError(err) {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		log.Printf("FAIL: %v", err)
		os.Exit(1)
	}
	for _, fill := range result.Fills {
		fmt.Printf("fill %s %d @ %s commission %s liquidity %s\n", fill.ClientOrderID, fill.Quantity, fill.Price, fill.Commission, fill.Liquidity)
	}
	if err := algo.CheckSample(result); err != nil {
		log.Printf("FAIL: %v", err)
		os.Exit(1)
	}
	fmt.Println("PASS")
}

func isConnectError(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "no such host"))
}
