// Command simulate starts the FIX gateway of the market simulator.
package main

import (
	"flag"
	"log"
	"net"
	"time"

	"github.com/larytet/market_simulation/internal/gateway"
)

func main() {
	listenAddress := flag.String("listen", "127.0.0.1:9878", "FIX acceptor address")
	senderCompID := flag.String("sender-comp-id", "SIMULATOR", "CompID of the gateway (tag 49)")
	heartbeat := flag.Duration("heartbeat", 30*time.Second, "default heartbeat interval")
	flag.Parse()

	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("FIX gateway %s listening on %s", *senderCompID, listener.Addr())
	server := gateway.NewServer(gateway.Config{SenderCompID: *senderCompID, DefaultHeartbeatInterval: *heartbeat})
	if err := server.Serve(listener); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
