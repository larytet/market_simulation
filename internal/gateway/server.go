// Package gateway is the FIX 4.2 acceptor the trading algorithms connect to (docs/spec FR-70 to FR-80).
package gateway

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"github.com/larytet/market_simulation/internal/engine"
)

// Clock supplies the time stamped on messages. The simulated clock driven by the feed (FR-76) will
// implement it; SystemClock is used until then.
type Clock interface {
	Now() time.Time
}

// SystemClock is the wall clock.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

// Config of the gateway.
type Config struct {
	SenderCompID             string   // the gateway's own CompID (tag 49 on outbound messages)
	AllowedCounterpartyIDs   []string // CompIDs of algorithms allowed to log on; empty allows any
	DefaultHeartbeatInterval time.Duration
	Clock                    Clock
	EngineFactory            engine.Factory
}

// Server accepts FIX sessions, one per algorithm.
type Server struct {
	config   Config
	mutex    sync.Mutex
	sessions map[*session]struct{}
	closed   bool
}

// NewServer creates a server. Missing config values get defaults.
func NewServer(config Config) *Server {
	if config.SenderCompID == "" {
		config.SenderCompID = "SIMULATOR"
	}
	if config.DefaultHeartbeatInterval == 0 {
		config.DefaultHeartbeatInterval = 30 * time.Second
	}
	if config.Clock == nil {
		config.Clock = SystemClock{}
	}
	if config.EngineFactory == nil {
		config.EngineFactory = engine.NewStubFactory()
	}
	return &Server{config: config, sessions: map[*session]struct{}{}}
}

// Serve accepts connections until the listener is closed.
func (server *Server) Serve(listener net.Listener) error {
	for {
		connection, err := listener.Accept()
		if err != nil {
			server.mutex.Lock()
			closed := server.closed
			server.mutex.Unlock()
			if closed || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go server.handleConnection(connection)
	}
}

// Close drops all sessions. The caller closes the listener.
func (server *Server) Close() {
	server.mutex.Lock()
	server.closed = true
	for current := range server.sessions {
		current.connection.Close()
	}
	server.mutex.Unlock()
}

func (server *Server) handleConnection(connection net.Conn) {
	current := newSession(server, connection)
	server.mutex.Lock()
	server.sessions[current] = struct{}{}
	server.mutex.Unlock()
	defer func() {
		server.mutex.Lock()
		delete(server.sessions, current)
		server.mutex.Unlock()
		connection.Close()
	}()
	if err := current.run(); err != nil {
		log.Printf("session %s ended: %v", current.name(), err)
	}
}
