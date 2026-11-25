// Package feed reads recorded datafeedfs `stream` files and replays them into the market.
package feed

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/larytet/market_simulation/internal/book"
)

// SymbolEvent is a stream event with its symbol.
type SymbolEvent struct {
	Symbol string
	book.Event
}

// Reader yields events in feed order. Next returns io.EOF at the end.
type Reader interface {
	Next() (SymbolEvent, error)
}

var timestampLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
}

func parseTimestamp(text string) (time.Time, error) {
	for _, layout := range timestampLayouts {
		if parsed, err := time.ParseInLocation(layout, text, time.UTC); err == nil {
			return parsed.UTC(), nil
		}
	}
	if nanoseconds, err := strconv.ParseInt(text, 10, 64); err == nil {
		return time.Unix(0, nanoseconds).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp %q", text)
}

type csvReader struct {
	symbol  string
	file    *os.File
	reader  *csv.Reader
	columns map[string]int
}

// OpenCSV opens a datafeedfs stream file (`stream/<SYMBOL>/<DAY>.csv`) of one symbol.
// Columns: timestamp, event, seq, ref, new_ref, side, price, shares.
func OpenCSV(path, symbol string) (Reader, io.Closer, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("%s: header: %w", path, err)
	}
	columns := map[string]int{}
	for index, name := range header {
		columns[strings.TrimSpace(name)] = index
	}
	for _, name := range []string{"timestamp", "event", "side", "price", "shares"} {
		if _, found := columns[name]; !found {
			file.Close()
			return nil, nil, fmt.Errorf("%s: column %q missing", path, name)
		}
	}
	return &csvReader{symbol: symbol, file: file, reader: reader, columns: columns}, file, nil
}

func (r *csvReader) field(record []string, name string) string {
	index, found := r.columns[name]
	if !found || index >= len(record) {
		return ""
	}
	return strings.TrimSpace(record[index])
}

func (r *csvReader) Next() (SymbolEvent, error) {
	record, err := r.reader.Read()
	if err != nil {
		return SymbolEvent{}, err
	}
	timestamp, err := parseTimestamp(r.field(record, "timestamp"))
	if err != nil {
		return SymbolEvent{}, err
	}
	event := book.Event{Time: timestamp}
	if kind := r.field(record, "event"); len(kind) > 0 {
		event.Kind = kind[0]
	}
	event.Seq, _ = strconv.ParseInt(r.field(record, "seq"), 10, 64)
	event.Ref, _ = strconv.ParseUint(r.field(record, "ref"), 10, 64)
	event.NewRef, _ = strconv.ParseUint(r.field(record, "new_ref"), 10, 64)
	if side := r.field(record, "side"); len(side) > 0 {
		event.Side = book.Side(side[0])
	}
	if price, err := strconv.ParseFloat(r.field(record, "price"), 64); err == nil {
		event.Price = book.ToPrice(price)
	}
	event.Shares, _ = strconv.ParseInt(r.field(record, "shares"), 10, 64)
	return SymbolEvent{Symbol: r.symbol, Event: event}, nil
}
