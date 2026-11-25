package feed

import (
	"io"
	"os"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/larytet/market_simulation/internal/book"
)

// streamRow is the schema of datafeedfs `stream/<SYMBOL>/<day>.parquet`.
type streamRow struct {
	Timestamp int64   `parquet:"timestamp"` // nanoseconds since the epoch, UTC
	Event     string  `parquet:"event"`
	Seq       uint64  `parquet:"seq"`
	Ref       uint64  `parquet:"ref"`
	NewRef    uint64  `parquet:"new_ref"`
	Side      string  `parquet:"side"`
	Price     float64 `parquet:"price"`
	Shares    int32   `parquet:"shares"`
}

type parquetReader struct {
	symbol string
	reader *parquet.GenericReader[streamRow]
	rows   []streamRow
	index  int
	count  int
}

// OpenParquet opens a datafeedfs stream file in parquet format; it is much faster than the CSV rendering.
func OpenParquet(path, symbol string) (Reader, io.Closer, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	reader := parquet.NewGenericReader[streamRow](file)
	return &parquetReader{symbol: symbol, reader: reader, rows: make([]streamRow, 16384)}, closers{reader, file}, nil
}

type closers []io.Closer

func (list closers) Close() error {
	var first error
	for _, closer := range list {
		if err := closer.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (r *parquetReader) Next() (SymbolEvent, error) {
	if r.index >= r.count {
		count, err := r.reader.Read(r.rows)
		if count == 0 {
			if err == nil {
				err = io.EOF
			}
			return SymbolEvent{}, err
		}
		r.index, r.count = 0, count
	}
	row := &r.rows[r.index]
	r.index++
	event := book.Event{
		Time: time.Unix(0, row.Timestamp).UTC(), Seq: int64(row.Seq), Ref: row.Ref, NewRef: row.NewRef,
		Price: book.ToPrice(row.Price), Shares: int64(row.Shares),
	}
	if len(row.Event) > 0 {
		event.Kind = row.Event[0]
	}
	if len(row.Side) > 0 {
		event.Side = book.Side(row.Side[0])
	}
	return SymbolEvent{Symbol: r.symbol, Event: event}, nil
}

// Open picks the reader by file extension: .parquet or .csv.
func Open(path, symbol string) (Reader, io.Closer, error) {
	if len(path) > 8 && path[len(path)-8:] == ".parquet" {
		return OpenParquet(path, symbol)
	}
	return OpenCSV(path, symbol)
}
