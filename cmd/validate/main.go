// Command validate rebuilds the book of one symbol-day from the `stream` file and compares the top levels
// with the `orderbook` file of datafeedfs after every message that has a row there (docs/spec FR-3).
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/larytet/market_simulation/internal/book"
	"github.com/larytet/market_simulation/internal/feed"
)

type bookRow struct {
	seq    int64
	event  string
	bids   [5]book.Level
	asks   [5]book.Level
	parsed bool
}

// orderbookRow is the schema of datafeedfs orderbook parquet files (depth 5).
type orderbookRow struct {
	Event    string  `parquet:"event"`
	Bid1     float64 `parquet:"bid_1"`
	BidSize1 int32   `parquet:"bid_size_1"`
	Ask1     float64 `parquet:"ask_1"`
	AskSize1 int32   `parquet:"ask_size_1"`
	Bid2     float64 `parquet:"bid_2"`
	BidSize2 int32   `parquet:"bid_size_2"`
	Ask2     float64 `parquet:"ask_2"`
	AskSize2 int32   `parquet:"ask_size_2"`
	Bid3     float64 `parquet:"bid_3"`
	BidSize3 int32   `parquet:"bid_size_3"`
	Ask3     float64 `parquet:"ask_3"`
	AskSize3 int32   `parquet:"ask_size_3"`
	Bid4     float64 `parquet:"bid_4"`
	BidSize4 int32   `parquet:"bid_size_4"`
	Ask4     float64 `parquet:"ask_4"`
	AskSize4 int32   `parquet:"ask_size_4"`
	Bid5     float64 `parquet:"bid_5"`
	BidSize5 int32   `parquet:"bid_size_5"`
	Ask5     float64 `parquet:"ask_5"`
	AskSize5 int32   `parquet:"ask_size_5"`
	Seq      uint64  `parquet:"seq"`
}

func parquetLevel(price float64, size int32) book.Level {
	if math.IsNaN(price) || size == 0 {
		return book.Level{}
	}
	return book.Level{Price: book.ToPrice(price), Size: int64(size)}
}

type parquetRows struct {
	reader *parquet.GenericReader[orderbookRow]
	rows   []orderbookRow
	index  int
	count  int
}

func (r *parquetRows) next() (bookRow, error) {
	if r.index >= r.count {
		count, err := r.reader.Read(r.rows)
		if count == 0 {
			if err == nil {
				err = io.EOF
			}
			return bookRow{}, err
		}
		r.index, r.count = 0, count
	}
	row := &r.rows[r.index]
	r.index++
	return bookRow{
		seq: int64(row.Seq), event: row.Event,
		bids: [5]book.Level{parquetLevel(row.Bid1, row.BidSize1), parquetLevel(row.Bid2, row.BidSize2), parquetLevel(row.Bid3, row.BidSize3), parquetLevel(row.Bid4, row.BidSize4), parquetLevel(row.Bid5, row.BidSize5)},
		asks: [5]book.Level{parquetLevel(row.Ask1, row.AskSize1), parquetLevel(row.Ask2, row.AskSize2), parquetLevel(row.Ask3, row.AskSize3), parquetLevel(row.Ask4, row.AskSize4), parquetLevel(row.Ask5, row.AskSize5)},
	}, nil
}

type rowSource interface{ next() (bookRow, error) }

type rowReader struct {
	reader  *csv.Reader
	columns map[string]int
}

func newRowReader(path string) (rowSource, *os.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	if strings.HasSuffix(path, ".parquet") {
		return &parquetRows{reader: parquet.NewGenericReader[orderbookRow](file), rows: make([]orderbookRow, 16384)}, file, nil
	}
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		return nil, nil, err
	}
	columns := map[string]int{}
	for index, name := range header {
		columns[name] = index
	}
	return &rowReader{reader: reader, columns: columns}, file, nil
}

func (r *rowReader) next() (bookRow, error) {
	record, err := r.reader.Read()
	if err != nil {
		return bookRow{}, err
	}
	row := bookRow{event: record[r.columns["event"]]}
	row.seq, _ = strconv.ParseInt(record[r.columns["seq"]], 10, 64)
	for level := 1; level <= 5; level++ {
		row.bids[level-1] = r.level(record, "bid", level)
		row.asks[level-1] = r.level(record, "ask", level)
	}
	return row, nil
}

func (r *rowReader) level(record []string, side string, level int) book.Level {
	price, err := strconv.ParseFloat(record[r.columns[fmt.Sprintf("%s_%d", side, level)]], 64)
	size, _ := strconv.ParseInt(record[r.columns[fmt.Sprintf("%s_size_%d", side, level)]], 10, 64)
	if err != nil || math.IsNaN(price) || size == 0 {
		return book.Level{}
	}
	return book.Level{Price: book.ToPrice(price), Size: size}
}

func main() {
	streamPath := flag.String("stream", "", "stream CSV of the symbol-day")
	orderbookPath := flag.String("orderbook", "", "orderbook CSV of the same symbol-day")
	symbol := flag.String("symbol", "SYM", "symbol")
	limit := flag.Int64("limit", 0, "stop after this many stream events (0 for the whole day)")
	flag.Parse()

	stream, closeStream, err := feed.Open(*streamPath, *symbol)
	if err != nil {
		log.Fatal(err)
	}
	defer closeStream.Close()
	rows, closeRows, err := newRowReader(*orderbookPath)
	if err != nil {
		log.Fatal(err)
	}
	defer closeRows.Close()

	current := book.New()
	var events, compared, mismatches, rowsWithoutEvent int64
	var pending *bookRow
	started := time.Now()
	for {
		event, err := stream.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		events++
		current.Apply(event.Event)
		for {
			if pending == nil {
				row, err := rows.next()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Fatal(err)
				}
				pending = &row
			}
			if pending.seq > event.Seq {
				break
			}
			if pending.seq < event.Seq {
				rowsWithoutEvent++
			} else if pending.event == "Q" {
				compared++
				if difference := compare(current, pending); difference != "" {
					mismatches++
					if mismatches <= 10 {
						fmt.Printf("MISMATCH seq %d (stream event %c at %s): %s\n", event.Seq, event.Kind, event.Time.Format("15:04:05.000000000"), difference)
					}
				}
			}
			pending = nil
		}
		if *limit > 0 && events >= *limit {
			break
		}
	}
	fmt.Printf("stream events %d, book rows compared %d, mismatches %d (%.4f%%), orderbook rows without a stream event %d, book anomalies %d, crossed at end %v, %s\n",
		events, compared, mismatches, 100*float64(mismatches)/math.Max(1, float64(compared)), rowsWithoutEvent, current.Anomalies, current.IsCrossed(), time.Since(started).Round(time.Second))
}

func compare(current *book.Book, row *bookRow) string {
	var differences []string
	for index, side := range []book.Side{book.Buy, book.Sell} {
		want := row.bids
		if index == 1 {
			want = row.asks
		}
		got := current.Depth(side, 5)
		for level := 0; level < 5; level++ {
			var have book.Level
			if level < len(got) {
				have = got[level]
			}
			if have != want[level] {
				differences = append(differences, fmt.Sprintf("%c%d have %v@%d want %v@%d", side, level+1, have.Price.Dollars(), have.Size, want[level].Price.Dollars(), want[level].Size))
				break
			}
		}
	}
	return strings.Join(differences, "; ")
}
