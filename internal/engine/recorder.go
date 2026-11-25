package engine

import (
	"encoding/csv"
	"io"
	"strconv"
	"sync"
)

// CSVRecorder writes the fill stream (docs/spec 4.1) as CSV. stream is "main" for fills sent to the algorithm
// and "uncertain" for the expected-fill stream.
type CSVRecorder struct {
	mutex  sync.Mutex
	writer *csv.Writer
}

// NewCSVRecorder writes the header and returns the recorder. Call Flush before the process ends.
func NewCSVRecorder(output io.Writer) *CSVRecorder {
	writer := csv.NewWriter(output)
	writer.Write([]string{
		"session", "client_order_id", "symbol", "side", "fill_time", "venue", "price", "quantity",
		"liquidity", "commission", "fill_probability", "model", "stream",
	})
	writer.Flush()
	return &CSVRecorder{writer: writer}
}

// Record implements Recorder.
func (recorder *CSVRecorder) Record(sessionName string, report Report, uncertain bool) {
	stream := "main"
	if uncertain {
		stream = "uncertain"
	}
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	recorder.writer.Write([]string{
		sessionName, report.Order.ClientOrderID, report.Order.Symbol, report.Order.Side,
		report.EventTime.Format("2006-01-02T15:04:05.000000000Z"), report.Venue,
		strconv.FormatFloat(report.LastPrice, 'f', -1, 64), strconv.FormatInt(report.LastQuantity, 10),
		report.Liquidity, strconv.FormatFloat(report.Commission, 'f', 4, 64),
		strconv.FormatFloat(report.FillProbability, 'f', 4, 64), report.QueueModel, stream,
	})
	recorder.writer.Flush()
}
