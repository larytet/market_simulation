package feed

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/larytet/market_simulation/internal/book"
)

func writeStream(t *testing.T, content string) string {
	path := filepath.Join(t.TempDir(), "stream.csv")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReadCSV(t *testing.T) {
	path := writeStream(t, "timestamp,event,seq,ref,new_ref,side,price,shares\n"+
		"2026-11-20 14:30:00.123456789+00:00,A,1,7,0,B,10.25,100\n"+
		"2026-11-20T14:30:00.5Z,U,2,7,8,B,10.26,50\n")
	reader, closer, err := OpenCSV(path, "SPY")
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	first, err := reader.Next()
	if err != nil || first.Symbol != "SPY" || first.Kind != 'A' || first.Ref != 7 || first.Price != book.ToPrice(10.25) || first.Shares != 100 || first.Side != book.Buy {
		t.Fatalf("first %+v err %v", first, err)
	}
	if first.Time.Nanosecond() != 123456789 {
		t.Fatalf("nanoseconds lost: %v", first.Time)
	}
	second, _ := reader.Next()
	if second.Kind != 'U' || second.NewRef != 8 {
		t.Fatalf("second %+v", second)
	}
	if _, err := reader.Next(); err != io.EOF {
		t.Fatalf("want EOF, got %v", err)
	}
}

func TestMergeOrdersByTime(t *testing.T) {
	header := "timestamp,event,seq,ref,new_ref,side,price,shares\n"
	first, c1, _ := OpenCSV(writeStream(t, header+"2026-11-20T14:30:00.1Z,A,1,1,0,B,1,1\n2026-11-20T14:30:00.3Z,A,2,2,0,B,1,1\n"), "A")
	second, c2, _ := OpenCSV(writeStream(t, header+"2026-11-20T14:30:00.2Z,A,1,1,0,B,1,1\n"), "B")
	defer c1.Close()
	defer c2.Close()
	merged := Merge(first, second)
	var symbols string
	peeked, _ := merged.Peek()
	if peeked.Symbol != "A" {
		t.Fatalf("peek %+v", peeked)
	}
	for {
		event, err := merged.Next()
		if err != nil {
			break
		}
		symbols += event.Symbol
	}
	if symbols != "ABA" {
		t.Fatalf("order %q", symbols)
	}
}
