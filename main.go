package main

import (
	"strconv"
	"fmt"
	"io"
	"log"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"time"
)

// max seems to be 1073741824B, ~1GB

const (
	Buf2KB  = 2 << 10  // default of bufio
	Buf3KB  = 3 << 10  // 2 KiB
	Buf4KB  = 4 << 10  // 2 KiB
	Buf5KB  = 5 << 10  // 2 KiB
	Buf6KB  = 6 << 10  // 2 KiB
	Buf7KB  = 7 << 10  // 2 KiB
	Buf8KB  = 8 << 10  // 8 KiB
	Buf16KB = 16 << 10 // 16 KiB
)

// optimal buffer for now: 1000 << 10

func main() {
	bufSize := 25
	initPprof()

	f, startTime := openMeasurements("measurements_3r.txt")

	defer f.Close()
	durationOpenFile := time.Since(startTime)

	entries := make(map[string]*Entry)

	type bfr struct {
		b   []byte
		lo  []byte // left-overs from last chunk
		len int
		lc  bool // indicates last chunk
		f   *os.File
	}

	b := bfr{
		b:   make([]byte, bufSize),
		f:   f,
		len: bufSize,
	}
	// r := bufio.NewReaderSize(f, bufSize)

	startScan := time.Now()
	for {
		// this gives us a chunk of the file (up to bufSize)
		// at this point we don't know whether it ends in the middle
		// of a line or at the end of one (kinda unlikely)
		n, err := f.Read(b.b)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal("read failed with: %w", err)
		}

		if n < b.len {
			// this is our last chunk, which means there might be
			// bytes remaining in b.b
			b.lc = true
		}

		// let's read backwards into the chunk until we find a linebreak
		var nlo int
		for i := n - 1; i > 0; i-- {
			if b.b[i] == '\n' {
				nlo = i
				break
			}
		}
		// b.lo contains the left-overs of the previous chunk, most likely
		// a split line.
		// Let's prefix our next chunk with it to make it complete.

		// We exlude the last line-break to indicate the end of a chunk. 
		chunk := append(b.lo, b.b[:nlo+1]...)
		fmt.Printf("chunk: %s\n", strconv.Quote(string(chunk)))
		b.lo = append([]byte{}, b.b[nlo+1:n]...)

		// [-- start process
		// every byte before ';' belongs to the key (station name)
		// every byte after that represents the reading between '-99.0' and '99.9' 
		var lineStart int
		for i, byte := range chunk {
			if byte == '\n' {
				line := chunk[lineStart:i]
				lineStart = i+1

			} 
			// linestart:i == line
		}
		// end process --]

		// b[:n] contains read data
		// b[:nlo] contains chunk with left-overs cut off
		// fmt.Printf("lo: %s\n", strconv.Quote(string(b.lo)))
		// fmt.Printf("b: %s\n", strconv.Quote(string(b.b[:nlo])))

		// processing idea:
		// 1) goroutine that handles the map
		// - reads Entries from a chan
		//		? in map
		//			check min/max, add to sum, incr count
		//		! add to map
		// 2) gomaxprocs-1 goroutines that process chunks to
		//    entries and send the entry to a chan
	}

	durationScan := time.Since(startScan)
	startOutput := time.Now()

	for city, entry := range entries {
		fmt.Printf("%s;%.1f;%.1f;%.1f\n", city, entry.Min, entry.Sum/entry.Count, entry.Max) // min, mean, max
	}

	durationOutput := time.Since(startOutput)
	durationAll := time.Since(startTime)

	fmt.Printf("opening: %v, scan: %v, output: %v, all: %v\n", durationOpenFile, durationScan, durationOutput, durationAll)
}

func initPprof() {
	f, _ := os.Create("cpu.prof")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	f2, _ := os.Create("mem.prof")
	pprof.WriteHeapProfile(f2)
}

func openMeasurements(path string) (*os.File, time.Time) {
	start := time.Now()
	file, err := os.Open(path)
	if err != nil {
		log.Fatal("unable to open measurements, err: ", err)
	}
	return file, start
}

type Entry struct {
	Min   float64
	Max   float64
	Sum   float64
	Count float64
}

func NewEntry(measurement float64) *Entry {
	return &Entry{
		Min:   measurement,
		Max:   measurement,
		Sum:   measurement,
		Count: 1,
	}
}

func (e *Entry) housekeep(measurement float64) {
	e.Count++
	e.Sum += measurement

	if measurement < e.Min {
		e.Min = measurement
	}

	if measurement > e.Max {
		e.Max = measurement
	}
}
