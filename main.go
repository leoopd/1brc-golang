package main

import (
	"fmt"
	"io"
	"log"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"time"
)

// max seems to be 1073741824B, ~1GB
// optimal buffer for now: 1000 << 10

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

func main() {
	fmt.Println("starting...")
	bufSize := 1000 << 10
	initPprof()

	f, startTime := openMeasurements("measurements_1b.txt")

	defer f.Close()
	durationOpenFile := time.Since(startTime)

	type bfr struct {
		b   []byte
		lo  []byte // left-overs from last chunk
		len int
		lc  bool // indicates last chunk
		f   *os.File
	}

	b := bfr{
		b: make([]byte, bufSize),
		f: f,
	}

	startScan := time.Now()

	// let's create this here for now since we otherwise overwrite it on
	// every new chunk
	entries := make(map[string]*Station)
	for {
		// this gives us a chunk of the file (up to bufSize)
		// at this point we don't know whether it ends in the middle
		// of a line or at the end of one (kinda unlikely)
		n, err := b.f.Read(b.b)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal("read failed with: %w", err)
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
		//	fmt.Printf("chunk created: %s\n", strconv.Quote(string(chunk)))
		b.lo = append([]byte{}, b.b[nlo+1:n]...)

		// [-- start process
		// every byte before ';' belongs to station name,
		// every byte after that belongs to the reading ('-99.0' - '99.9')

		var lineStart int
		var semicolon int
		var rdng reading
		for i, byte := range chunk {
			if byte == ';' {
				// linestart:i contains name
				semicolon = i
				rdng = reading{name: string(chunk[lineStart:semicolon])}
			}
			if byte == '\n' {
				// ;:i contains float value
				lineStart = i + 1
				rdng.value = processToFloat(chunk[semicolon+1 : i])

				if station, ok := entries[rdng.name]; ok {
					station.count++
					station.sum += rdng.value
					if station.min > rdng.value {
						station.min = rdng.value
					}
					if station.max < rdng.value {
						station.max = rdng.value
					}
				} else {
					entries[rdng.name] = newStation(rdng)
				}
			}
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
		//    entries and send the rdng to a chan
	}
	fmt.Println(len(entries))

	durationScan := time.Since(startScan)
	startOutput := time.Now()

	durationOutput := time.Since(startOutput)
	durationAll := time.Since(startTime)

	fmt.Printf("opening: %v, scan: %v, output: %v, all: %v\n", durationOpenFile, durationScan, durationOutput, durationAll)
}

type reading struct {
	name  string
	value float32
}

type Station struct {
	min   float32
	max   float32
	sum   float32
	count int
}

func newStation(rdng reading) *Station {
	return &Station{
		min:   rdng.value,
		max:   rdng.value,
		sum:   rdng.value,
		count: 1,
	}
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

func processToFloat(b []byte) float32 {
	neg := false
	i := 0

	if b[0] == '-' {
		neg = true
		i++
	}

	intPart := b[i] - '0'
	decimal := b[len(b)-1] - '0'
	i++

	for ; i < len(b); i++ {
		if b[i] == '.' {
			break
		}
		intPart += (b[i] - '0') * 10
	}

	if neg {
		return -(float32(intPart) + float32(decimal)/10)
	}
	return float32(intPart) + float32(decimal)/10
}
