package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"time"
)

const (
	Buf16B  = 16       // 16 B
	Buf256B = 256      // 256 B
	Buf512B = 512      // 512 B
	Buf1KB  = 1 << 10  // 1 KiB  (1024 B)
	Buf2KB  = 2 << 10  // 2 KiB
	Buf8KB  = 8 << 10  // 8 KiB
	Buf16KB = 16 << 10 // 16 KiB
)

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

func main() {
	bufSize := Buf2KB
	initPprof()

	f, startTime := openMeasurements("measurements_100m.txt")
	defer f.Close()
	durationOpenFile := time.Since(startTime)

	// entries := make(map[string]*Entry)
	b := make([]byte, bufSize)
	r := bufio.NewReaderSize(f, bufSize)

	startScan := time.Now()
	for {
		n, err := r.Read(b)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal("read failed with: %w", err)
		}

		fmt.Println(string(b[:n]))
	}

	durationScan := time.Since(startScan)
	startOutput := time.Now()

	//	for city, entry := range entries {
	//		fmt.Printf("%s;%.1f;%.1f;%.1f\n", city, entry.Min, entry.Sum/entry.Count, entry.Max) // min, mean, max
	//	}

	durationOutput := time.Since(startOutput)
	durationAll := time.Since(startTime)

	fmt.Printf("opening: %v, scan: %v, output: %v, all: %v\n", durationOpenFile, durationScan, durationOutput, durationAll)
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
