package main

import (
	"fmt"
	"io"
	"log"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"sync"
	"time"
)

// max seems to be 1073741824B, ~1GB
// optimal buffer for now: 1000 << 10

func main() {
	// initPprof()
	f2, _ := os.Create("cpu.prof")
	pprof.StartCPUProfile(f2)
	defer pprof.StopCPUProfile()

	f3, _ := os.Create("mem.prof")
	pprof.WriteHeapProfile(f3)

	f, startTime := openMeasurements("measurements_1b.txt")
	defer f.Close()
	durationOpenFile := time.Since(startTime)

	bufSize := 650000 << 10
	chunkCh := make(chan *[]byte, 130)
	startScan := time.Now()
	var leftOver []byte
	for {
		reserved := len(leftOver)
		// make chunk that is big enough to hold next read and leftOver
		// from previous read
		chunk := make([]byte, 0, reserved+bufSize)
		chunk = append(chunk, leftOver...)
		chunk = chunk[:cap(chunk)]

		n, err := f.Read(chunk[reserved:])
		var nlo int
		// if chunk doesn't end in 'n'
		lastNewElem := reserved + n - 1
		if chunk[lastNewElem] != '\n' {
			for i := lastNewElem; i >= 0; i-- {
				if chunk[i] == '\n' {
					nlo = i
					break
				}
			}
			leftOver = chunk[nlo+1 : lastNewElem]
			chunk = chunk[:nlo+1]
		}

		// chunk: [leftOver, new data incl. next leftOver]

		if n < bufSize || err == io.EOF {
			// we need to cut off trailing space for our last chunk
			chunk = chunk[:reserved+n]
			chunkCh <- &chunk
			close(chunkCh)
			break
		}
		chunkCh <- &chunk
	}

	durationScan := time.Since(startScan)
	startProcessing := time.Now()

	var wg sync.WaitGroup
	valCh := make(chan *[]reading, 130)
	for range 20 {
		wg.Add(1)
		go processChunks(&wg, chunkCh, valCh)
	}

	startMap := time.Now()
	var wgM sync.WaitGroup
	mapCh := make(chan *map[string]station, 13000)
	wgM.Add(1)
	go writeToMap(&wgM, mapCh, valCh)

	fmt.Println("waiting for processing...")
	wg.Wait()
	close(valCh)
	durationProcessing := time.Since(startProcessing)

	fmt.Println("waiting for map...")
	wgM.Wait()
	entryMap := <-mapCh
	fmt.Println(entryMap)

	durationStartMap := time.Since(startMap)
	durationAll := time.Since(startTime)

	fmt.Printf("opening: %v, scan: %v, processing: %v, map: %v, all: %v\n", durationOpenFile, durationScan, durationProcessing, durationStartMap, durationAll)
	fmt.Println(len(*entryMap))
}

func processChunks(wg *sync.WaitGroup, chunks chan *[]byte, readingChan chan *[]reading) {
	defer wg.Done()
	for chunkPtr := range chunks {

		var lineStart int
		var semicolon int
		var name string
		var readings []reading
		for i, byte := range *chunkPtr {
			if byte == ';' {
				// linestart:i contains name
				semicolon = i
				name = string((*chunkPtr)[lineStart:i])
			}

			// we found our float value, chunk[semicolon:i]
			if byte == '\n' {
				currentValue := processToFloat((*chunkPtr)[semicolon+1 : i])
				// move linestart to next byte
				lineStart = i + 1

				e := reading{name: name, value: currentValue}
				readings = append(readings, e)
			}
		}
		// end process --]
		readingChan <- &readings
	}
}

type reading struct {
	name  string
	value float64
}

type station struct {
	min   float64
	sum   float64
	max   float64
	count float64
}

func writeToMap(wg *sync.WaitGroup, mapCh chan *map[string]station, vCh chan *[]reading) {
	defer wg.Done()
	entries := make(map[string]station)
	for readings := range vCh {
		for _, reading := range *readings {
			v := reading.value
			if entry, ok := entries[reading.name]; ok {
				entry.count++
				entry.sum += v
				if v < entry.min {
					entry.min = v
				} else if v > entry.max {
					entry.max = v
				}
				entries[reading.name] = entry
			} else {
				entries[reading.name] = station{min: v, sum: v, max: v, count: 1}
			}
		}
	}
	mapCh <- &entries
}

func openMeasurements(path string) (*os.File, time.Time) {
	start := time.Now()
	file, err := os.Open(path)
	if err != nil {
		log.Fatal("unable to open measurements, err: ", err)
	}
	return file, start
}

func processToFloat(b []byte) float64 {
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
		return -(float64(intPart) + float64(decimal)/10)
	}
	return float64(intPart) + float64(decimal)/10
}
