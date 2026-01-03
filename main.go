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

	startProcessing := time.Now()

	var wg sync.WaitGroup
	shardChan := make(chan map[string]Station, 5)
	chunkChan := make(chan []byte, 5)

	for range 20 {
		wg.Add(1)
		go processChunks(&wg, chunkChan, shardChan)
	}

	var wgMap sync.WaitGroup
	wgMap.Add(1)

	startMap := time.Now()
	go mergeMaps(&wgMap, shardChan)

	bufSize := 100 << 20 //100MB
	startScan := time.Now()
	var leftOver []byte
	for {
		reserved := len(leftOver)
		// make chunk that is big enough to hold next read
		// and leftOver from previous read
		chunk := make([]byte, 0, reserved+bufSize)
		chunk = append(chunk, leftOver...)
		chunk = chunk[:cap(chunk)]

		n, err := f.Read(chunk[reserved:])
		var idxLinebreak int

		// if chunk doesn't end in 'n'
		lastNewElem := reserved + n - 1
		if chunk[lastNewElem] != '\n' {
			// walk bakwards until we find the first linebreak
			for i := lastNewElem; i >= 0; i-- {
				if chunk[i] == '\n' {
					idxLinebreak = i
					break
				}
			}
			leftOver = chunk[idxLinebreak+1 : lastNewElem+1]
			chunk = chunk[:idxLinebreak+1]
		}

		// chunk: [
		// :reserved > leftovers from previours
		// reserved+1:reserved+n-1 > new data read
		// reserved+n-1:len(chunk) > potentially old unusable data
		// ]
		if n < bufSize {
			// we need to cut off trailing space for our last chunk
			chunk = chunk[:reserved+n-1]
			chunkChan <- chunk
			close(chunkChan)
			continue
		}

		chunkChan <- chunk
		if err == io.EOF {
			break
		}
	}

	durationScan := time.Since(startScan)

	fmt.Println("waiting for processing...")
	wg.Wait()
	close(shardChan)
	durationProcessing := time.Since(startProcessing)

	fmt.Println("waiting for map...")
	wgMap.Wait()

	durationStartMap := time.Since(startMap)
	durationAll := time.Since(startTime)

	fmt.Printf("opening: %v, scan: %v, processing: %v, map: %v, all: %v\n", durationOpenFile, durationScan, durationProcessing, durationStartMap, durationAll)
}

func processChunks(wg *sync.WaitGroup, chunks chan []byte, shardCh chan map[string]Station) {
	defer wg.Done()
	for chunk := range chunks {
		var (
			lineStart int
			semicolon int
		)
		shard := make(map[string]Station)

		for i, elem := range chunk {
			if elem == ';' {
				// linestart:i contains name
				semicolon = i
			}
			// we found our float value, chunk[semicolon:i]
			if elem == '\n' {
				lineStart = i + 1
				name := string(chunk[lineStart:i])
				value := processToFloat(chunk[semicolon+1 : i])
				if station, ok := shard[name]; ok {
					station.count++
					station.sum += value
					if value < station.minVal {
						station.minVal = value
					} else if value > station.minVal {
						station.maxVal = value
					}
					shard[name] = station
				} else {
					shard[name] = Station{minVal: value, sum: value, maxVal: value, count: 1}
				}
			}
		}
		shardCh <- shard
	}

}

type Station struct {
	minVal float64
	sum    float64
	maxVal float64
	count  float64
}

func mergeMaps(wg *sync.WaitGroup, shardChan chan map[string]Station) {
	defer wg.Done()
	globalStations := make(map[string]Station)

	for shard := range shardChan {
		for name, shardStation := range shard {
			if globalStation, ok := globalStations[name]; ok {
				globalStation.count++
				globalStation.sum += shardStation.sum
				if shardStation.minVal < globalStation.minVal {
					globalStation.minVal = shardStation.minVal
				} else if shardStation.maxVal > globalStation.minVal {
					globalStation.maxVal = shardStation.maxVal
				}
				globalStations[name] = globalStation
			} else {
				globalStations[name] = shardStation
			}
		}
	}
	fmt.Println(globalStations)
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
