package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

// max seems to be 1073741824B, ~1GB
// optimal buffer for now: 1000 << 10

func main() {
	maxProcs := runtime.GOMAXPROCS(0)

	startTime := time.Now()
	f2, _ := os.Create("cpu.prof")
	_ = pprof.StartCPUProfile(f2)
	defer pprof.StopCPUProfile()

	f3, _ := os.Create("mem.prof")
	_ = pprof.WriteHeapProfile(f3)

	f, startTime := openMeasurements("measurements_1b.txt")

	defer func() {
		_ = f.Close()
	}()

	var wg sync.WaitGroup
	shardChan := make(chan map[string]Station)
	chunkChan := make(chan []byte, maxProcs*2)

	for range maxProcs {
		wg.Add(1)
		go processChunks(&wg, chunkChan, shardChan)
	}

	var wgMap sync.WaitGroup
	wgMap.Add(1)
	go mergeMaps(&wgMap, shardChan)

	bufSize := 1 << 20 // 1MB
	maxLine := 128
	r := bufio.NewReaderSize(f, bufSize+maxLine)
	for {
		chunk := make([]byte, bufSize+maxLine)
		n, err := r.Read(chunk[:bufSize])
		if n == 0 && err == io.EOF {
			break
		}

		if chunk[n-1] != '\n' {
			for {
				nextByte, err := r.ReadByte()
				if err != nil {
					log.Fatal("couldn't proceed reading byte: %w", err)
				}
				n++
				if nextByte == '\n' {
					break
				}
			}
		}

		chunkChan <- chunk[:n]
		if err == io.EOF {
			break
		}
	}
	close(chunkChan)

	fmt.Println("waiting for processing...")
	wg.Wait()
	close(shardChan)

	fmt.Println("waiting for map...")
	wgMap.Wait()

	fmt.Printf("took %v", time.Since(startTime))
}

func processChunks(wg *sync.WaitGroup, in chan []byte, out chan map[string]Station) {
	defer wg.Done()
	shard := make(map[string]Station)
	for chunk := range in {
		var (
			lineStart int
			semicolon int
		)

		for i, elem := range chunk {
			if elem == ';' {
				// linestart:i contains name
				semicolon = i
			}
			// we found our float value, chunk[semicolon:i]
			if elem == '\n' {
				name := string(chunk[lineStart:semicolon])
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

				lineStart = i + 1
			}
		}
	}
	out <- shard
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
