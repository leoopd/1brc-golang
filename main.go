package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"time"
)

// max seems to be 1073741824B, ~1GB
// optimal buffer for now: 1000 << 10

func main() {
	f, err := os.Create("cpu.prof")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if err = pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

	startTime := time.Now()
	run()
	fmt.Printf("took %v", time.Since(startTime))
}

type Chunk struct {
	ptr *[]byte
	n   int
}

func run() {
	bufSize := 2048 * 2048
	maxLine := 128

	chunkPool := sync.Pool{
		New: func() any {
			c := make([]byte, bufSize+maxLine)
			return &c
		},
	}

	maxProcs := runtime.GOMAXPROCS(0)
	fmt.Println("GOMAXPROCS:", maxProcs)

	f := openMeasurements("measurements_1b.txt")
	defer f.Close()

	// create workers to process chunks
	var wg sync.WaitGroup
	shardChan := make(chan map[string]*Station)
	chunkChan := make(chan Chunk, maxProcs)
	for range maxProcs {
		wg.Add(1)
		go processChunks(&wg, &chunkPool, chunkChan, shardChan)
	}

	// create single worker to merge shards from
	// processing workers to global map result
	mapChan := make(chan map[string]*Station)
	go mergeMaps(shardChan, mapChan)
	r := bufio.NewReaderSize(f, 2048*2048)

	for {
		chunkPtr := chunkPool.Get().(*[]byte)
		chunk := *chunkPtr
		n, err := r.Read(chunk[:bufSize])
		if n == 0 && err == io.EOF {
			break
		}

		if n > 0 && chunk[n-1] != '\n' {
			for {
				nextByte, err := r.ReadByte()
				if err != nil {
					if err == io.EOF {
						break
					}
					log.Fatal("couldn't proceed reading byte: %w", err)
				}
				chunk[n] = nextByte
				n++
				if nextByte == '\n' {
					break
				}
			}
		}
		chunkChan <- Chunk{ptr: chunkPtr, n: n}
	}
	close(chunkChan)

	// wait for processing workers to finish working on chunks
	wg.Wait()
	close(shardChan)

	// sort map, calculate results
	processOutput(mapChan)
}

func processOutput(in chan map[string]*Station) {
	global := <-in

	keys := make([]string, len(global))
	i := 0
	for k := range global {
		keys[i] = k
		i++
	}
	sort.Strings(keys)

	for _, key := range keys {
		localStation := global[key]
		fmt.Printf("%s;%.1f;%.1f;%.1f\n", key, localStation.minVal, localStation.sum/localStation.count, localStation.maxVal)
	}
}

func processChunks(wg *sync.WaitGroup, chunkPool *sync.Pool, in chan Chunk, out chan map[string]*Station) {
	defer wg.Done()
	shard := make(map[string]*Station)

	for c := range in {
		chunk := (*c.ptr)[:c.n]
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
				lineStart = i + 1

				station, ok := shard[name]
				if !ok {
					newStation := &Station{minVal: value, sum: value, maxVal: value, count: 1}
					shard[name] = newStation
					continue
				}
				station.count++
				station.sum += value
				if value < station.minVal {
					station.minVal = value
				} else if value > station.maxVal {
					station.maxVal = value
				}
			}
		}
		*c.ptr = (*c.ptr)[:cap(*c.ptr)]
		chunkPool.Put(c.ptr)
	}

	out <- shard
}

type Station struct {
	minVal float64
	sum    float64
	maxVal float64
	count  float64
}

func mergeMaps(in chan map[string]*Station, out chan map[string]*Station) {
	globalStations := make(map[string]*Station)

	for shard := range in {
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
	out <- globalStations
}

func openMeasurements(path string) *os.File {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal("unable to open measurements, err: ", err)
	}
	return file
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
