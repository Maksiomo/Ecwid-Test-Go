package main

import (
	"bufio"
	ipstorage "ecwid-go-task/ipStorage"
	"ecwid-go-task/utils"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Toscale-platform/kit/env"
	"github.com/Toscale-platform/kit/log"

	"github.com/go-mmap/mmap"
	"github.com/joho/godotenv"
)

func main() {
	// init env
	err := godotenv.Load()
	if err != nil {
		log.Error().Err(err).Send()
		return
	}

	start := time.Now()

	log.Info().Msg("Init connection to file")

	// memory map our file, putting all necessary pointers to address space,
	// which significantly boosts reading speed (~70-80%)
	mmapedFile, err := mmap.Open(env.GetString("TARGET_FILE_PATH"))
	if err != nil {
		log.Error().Err(err).Send()
		return
	}
	defer mmapedFile.Close()

	// define how many threads are optimal for pc, 
	// we don't want to process lines one by one, as it would take ages to work 120gb benchmark 
	numWorkers := runtime.NumCPU()
	// channel to which all processed batches will be passed
	resultsChan := make(chan map[uint32]bool, numWorkers)
	// wait groups for synchronisation
	var parseWg, saveWg sync.WaitGroup

	log.Info().Msg("Parsing file with ip addresses")

	// parse wg logic
	saveWg.Add(1)
	go func() {
		resBatch := 0
		defer saveWg.Done()
		for recordBatch := range resultsChan {
			resBatch++
			if resBatch % 10000 == 0 {
				log.Info().Msg(fmt.Sprintf("batch %d", resBatch))
			}
			// add each found record of batch to storage
			for record := range recordBatch {
				ipstorage.AddUint(record)
			}
		}
	}()

	// file reader wg logic
	// buffer to read from  file, size in bytes
	buf := make([]byte, env.GetInt("DATA_CHUNK_SIZE"))
	// we need only correct lines, so any leftovers should be passed to next batch
	leftover := make([]byte, 0, env.GetInt("DATA_CHUNK_SIZE"))

	go func() {
		for {
			// read target amount of bytes from source file
			bytesRead, err := mmapedFile.Read(buf)
			// if any info was found
			if bytesRead > 0 {
				chunk := make([]byte, bytesRead)
				copy(chunk, buf[:bytesRead])
				// get all full lines into validChunk, any leftovers will pass to next cycle
				validChunk, newLeftover := processChunkWithChans(chunk, leftover)
				leftover = newLeftover
				if len(validChunk) > 0 {
					parseWg.Add(1)
					// process info in target chunck
					go processChunkData(validChunk, resultsChan, &parseWg)
				}
			}
			if err != nil {
				break
			}
		}
		parseWg.Wait()
		// close channels after parsing job is done
		close(resultsChan)
	}()

	saveWg.Wait()

	duration := time.Since(start)
	// show result
	log.Info().Msg(fmt.Sprintf(`Total amount of uniq ip addresses = %d, with a total runtime of %s`, ipstorage.CountUintUniq(), duration.String()))
}

func processChunkWithChans(chunk, leftover []byte) (validChunk, newLeftover []byte) {
	firstNewline := -1
	lastNewline := -1
	// parsing chunk line by line, finding first and last line id
	for i, b := range chunk {
		if b == '\n' {
			if firstNewline == -1 {
				firstNewline = i
			}
			lastNewline = i
		}
	}
	// if new first line detected, remove all unfinished line info to leftovers,
	//  rest to new chunk
	if firstNewline != -1 {
		validChunk = append(leftover, chunk[:lastNewline+1]...)
		newLeftover = make([]byte, len(chunk[lastNewline+1:]))
		copy(newLeftover, chunk[lastNewline+1:])
	} else {
		// otherwise, just add whole chunk to leftovers
		newLeftover = append(leftover, chunk...)
	}
	return validChunk, newLeftover
}

func processChunkData(chunk []byte, resultsChan chan<- map[uint32]bool, wg *sync.WaitGroup) {
	defer wg.Done()

	parsedIps := make(map[uint32]bool)
	scanner := bufio.NewScanner(strings.NewReader(string(chunk)))

	// scanning batch lines one by one
	for scanner.Scan() {
		// convert ips from string to uint32, so they take way less space
		parsedIp := utils.ConvertIpToInt(scanner.Text())
		parsedIps[parsedIp] = true
	}

	// Send converted ips to resultsChan
	resultsChan <- parsedIps
}
