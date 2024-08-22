package main

import (
	"bufio"
	"ecwid-go-task/trie"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Toscale-platform/kit/env"
	"github.com/Toscale-platform/kit/log"

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

	// resultMap := make(map[string]int)

	log.Info().Msg("Init trie")
	trie := trie.NewTrie()

	log.Info().Msg("Init connection to file")
	file, err := os.Open(env.GetString("TARGET_FILE_PATH"))
	if err != nil {
		log.Error().Err(err).Send()
		return
	}

	defer file.Close()

	numWorkers := runtime.NumCPU()
	resultsChan := make(chan map[string]string, numWorkers)
	var parseWg, saveWg sync.WaitGroup

	log.Info().Msg("Parsing file with ip addresses")
	saveWg.Add(1)
	go func() {
		resBatch := 0
		defer saveWg.Done()
		for recordBatch := range resultsChan {
			resBatch++
			if resBatch % 2500 == 0 {
				log.Info().Msg(fmt.Sprintf("batch %d", resBatch))
			}
			for _, record := range recordBatch {
				err := trie.AddWord(record)
				if err != nil {
					log.Error().Err(err).Send()
				}

				// _, exists := resultMap[record]
				// if !exists {
				// 	resultMap[record] = 1
				// }
			}
		}
	}()

	buf := make([]byte, env.GetInt("DATA_CHUNK_SIZE"))
	leftover := make([]byte, 0, env.GetInt("DATA_CHUNK_SIZE"))

	go func() {
		chunckCount := 0
		for {
			bytesRead, err := file.Read(buf)
			if bytesRead > 0 {
				chunk := make([]byte, bytesRead)
				copy(chunk, buf[:bytesRead])
				validChunk, newLeftover := processChunkWithChans(chunk, leftover)
				leftover = newLeftover
				if len(validChunk) > 0 {
					parseWg.Add(1)
					chunckCount++
					if chunckCount % 5000 == 0 {
						log.Info().Msg(fmt.Sprintf("Processing chunk № %d", chunckCount))
						parseWg.Done()
						break
					}
					go processChunkData(validChunk, resultsChan, &parseWg)
				}
			}
			if err != nil {
				break
			}
		}
		parseWg.Wait()
		close(resultsChan)
	}()

	saveWg.Wait()

	// err = ParseFile(file, trie)

	// if err != nil {
	// 	log.Error().Err(err).Send()
	// 	return
	// }

	duration := time.Since(start)
	log.Info().Msg(fmt.Sprintf("finish parsing file, took %s", duration.String()))
	count := trie.CountUniqFullWords(trie.Root)
	// count := len(resultMap)
	duration = time.Since(start)
	// show result
	log.Info().Msg(fmt.Sprintf(`Total amount of uniq ip addresses = %d, with a total runtime of %s`, count, duration.String()))
}

func processChunkWithChans(chunk, leftover []byte) (validChunk, newLeftover []byte) {
	firstNewline := -1
	lastNewline := -1
	for i, b := range chunk {
		if b == '\n' {
			if firstNewline == -1 {
				firstNewline = i
			}
			lastNewline = i
		}
	}
	if firstNewline != -1 {
		validChunk = append(leftover, chunk[:lastNewline+1]...)
		newLeftover = make([]byte, len(chunk[lastNewline+1:]))
		copy(newLeftover, chunk[lastNewline+1:])
	} else {
		newLeftover = append(leftover, chunk...)
	}
	return validChunk, newLeftover
}

func processChunkData(chunk []byte, resultsChan chan<- map[string]string, wg *sync.WaitGroup) {
	defer wg.Done()

	parsedIps := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(string(chunk)))

	for scanner.Scan() {
		line := scanner.Text()
		parsedIps[line] = line
	}

	// Send the computed stats to resultsChan
	resultsChan <- parsedIps
}
