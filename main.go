package main

import (
	"bufio"
	"ecwid-go-task/trie"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"time"

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

	log.Info().Msg("Init trie")
	trie := trie.NewTrie()

	log.Info().Msg("Init connection to file")
	file, err := os.Open(os.Getenv("TARGET_FILE_PATH"))
	if err != nil {
		log.Error().Err(err).Send()
		return
	}

	defer file.Close()

	start := time.Now()

	log.Info().Msg("Parsing file with ip addresses")
	err = ParseFile(file, trie)

	if err != nil {
		log.Error().Err(err).Send()
		return
	}

	log.Info().Msg("finish parsing file")
	count := trie.CountUniqFullWords(trie.Root)

	duration := time.Since(start)
	// show result
	log.Info().Msg(fmt.Sprintf("Total amount of uniq ip addresses = %d,\n with a total runtime of %s", count, duration.String()))
}

func ParseFile(file *os.File, trie *trie.Trie) error {
	linesPool := sync.Pool{New: func() interface{} {
		return make([]byte, 250 * 1024)
	}}

	stringPool := sync.Pool{New: func() interface{} {
		lines := ""
		return &lines
	}}

	r := bufio.NewReader(file)

	var wg sync.WaitGroup

	for {
		bufPtr := linesPool.Get() //.([]uint8)

		var buf []byte

		switch v := bufPtr.(type) {
		default: {
			log.Error().Err(fmt.Errorf("invalid ptr type %T", v))
			continue
		}
		case *string: {
			buf = []byte(*bufPtr.(*string))
		}
		case []uint8: {
			buf = []byte(bufPtr.([]uint8))
		} 
		}

		n, err := r.Read(buf)
		buf = buf[:n]

		if n == 0 {
			if err != nil {
				fmt.Println(err)
				break
			}
			if err == io.EOF {
				break
			}
			return err
		}

		nextUntillNewline, err := r.ReadBytes('\n')

		if err != io.EOF {
			buf = append(buf, nextUntillNewline...)
		}

		wg.Add(1)
		go func() {
			ProcessChunk(buf, &linesPool, &stringPool, trie)
			wg.Done()
		}()

	}

	wg.Wait()
	return nil
}

func ProcessChunk(chunk []byte, linesPool, stringPool *sync.Pool, trie *trie.Trie) {
	var chunkWg sync.WaitGroup

	lines := *stringPool.Get().(*string)
	lines = string(chunk)

	linesPool.Put(&lines)

	linesSlice := strings.Split(lines, "\n")

	stringPool.Put(&lines)

	chunkSize := 300
	n := len(linesSlice)
	totalProcessThreads := n / chunkSize

	if n%chunkSize != 0 {
		totalProcessThreads++
	}

	for i := 0; i < totalProcessThreads; i++ {
		chunkWg.Add(1)

		go func(s int, e int) {
			defer chunkWg.Done()
			for i := s; i < e; i++ {
				text := linesSlice[i]
				if len(text) == 0 {
					continue
				}
				trie.AddWord(text)
			}
		}(i*chunkSize, int(math.Min(float64((i+1)*chunkSize), float64(len(linesSlice)))))
	}

	chunkWg.Wait()
	linesSlice = nil
}