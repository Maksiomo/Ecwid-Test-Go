package main

import (
	"bufio"
	"ecwid-go-task/trie"
	"fmt"
	"log"
	"os"
	"time"
)

func main() {

	// init trie node
	trie := trie.NewTrie()

	// init connection to file
	file, err := os.Open(os.Getenv("TARGET_FILE_PATH"))
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	start := time.Now()

	// add words to trie
	for scanner.Scan() {
		trie.AddWord(scanner.Text())
	}

	count := trie.CountUniqFullWords(trie.Root)

	duration := time.Since(start)
	// show result
	fmt.Printf("Total amount of uniq ip addresses = %d,\n with a total runtime of %s", count, duration.String())
}
