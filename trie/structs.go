package trie

// struct, explaining prefix tree structure 
type Trie struct {
	Root *TrieNode
}

// struct, explaining prefix tree node structure
type TrieNode struct {
	IsEnd bool
	// a slice of children trie nodes, each node has 11 children (0..9 + '.')
	Children [11]*TrieNode
}