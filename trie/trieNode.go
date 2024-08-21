package trie

// create a Trie Node
func NewTrieNode() *TrieNode {
	// init a trieNode with target value
	trieNode := &TrieNode{IsEnd: false}
	for i := 0; i < 11; i ++ {
		// init all possible children of that node
		trieNode.Children[i] = nil
	}
	return trieNode
}
