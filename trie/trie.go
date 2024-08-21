package trie

// init a trie
func NewTrie() *Trie {
	/*
	 create a root node, as it won't be used in calculations,
	 i've set it to a null character
	*/
	root := NewTrieNode()
	return &Trie{Root: root}
}

// adds a word to a trie
func (t *Trie) AddWord(word string) error {
	// we always start to traverse our trie from the root 
	currentNode := t.Root

	// we begin to insert characters of target word one by one
	for i := 0; i < len(word); i++ {
		/*
		 ASCII index of '.' == 46
		 ASCII index of digits from 0 to 9 are from 48 to 57
		 so in order to get index of needed child trie node
		 we subtract a '.' from a character, 
		 and if number is positive, subtract extra 1 
		*/
		
		childIndex := word[i] - '.'

		if childIndex > 0 {
			childIndex--
		}

		// if target child node is non existent -> create it
		if currentNode.Children[childIndex] == nil {
			currentNode.Children[childIndex] = NewTrieNode()
		}

		if i == len(word) - 1 {
			currentNode.Children[childIndex].IsEnd = true 
		}
		// move current node pointer to target node for next cycle
		currentNode = currentNode.Children[childIndex]
	}

	return nil
}

// a recursive function which count all full words in trie from target node
func (t *Trie) CountUniqFullWords(node *TrieNode) (int) {

	// if node is non existent return 0
	if (node == nil) {
		return 0
	}
	
	res := 0

	// if there is an ip end at target node, we add it to total
	if node.IsEnd {
		res++
	}

	// if it exists, than we count all existent end nodes for all their children
	for i := 0; i < 11; i++ {
		if node.Children[i] != nil {
			res += t.CountUniqFullWords(node.Children[i])
		}
	}

	return (res)
}