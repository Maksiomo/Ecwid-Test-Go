package ipstorage

// amount of bits we need to store ip value
const VALUE_SIZE = 6;
// bit mask
const VALUE_MASK = 0x3F;
// total bits needed for all posible ip addresses
const STORAGE_SIZE = uint32( 1 << (32 - VALUE_SIZE + 1))

var storage []uint32 = make([]uint32, STORAGE_SIZE)

// adds uinted ip to storage
func AddUint(ip uint32) {
	index := ip >> VALUE_SIZE
	value := ip & VALUE_MASK
	storage[index] |= uint32(1) << value
}

// counts all records in storage
func CountUintUniq() int {
	resMap := make(map[uint32]bool)
	for i := range storage {
		resMap[storage[i]] = true
	}
	return len(resMap)
}