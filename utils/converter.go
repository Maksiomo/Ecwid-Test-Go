package utils

import (
	"encoding/binary"
	"net"
)

// function that parses stringified ip address from .txt file into lightweight uint32
func ConvertIpToInt(ipAddress string) uint32 {
	ip := net.ParseIP(ipAddress)
	return binary.BigEndian.Uint32(ip[12:16])
}