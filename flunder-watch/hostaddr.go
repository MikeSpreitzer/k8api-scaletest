package main

import (
	"fmt"
	"math/big"
	"net"
)

var last6 = net.ParseIP("FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF")

func GetHostAddr() string {
	addrs, err := net.InterfaceAddrs()
	fmt.Printf("InterfaceAddrs()=%s, %v\n", addrs, err)
	minIPBytes := last6
	minIPNat := BigNatFromBytes(minIPBytes)
	for _, addr := range addrs {
		addrStr := addr.String()
		ipBytes, _, err := net.ParseCIDR(addrStr)
		if err != nil {
			fmt.Errorf("net.ParseCIDR(%q) threw %#v\n", addrStr, err)
		}
		if ipBytes.IsLoopback() {
			continue
		}
		ipNat := BigNatFromBytes(ipBytes)
		if ipNat.Cmp(minIPNat) < 0 {
			fmt.Printf("Preferring %v from %s\n", ipNat, addrStr)
			minIPNat = ipNat
			minIPBytes = ipBytes
		}
	}
	return minIPBytes.String()
}

func BigNatFromBytes(bytes []byte) *big.Int {
	var ans big.Int
	ans.SetBytes(bytes)
	return &ans
}
