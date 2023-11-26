package main

import (
	"fmt"
	"NASPprojekat/BloomFilter"
	//"NASPprojekat/SkipList"
	//"NASPprojekat/CountMinSketch"
	//"NASPprojekat/HyperLogLog"
	//"NASPprojekat/BTree"
)

var elem1 = []byte("Stringic")

func main() {
	var bf = BloomFilter.BloomFilter{}
	bf.Init(100,0.2)
	fmt.Println(bf.Check_elem(elem1))
	bf.Add(elem1)
	fmt.Println(bf.Check_elem(elem1))
}