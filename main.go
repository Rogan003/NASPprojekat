package main

import (
	"fmt"
	"NASPprojekat/BloomFilter"
	//"NASPprojekat/SkipList"
	//"NASPprojekat/CountMinSketch"
	//"NASPprojekat/HyperLogLog"
	"NASPprojekat/BTree"
	//"NASPprojekat/MerkleTree"
)

var elem1 = []byte("Stringic")

func main() {
	var bf = BloomFilter.BloomFilter{}
	bf.Init(100,0.2)
	fmt.Println(bf.Check_elem(elem1))
	bf.Add(elem1)
	fmt.Println(bf.Check_elem(elem1))
	
	var btree = BTree.BTree{}
	btree.Init(4)
	btree.Add(10)
	btree.Add(8)
	btree.Add(14)
	btree.Add(5)
	btree.Add(4)
	btree.Add(12)
	btree.Add(19)
	btree.Add(7)
	btree.Add(20)
	btree.Add(11)
	btree.Add(2)
	btree.Add(9)
	btree.Add(16)
	btree.Add(22)
	btree.Add(13)

	// btree.RootElem()
	// btree.RootChildElem()

	for _, value := range btree.AllElem() {
		fmt.Printf("%d ", value)
	}
	fmt.Printf("\n")

	_,_,isThere := btree.Find(16)
	fmt.Println(isThere)
	_,_,isThere = btree.Find(15)
	fmt.Println(isThere)
}