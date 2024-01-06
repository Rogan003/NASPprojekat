package main

import (
	"fmt"
	//"NASPprojekat/BloomFilter"
	//"NASPprojekat/SkipList"
	//"NASPprojekat/CountMinSketch"
	//"NASPprojekat/HyperLogLog"
	//"NASPprojekat/BTree"
	//"NASPprojekat/MerkleTree"
	//"NASPprojekat/WriteAheadLog"
	"NASPprojekat/Memtable"
)

var elem1 = []byte("Stringic")

func main() {

	
	conf := config()
	/*var bf = BloomFilter.BloomFilter{}
	bf.Init(100,0.2)
	fmt.Println(bf.Check_elem(elem1))
	bf.Add(elem1)
	fmt.Println(bf.Check_elem(elem1))
	*/

	/*
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
	btree.Add(3)
	//btree.RootElem()
	//btree.RootChildElem()
	//btree.RootGrandChildElem()
	for _, value := range btree.AllElem() {
		fmt.Printf("%d ", value)
	}
	fmt.Printf("\n")

	_,_,isThere := btree.Find(16)
	fmt.Println(isThere)
	_,_,isThere = btree.Find(15)
	fmt.Println(isThere)
	*/

	
	mt := Memtable.Memtable{}
	mt.Init(conf.MemtableStructure, int(conf.MemtableSize))

	fmt.Println("Adding sv36!")
	mt.Add("sv36", []byte{10}, 1)

	fmt.Println("Adding sv48!")
	mt.Add("sv48", []byte{10}, 2)

	fmt.Println("Adding ab45!")
	mt.Add("ab45", []byte{6}, 3)

	fmt.Println("Adding de34!")
	mt.Add("de34", []byte{5}, 4)
	
	fmt.Println("Adding tr55!")
	mt.Add("tr55", []byte{7}, 5)

	fmt.Print("Get ii1! -> ")
	mt.Get("ii1")
	

	fmt.Println("Adding ii1!")
	mt.Add("ii1", []byte{9}, 6)

	fmt.Print("Get ii1! -> ")
	mt.Get("ii1")


	fmt.Println("Adding ii5!")
	mt.Add("ii5", []byte{8}, 7)

	fmt.Println("Adding ra4!")
	mt.Add("ra4", []byte{8}, 8)

	fmt.Println("Adding ra223!")
	mt.Add("ra223", []byte{6}, 9)

	fmt.Println("Adding ok12!")
	mt.Add("ok12", []byte{7}, 10)

	fmt.Println("Adding qw23!")
	mt.Add("qw23", []byte{10}, 11)

	fmt.Println("Adding yt4!")	
	mt.Add("yt4", []byte{8}, 12)

	fmt.Println("Adding pr49!")
	mt.Add("pr49", []byte{7}, 13)

	fmt.Println("Adding de52!")
	mt.Add("de52", []byte{9}, 14)

	fmt.Println("Adding aa21!")
	mt.Add("aa21", []byte{5}, 15)

	fmt.Println("Adding mr32!")
	mt.Add("mr32", []byte{8}, 16)

	fmt.Println("Adding mr21!")
	mt.Add("mr21", []byte{7}, 17)


	fmt.Println("Deleting yt4!")
	mt.Delete("yt4", 18)

	fmt.Println("Deleting sv36!")
	mt.Delete("sv36", 19)


	fmt.Println("Adding tt11!")
	mt.Add("tt11", []byte{9}, 14)

	fmt.Println("Adding tt12!")
	mt.Add("tt12", []byte{5}, 15)

	fmt.Println("Adding tt13!")
	mt.Add("tt13", []byte{8}, 16)

	fmt.Println("Adding tt14!")
	mt.Add("tt14", []byte{7}, 17)

	fmt.Println("Adding tt15!")
	mt.Add("tt15", []byte{7}, 17)
}