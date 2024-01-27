package main

import (
	"fmt"
	"os"
	"bufio"
	"strconv"
	//"NASPprojekat/Config"
	//"NASPprojekat/BloomFilter"
	// "NASPprojekat/SkipList"
	//"NASPprojekat/CountMinSketch"
	//"NASPprojekat/HyperLogLog"
	//"NASPprojekat/BTree"
	//"NASPprojekat/MerkleTree"
	// "NASPprojekat/WriteAheadLog"
	//"NASPprojekat/Memtable"
	//"NASPprojekat/Cache"
	//"NASPprojekat/SSTable"
	//"NASPProjekat/engine.go"
)

var elem1 = []byte("Stringic")

func main() {
	/*
	conf, _ := Config.ConfigInst()

	mt := Memtable.NMemtables{}
	mt.Init(conf.MemtableStructure, int(conf.MemtableSize), conf.MemtableNumber)
	
	mt.Add("sv36", []byte{10})
	mt.Add("sv48", []byte{10})
	mt.Add("ab45", []byte{6})
	mt.Add("de34", []byte{5})
	mt.Add("tr55", []byte{7})
	mt.Get("ii1")
	mt.Add("ii1", []byte{9})
	mt.Get("ii1")
	mt.Add("ii5", []byte{8})
	mt.Add("ra4", []byte{8})
	mt.Add("ra223", []byte{6})
	mt.Add("ok12", []byte{7})
	mt.Add("qw23", []byte{10})
	mt.Add("yt4", []byte{8})
	mt.Add("pr49", []byte{7})
	mt.Add("de52", []byte{9})
	mt.Add("aa21", []byte{5})
	mt.Add("mr32", []byte{8})
	mt.Add("mr21", []byte{7})
	mt.Delete("yt4")
	mt.Delete("sv36")
	*/

	fmt.Println("==================DOBRODOSLI==================")
	for{
		fmt.Println("1. Opcija 1")
		fmt.Println("2. Opcija 2")
		fmt.Println("3. Moj HLL")
		fmt.Println("4. Opcija 4")
		fmt.Println("5. Opcija 5")
		fmt.Println("x. Izlaz")
		fmt.Print("Unesi broj opcije: ")

		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		option:= scanner.Text()
		if option == "x"{
			break
		}
		optionInt,_ := strconv.Atoi(option)

		switch optionInt {
			case 3:

				fmt.Println("1. Kreiraj HLL")
				fmt.Println("2. Dodaj u HLL")
				fmt.Println("3. Kardinalnost")
				fmt.Println("4. Obrisi HLL")
				fmt.Println("x. Vrati se")
				fmt.Println("Unesite opciju: ")
				scannerhll:= bufio.NewScanner(os.Stdin)
				scannerhll.Scan()
				optionhll := scannerhll.Text()
				if optionhll == "x"{
					continue
				}

				optionhllInt,_ :=strconv.Atoi(optionhll)
				switch optionhllInt {
				case 1:
				
				

			case 'x':
				break

			default:
				fmt.Println("Nepostojeca opcija. Pokusajte ponovo.")
			}
		}
	}
	

	// hll :=HyperLogLog.Init(10)
	// //fmt.Println(hll.p,",",hll.m)
	// element1 := []byte("vanja")
	// element2 := []byte("vanja")
	// element3 := []byte("kostic")
	// element4 := []byte("sv292022")
	// element5 := []byte("asdfghjkl")
	// hll.Add(element1)
	// hll.Add(element3)
	// hll.Add(element2)
	// hll.Add(element4)
	// hll.Add(element5)

	// estimation := hll.Estimate()
	// fmt.Printf("Procenjena kardinalnost: %f\n", estimation)

	// hll.Delete()
	// estimation2 := hll.Estimate()
	// fmt.Printf("Procenjena kardinalnost: %f\n", estimation2)

	
	// hll.Serialize("files/hyperloglog.gob")
	// hll.Deserialize("files/hyperloglog.gob")

	// hll.DeleteHLL()
	// wal,_ := WriteAheadLog.NewWAL("files/WAL",10000000000,10)
	// mt := Memtable.Memtable{}
	// mt.Init(conf.MemtableStructure, int(conf.MemtableSize))
	// engine.Put(wal,mt, "sv29", "vanja")

	/*
	conf := config()
	var bf = BloomFilter.BloomFilter{}
	bf.Init(100,0.2)
	fmt.Println(bf.Check_elem(elem1))
	bf.Add(elem1)
	fmt.Println(bf.Check_elem(elem1))
	*/

		/*
	var btree = BTree.BTree{}
	btree.Init(4)
	btree.Add("sv36", []byte{10})
	btree.Add("aa1", []byte{8})
	btree.Add("ab5", []byte{9})
	btree.Add("fd4", []byte{5})
	btree.Add("ac3", []byte{110})
	btree.Add("bw3", []byte{104})
	btree.Add("pw2", []byte{50})
	btree.Add("zsw23", []byte{16})
	btree.Add("ok43", []byte{34})
	btree.Add("aw12", []byte{21})
	btree.Add("xs3", []byte{70})
	btree.Add("ud43", []byte{91})
	btree.Add("mo023", []byte{34})
	btree.Add("ko23", []byte{12})
	btree.Add("sw231", []byte{61})
	btree.Add("hg01", []byte{78})
	//btree.RootElem()
	//btree.RootChildElem()
	//btree.RootGrandChildElem()
	for _, value := range btree.AllElem() {
		fmt.Printf("%s %d %t %s\n", value.Transaction.Key, value.Transaction.Value, value.Tombstone, value.ToByte())
	}
	fmt.Printf("\n")

	_,_,isThere,_ := btree.Find("sv36")
	fmt.Println(isThere)
	_,_,isThere,_ = btree.Find("sv37")
	fmt.Println(isThere)
	*/

	/*
	mt := Memtable.Memtable{}
	mt.Init(conf.MemtableStructure, int(conf.MemtableSize))

	mt.Add("sv36", []byte{10})
	mt.Add("sv48", []byte{10})
	mt.Add("ab45", []byte{6})
	mt.Add("de34", []byte{5})
	mt.Add("tr55", []byte{7})
	mt.Get("ii1")
	mt.Add("ii1", []byte{9})
	mt.Get("ii1")
	mt.Add("ii5", []byte{8})
	mt.Add("ra4", []byte{8})
	mt.Add("ra223", []byte{6})
	mt.Add("ok12", []byte{7})
	mt.Add("qw23", []byte{10})
	mt.Add("yt4", []byte{8})
	mt.Add("pr49", []byte{7})
	mt.Add("de52", []byte{9})
	mt.Add("aa21", []byte{5})
	mt.Add("mr32", []byte{8})
	mt.Add("mr21", []byte{7})
	mt.Delete("yt4")
	mt.Delete("sv36")
	*/


	// wal := WriteAheadLog.
	// wal,_:= WriteAheadLog.NewWAL("files/WAL",10000000000,10)
	// fmt.Println(wal.Duration(),",",wal.Path())
	// t := WriteAheadLog.NewTransaction("vanja","kostic")
	// e := WriteAheadLog.NewEntry(false, t)

	// cache := Cache.NewLRUCache(3)

	// cache.Insert("key1", "vanja")
	// cache.Insert("key2", "kostic")

	// fmt.Println(cache.Get("key1")) 
	// fmt.Println(cache.Get("key2")) 
	// fmt.Println(cache.Get("key3")) 

	// cache.Insert("key3", "sv29") 
	// cache.Insert("key4", "2022") 
	// fmt.Println(cache.Get("key3")) 
	// fmt.Println(cache.Get("key4")) 
	// fmt.Println(cache.Get("key1")) 

}