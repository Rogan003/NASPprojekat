package main

import (
	"fmt"
	"os"
	"bufio"
	"strconv"
	"time"
	"NASPprojekat/Config"
	//"NASPprojekat/BloomFilter"
	// "NASPprojekat/SkipList"
	//"NASPprojekat/CountMinSketch"
	//"NASPprojekat/HyperLogLog"
	//"NASPprojekat/BTree"
	//"NASPprojekat/MerkleTree"
	"NASPprojekat/WriteAheadLog"
	"NASPprojekat/Memtable"
	"NASPprojekat/Cache"
	//"NASPprojekat/SSTable"
	//"NASPProjekat/engine.go"
	"NASPprojekat/TokenBucket"
)

var elem1 = []byte("Stringic")

func main() {
	conf, err := Config.ConfigInst()

	if err != nil {
		return
	}

	lsm := Config.NewLMSTree(conf)

	mt := Memtable.NMemtables{}
	mt.Init(conf.MemtableStructure, int(conf.MemtableSize), conf.MemtableNumber, lsm)

	tb := TokenBucket.TokenBucket{}
	tb.Init(conf.TokenBucketSize, time.Minute)

	wal, err := WriteAheadLog.NewWAL("files/WAL", 60000000000, 5) // ne znam ove parametre kako i sta?
	// inace ovo je putanja do foldera gde bi WAL segmenti mogli biti smesteni, ovaj ogroman broj je kao sat vremena za duration, i eto
	// low watermark lupih 5, ne znam gde treba conf.WalSize???

	if err != nil {
		return
	}

	cache := Cache.NewLRUCache(int(conf.CacheCapacity))

	fmt.Println("==================DOBRODOSLI==================")
	for{
		fmt.Println("1. PUT")
		fmt.Println("2. GET")
		fmt.Println("3. DELETE")
		fmt.Println("4. Moj Bloom Filter")
		fmt.Println("5. Moj HLL")
		fmt.Println("x. Izlaz")
		fmt.Print("Unesi broj opcije: ")

		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		option:= scanner.Text()
		if option == "x"{
			break
		}
		optionInt,_ := strconv.Atoi(option)

		switch optionInt { // DODATI PROVERE REZERVISANIH KLJUCEVA SVUDA, ONI NE SMEJU INACE BITI KORISCENI
			case 1:
				fmt.Println("Unesite kljuc elementa: ")
				scanner.Scan()
				key := scanner.Text()
				fmt.Println("Unesite vrednost elementa: ")
				scanner.Scan()
				value := scanner.Bytes()
				done := Put(wal, &mt, cache, key, value, &tb)

				if done {
					fmt.Printf("Uspesno dodat/azuriran kljuc %s!\n", key)
				} else {
					fmt.Printf("GRESKA! Neuspesno dodavanje kljuca %s!\n", key)
				}
			
			case 2:
				fmt.Println("Unesite kljuc elementa: ")
				scanner.Scan()
				key := scanner.Text()
				elem, done := Get(&mt, cache, key, &tb)

				if done {
					fmt.Printf("Vrednost pod kljucem %s: %s\n", key, elem)
				} else {
					fmt.Printf("GRESKA! Neuspesno dobavljanje kljuca %s!\n", key)
				}

			case 3:
				fmt.Println("Unesite kljuc elementa: ")
				scanner.Scan()
				key := scanner.Text()
				_, done := Delete(wal, &mt, cache, key, &tb)

				if done {
					fmt.Printf("Uspesno obrisan element pod kljucem %s!\n", key)
				} else {
					fmt.Printf("GRESKA! Neuspesno brisanje kljuca %s!\n", key)
				}

			case 4:
				for {
					fmt.Println("1. Kreiraj Bloom Filter")
					fmt.Println("2. Dodaj u Bloom Filter")
					fmt.Println("3. Proveri u Bloom Filteru")
					fmt.Println("4. Obrisi Bloom Filter")
					fmt.Println("x. Vrati se")
					fmt.Println("Unesite opciju: ")

					scannerhll:= bufio.NewScanner(os.Stdin)
					scannerhll.Scan()
					optionbf := scannerhll.Text()

					if optionbf == "x"{
						break
					}

					switch optionbf { // da li je bolje da se dodavanje u instancu i provera postojanja vrse u jednom istom delu?
						case "1":
							fmt.Println("Unesite kljuc bf: ")
							scanner.Scan()
							key := scanner.Text()
							key_real := "bf_" + key
							_, done := Get(&mt, cache, key_real, &tb)

							if done {
								fmt.Println("Greska! BF sa datim kljucem vec postoji!")
								continue
							}

							fmt.Println("Unesite broj ocekivanih elemenata: ")
							var expectedElements int
    						_, err := fmt.Scanf("%d", &expectedElements)
							if err != nil {
								fmt.Println("Greska pri unosu!")
								continue
							}

							fmt.Println("Unesite false positive rate: ")
							var falsePositiveRate float64
    						_, err = fmt.Scanf("%g", &falsePositiveRate)
							if err != nil {
								fmt.Println("Greska pri unosu!")
								continue
							}

							bytes, isOkay := CreateBF(expectedElements, falsePositiveRate)

							if isOkay {
								done = false
							} else {
								done = Put(wal, &mt, cache, key_real, bytes, &tb)
							}

							if done {
								fmt.Printf("Uspesno dodat kljuc bf %s!\n", key)
							} else {
								fmt.Printf("GRESKA! Neuspesno dodavanje kljuca bf %s!\n", key)
							}

						case "2":
							fmt.Println("Unesite kljuc bf: ")
							scanner.Scan()
							key := scanner.Text()
							key_real := "bf_" + key
							elem, done := Get(&mt, cache, key_real, &tb)

							if done {
								bf, err := DecodeBF(elem)

								if err {
									fmt.Printf("GRESKA! Neuspesno dobavljanje kljuca bf %s!\n", key)
									continue
								}

								fmt.Println("Unesite element koji zelite dodati u bf: ")
								scanner.Scan()
								value := scanner.Bytes()

								bf.Add(value)

								bytes, err := EncodeBF(bf)

								var done bool

								if err {
									done = false
								} else {
									done = Put(wal, &mt, cache, key_real, bytes, &tb)
								}

								if done {
									fmt.Printf("Uspesno dodat element %s u bf!\n", value)
								} else {
									fmt.Printf("GRESKA! Neuspesno dodavanje elementa %s u bf!\n", value)
								}
							} else {
								fmt.Printf("GRESKA! Neuspesno dobavljanje kljuca %s!\n", key)
							}

						case "3":
							fmt.Println("Unesite kljuc bf: ")
							scanner.Scan()
							key := scanner.Text()
							key_real := "bf_" + key
							elem, done := Get(&mt, cache, key_real, &tb)

							if done {
								bf, err := DecodeBF(elem)

								if err {
									fmt.Printf("GRESKA! Neuspesno dobavljanje kljuca bf %s!\n", key)
									continue
								}

								fmt.Println("Unesite element koji zelite proveriti u bf: ")
								scanner.Scan()
								value := scanner.Bytes()

								isThere := bf.Check_elem(value)

								if isThere {
									fmt.Printf("Element %s se nalazi u bf!\n", value) // mozda dodati nesto vise info oko ovoga
								} else {
									fmt.Printf("Element %s se ne nalazi u bf!\n", value)
								}

							} else {
								fmt.Printf("GRESKA! Neuspesno dobavljanje kljuca %s!\n", key)
							}

						case "4":
							fmt.Println("Unesite kljuc bf: ")
							scanner.Scan()
							key := scanner.Text()
							key_real := "bf_" + key
							_, done := Delete(wal, &mt, cache, key_real, &tb)

							if done {
								fmt.Printf("Uspesno obrisan element pod kljucem %s!\n", key)
							} else {
								fmt.Printf("GRESKA! Neuspesno brisanje kljuca %s!\n", key)
							}

						default:
							fmt.Println("Nepostojeca opcija. Pokusajte ponovo.")
					}
				}

			case 5:
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
				
				}

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