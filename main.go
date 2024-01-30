package main

import (
	"NASPprojekat/Config"
	"NASPprojekat/SimHash"
	"bufio"
	"fmt"
	"os"
	"strconv"
	"time"

	//"NASPprojekat/BloomFilter"
	// "NASPprojekat/SkipList"
	//"NASPprojekat/CountMinSketch"
	//"NASPprojekat/HyperLogLog"
	//"NASPprojekat/BTree"
	//"NASPprojekat/MerkleTree"
	"NASPprojekat/Cache"
	"NASPprojekat/Memtable"
	"NASPprojekat/WriteAheadLog"

	//"NASPprojekat/SSTable"
	//"NASPprojekat/engine.go"
	"NASPprojekat/TokenBucket"
)

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

	/*
		mt.Init(conf.MemtableStructure, int(conf.MemtableSize), conf.MemtableNumber, lsm)
		mt.Add("2", make([]byte, 10))
		mt.Arr[mt.R].Flush(lsm)
	*/

	/*
		PROVERA ZA KREIRANJE FAJLOVA SSTABLE
			mt.Add("2", make([]byte, 10))
			mt.Arr[mt.R].Flush(lsm)
	*/

	wal, err := WriteAheadLog.NewWAL("files_WAL/", nil, 60000000000, 5, conf.WalSize) // ne znam ove parametre kako i sta?
	// inace ovo je putanja do foldera gde bi WAL segmenti mogli biti smesteni, ovaj ogroman broj je kao sat vremena za duration, i eto
	// low watermark lupih 5, ne znam gde treba conf.WalSize??? ja sam ga lupio da bude segment size?

	if err != nil {
		fmt.Println("Greska pri ucitavanju sistema!")
		return
	}

	err = wal.OpenWAL()

	if err != nil {
		fmt.Println("Greska pri ucitavanju sistema!")
		return
	}

	// err = wal.RemakeWAL(&mt)

	// if err != nil {
	// 	fmt.Println("Greska pri ucitavanju sistema!")
	// 	return
	// }

	cache := Cache.NewLRUCache(int(conf.CacheCapacity))

	fmt.Println("==================DOBRODOSLI==================")
	for {
		fmt.Println("1. PUT")
		fmt.Println("2. GET")
		fmt.Println("3. DELETE")
		fmt.Println("4. Moj Bloom Filter")
		fmt.Println("5. Moj HLL")
		fmt.Println("6. Moj CMS")
		fmt.Println("7. Moj SimHash")
		fmt.Println("8. Range Scan")
		fmt.Println("9. Prefix Scan")
		fmt.Println("10. Range Iter")
		fmt.Println("11. Prefix Iter")
		fmt.Println("x. Izlaz")
		fmt.Print("Unesi broj opcije: ")

		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		option := scanner.Text()
		if option == "x" {
			break
		}
		optionInt, _ := strconv.Atoi(option)

		switch optionInt {
		case 1:
			fmt.Println("Unesite kljuc elementa: ")
			scanner.Scan()
			key := scanner.Text()

			if len(key) > 4 && (key[0:3] == "bf_" || key[0:4] == "cms_" || key[0:4] == "hll_" || key[0:3] == "sh_" || key[0:3] == "tb_") {
				fmt.Println("Uneti kljuc pocinje sa zabranjenim sistemskim prefiksom (karakteri do _ sa _)!")
				continue
			}

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

			if len(key) > 4 && (key[0:3] == "bf_" || key[0:4] == "cms_" || key[0:4] == "hll_" || key[0:3] == "sh_" || key[0:3] == "tb_") {
				fmt.Println("Uneti kljuc pocinje sa zabranjenim sistemskim prefiksom (karakteri do _ sa _)!")
				continue
			}

			elem, done := Get(&mt, cache, key, &tb, lsm)

			if done {
				fmt.Printf("Vrednost pod kljucem %s: %s\n", key, elem)
			} else {
				fmt.Printf("GRESKA! Neuspesno dobavljanje kljuca %s!\n", key)
			}

		case 3:
			fmt.Println("Unesite kljuc elementa: ")
			scanner.Scan()
			key := scanner.Text()

			if len(key) > 4 && (key[0:3] == "bf_" || key[0:4] == "cms_" || key[0:4] == "hll_" || key[0:3] == "sh_" || key[0:3] == "tb_") {
				fmt.Println("Uneti kljuc pocinje sa zabranjenim sistemskim prefiksom (karakteri do _ sa _)!")
				continue
			}

			_, done := Delete(wal, &mt, cache, key, &tb, lsm)

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

				scannerhll := bufio.NewScanner(os.Stdin)
				scannerhll.Scan()
				optionbf := scannerhll.Text()

				if optionbf == "x" {
					break
				}

				switch optionbf { // da li je bolje da se dodavanje u instancu i provera postojanja vrse u jednom istom delu?
				case "1":
					fmt.Println("Unesite kljuc bf: ")
					scanner.Scan()
					key := scanner.Text()
					key_real := "bf_" + key
					_, done := Get(&mt, cache, key_real, &tb, lsm)

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
					elem, done := Get(&mt, cache, key_real, &tb, lsm)

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
					elem, done := Get(&mt, cache, key_real, &tb, lsm)

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
					_, done := Delete(wal, &mt, cache, key_real, &tb, lsm)

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
			for {
				fmt.Println("1. Kreiraj HLL")
				fmt.Println("2. Dodaj u HLL")
				fmt.Println("3. Kardinalnost")
				fmt.Println("4. Obrisi HLL")
				fmt.Println("x. Vrati se")
				fmt.Println("Unesite opciju: ")
				scannerhll := bufio.NewScanner(os.Stdin)
				scannerhll.Scan()
				optionhll := scannerhll.Text()
				if optionhll == "x" {
					continue
				}

				optionhllInt, _ := strconv.Atoi(optionhll)
				switch optionhllInt {
				case 1:
					fmt.Println("Unesite kljuc hll: ")
					scanner.Scan()
					key := scanner.Text()
					key_real := "hll_" + key
					_, done := Get(&mt, cache, key_real, &tb, lsm)

					if done {
						fmt.Println("Greska! HLL sa datim kljucem vec postoji! ")
						continue
					}

					fmt.Println("Unesite preciznost :")
					var precision int
					_, err := fmt.Scanf("%d", &precision)
					if err != nil {
						fmt.Println("Greska pri unosu! ")
						continue
					}

					bytes, isOk := CreateHLL(uint8(precision))

					if isOk {
						done = false
					} else {
						done = Put(wal, &mt, cache, key_real, bytes, &tb)
					}

					if done {
						fmt.Printf("Uspesno dodat kljuc hll %s!\n", key)
					} else {
						fmt.Printf("GRESKA! Neuspesno dodavanje kljuca hll %s!\n", key)
					}

				case 2:
					fmt.Println("Unesite kljuc hll: ")
					scanner.Scan()
					key := scanner.Text()
					key_real := "hll_" + key
					elem, done := Get(&mt, cache, key_real, &tb, lsm)

					if done {
						hll, err := DecodeHLL(elem)

						if err {
							fmt.Printf("GRESKA! Neuspesno dobavljanje kljuca hll %s!\n", key)
							continue
						}

						fmt.Println("Unesite element koji zelite dodati u hll: ")
						scanner.Scan()
						value := scanner.Bytes()

						hll.Add(value)

						bytes, err := EncodeHLL(hll)

						var done bool

						if err {
							done = false
						} else {
							done = Put(wal, &mt, cache, key_real, bytes, &tb)
						}

						if done {
							fmt.Printf("Uspesno dodat element %s u hll!\n", value)
						} else {
							fmt.Printf("GRESKA! Neuspesno dodavanje elementa %s u hll!\n", value)
						}
					} else {
						fmt.Printf("GRESKA! Neuspesno dobavljanje kljuca %s!\n", key)
					}

				case 3:
					fmt.Println("Unesite kljuc hll: ")
					scanner.Scan()
					key := scanner.Text()
					key_real := "hll_" + key
					elem, done := Get(&mt, cache, key_real, &tb, lsm)

					if done {
						hll, err := DecodeHLL(elem)

						if err {
							fmt.Printf("GRESKA! Neuspesno dobavljanje kljuca hll %s!\n", key)
							continue
						}

						estimation := hll.Estimate()
						fmt.Printf("Kardinalnost je %f.\n", estimation)

					} else {
						fmt.Printf("GRESKA! Neuspesno dobavljanje kljuca %s!\n", key)
					}

				case 4:
					fmt.Println("Unesite kljuc hll: ")
					scanner.Scan()
					key := scanner.Text()
					key_real := "hll_" + key
					_, done := Delete(wal, &mt, cache, key_real, &tb, lsm)

					if done {
						fmt.Printf("Uspesno obrisan element pod kljucem %s!\n", key)
					} else {
						fmt.Printf("GRESKA! Neuspesno brisanje kljuca %s!\n", key)
					}
				default:
					fmt.Println("Nepostojeca opcija. Pokusajte ponovo.")
				}
			}

		case 6:
			fmt.Println("1. Kreiraj CMS")
			fmt.Println("2. Dodaj u CMS")
			fmt.Println("3. Provera ucestalosti u CMS")
			fmt.Println("4. Obrisi CMS")
			fmt.Println("x. Vrati se")
			fmt.Println("Unesite opciju: ")
			scanner := bufio.NewScanner(os.Stdin)
			scanner.Scan()
			option := scanner.Text()
			if option == "x" {
				break
			}
			optionInt, _ := strconv.Atoi(option)

			switch optionInt {
			case 1:
				fmt.Println("Unesite kljuc cms: ")
				scanner.Scan()
				key := scanner.Text()
				key_real := "cms_" + key
				_, done := Get(&mt, cache, key_real, &tb, lsm)

				if done {
					fmt.Println("Greska! CMS sa datim kljucem vec postoji!")
					continue
				}

				fmt.Println("Unesite sirinu cms-a: ")
				var width float64
				_, err := fmt.Scanf("%g", &width)
				if err != nil {
					fmt.Println("Greska pri unosu!")
					continue
				}
				fmt.Println("Unesite broj hash funkcija cms-a: ")
				var depth float64
				_, err = fmt.Scanf("%g", &depth)
				if err != nil {
					fmt.Println("Greska pri unosu!")
					continue
				}
				bytes, isOkay := CreateCMS(width, depth)
				if isOkay {
					done = false
				} else {
					done = Put(wal, &mt, cache, key_real, bytes, &tb)
				}

				if done {
					fmt.Printf("Uspesno dodat kljuc cms %s!\n", key)
				} else {
					fmt.Printf("GRESKA! Neuspesno dodavanje kljuca cms %s!\n", key)
				}
			case 2:
				fmt.Println("Unesite kljuc cms: ")
				scanner.Scan()
				key := scanner.Text()
				key_real := "cms_" + key
				elem, done := Get(&mt, cache, key_real, &tb, lsm)

				if done {
					cms, err := DecodeCMS(elem)

					if err {
						fmt.Printf("GRESKA! Neuspesno dobavljanje kljuca cms %s!\n", key)
						continue
					}
					fmt.Println("Unesite element koji zelite dodati u cms: ")
					scanner.Scan()
					value := scanner.Text()

					cms.AddToCMS(value)

					bytes, err := EncodeCMS(cms)

					var done bool

					if err {
						done = false
					} else {
						done = Put(wal, &mt, cache, key_real, bytes, &tb)
					}

					if done {
						fmt.Printf("Uspesno dodat element %s u cms!\n", value)
					} else {
						fmt.Printf("GRESKA! Neuspesno dodavanje elementa %s u cms!\n", value)
					}
				} else {
					fmt.Printf("GRESKA! Neuspesno dobavljanje kljuca %s!\n", key)
				}
			case 3:
				fmt.Println("Unesite kljuc cms: ")
				scanner.Scan()
				key := scanner.Text()
				key_real := "cms_" + key
				elem, done := Get(&mt, cache, key_real, &tb, lsm)

				if done {
					cms, err := DecodeCMS(elem)

					if err {
						fmt.Printf("GRESKA! Neuspesno dobavljanje kljuca cms %s!\n", key)
						continue
					}

					fmt.Println("Unesite element koji zelite proveriti u cms: ")
					scanner.Scan()
					value := scanner.Text()

					min := cms.SearchCSM(value)

					fmt.Printf("Min broj pojavljivanja elementa u cms je %d\n", min)

				} else {
					fmt.Printf("GRESKA! Neuspesno dobavljanje kljuca %s!\n", key)
				}
			case 4:
				fmt.Println("Unesite kljuc cms: ")
				scanner.Scan()
				key := scanner.Text()
				key_real := "cms_" + key
				_, done := Delete(wal, &mt, cache, key_real, &tb, lsm)
				if done {
					fmt.Printf("Uspesno obrisan element pod kljucem %s!\n", key)
				} else {
					fmt.Printf("GRESKA! Neuspesno brisanje kljuca %s!\n", key)
				}
			default:
				fmt.Println("Nepostojeca opcija. Pokusajte ponovo.")
			}

		case 7:
			fmt.Println("1. Cuvanje fingerprinta")
			fmt.Println("2. Hemingova udaljenost")
			fmt.Println("Unesite opciju: ")
			scanner := bufio.NewScanner(os.Stdin)
			scanner.Scan()
			option := scanner.Text()
			if option == "x" {
				break
			}
			optionInt, _ := strconv.Atoi(option)
			switch optionInt {
			case 1:
				fmt.Println("Unesite zeljeni tekst za simhash: ")
				scanner.Scan()
				text := scanner.Text()
				key_real := "sh_" + text
				_, done := Get(&mt, cache, key_real, &tb, lsm)
				if done {
					fmt.Println("Greska! Text vec postoji!")
					continue
				} else {
					textBytes := SimHash.SimHash(text)
					done = Put(wal, &mt, cache, key_real, textBytes[:], &tb)
					if done {
						fmt.Printf("Uspesno dodat element u simhash!\n")
					} else {
						fmt.Printf("GRESKA! Neuspesno dodavanje elementa u simhash!\n")
					}
				}
			case 2:
				fmt.Println("Unesite jedan tekst za racunanje hemingove distance: ")
				scanner.Scan()
				text1 := scanner.Text()
				key_real1 := "sh_" + text1
				elem1, done1 := Get(&mt, cache, key_real1, &tb, lsm)
				var textBytes1 [16]byte
				if done1 {
					copy(textBytes1[:], elem1)
				} else {
					textBytes1 = SimHash.SimHash(text1)
				}

				fmt.Println("Unesite drugi tekst za racunanje hemingove distance: ")
				scanner.Scan()
				text2 := scanner.Text()
				key_real2 := "sh_" + text2
				elem2, done2 := Get(&mt, cache, key_real2, &tb, lsm)
				var textBytes2 [16]byte
				if done2 {
					copy(textBytes2[:], elem2)
				} else {
					textBytes2 = SimHash.SimHash(text2)
				}
				distance := SimHash.HammingDistance(textBytes1, textBytes2)
				fmt.Printf("Hemingova distanca za trazene tekstove je %d\n", distance)
			default:
				fmt.Println("Nepostojeca opcija. Pokusajte ponovo.")
			}

		case 8:
			fmt.Println("Unesite opseg za skeniranje: ")
			var key1, key2 string
			fmt.Printf("Unesite prvi kljuc: ")
			fmt.Scanf("%s", &key1)
			fmt.Printf("\nUnesite drugi kljuc: ")
			fmt.Scanf("%s", &key2)
			RangeScan(&mt, key1, key2, conf.PageSize)

		case 9:
			var key string
			fmt.Printf("Unesite prefiks za skeniranje: ")
			fmt.Scanf("%s", &key)
			PrefixScan(&mt, key, conf.PageSize)

		case 10:
			fmt.Println("Unesite opseg za iteriranje: ")
			var key1, key2 string
			fmt.Printf("Unesite prvi kljuc: ")
			fmt.Scanf("%s", &key1)
			fmt.Printf("\nUnesite drugi kljuc: ")
			fmt.Scanf("%s", &key2)
			RangeIter(&mt, key1, key2)

		case 11:
			var key string
			fmt.Printf("Unesite prefiks za iteriranje: ")
			fmt.Scanf("%s", &key)
			PrefixIter(&mt, key)

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
