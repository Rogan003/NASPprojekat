package main

import (
	"NASPprojekat/BloomFilter"
	"NASPprojekat/Cache"
	"NASPprojekat/Config"
	"NASPprojekat/CountMinSketch"
	"NASPprojekat/HyperLogLog"
	"NASPprojekat/Memtable"
	"NASPprojekat/SSTable"
	"NASPprojekat/TokenBucket"
	"NASPprojekat/WriteAheadLog"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"strconv"
)

func Get(memtable *Memtable.NMemtables, cache *Cache.LRUCache, key string, tb *TokenBucket.TokenBucket, lsm *Config.LSMTree) ([]byte, bool) {

	ok := tb.ConsumeToken()
	if !ok {
		fmt.Print("\nGreska! Previse obavljenih requestova u odredjenom vremenskom rasponu!\n")
		return nil, false
	}

	data, found, _ := memtable.Get(key)
	if found {
		fmt.Println("Pronađeno u Memtable.")
		return data, true
	}

	// u cache nema provera tombstone???
	data, err := convertToBytes(cache.Get(key))
	if err == nil {
		fmt.Println("Pronađeno u cache.")
		return data, true
	}

	foundBF, fileBF := SearchTroughBloomFilters(key, lsm) // trazi u disku
	if foundBF {                                          // ovde nesto potencijalno ne valja, mozda treba dodati putanje u bloomFilterFilesNames?
		fmt.Println("Mozda postoji na disku.")
		//ucitavamo summary i index fajlove za sstable u kojem je mozda element (saznali preko bloomfiltera)
		summaryFileName := lsm.SummaryFilesNames[fileBF]
		indexFileName := lsm.IndexFilesNames[fileBF]
		foundValue := SSTable.Get(key, summaryFileName, indexFileName, lsm.DataFilesNames[fileBF])

		if reflect.DeepEqual(foundValue, []byte{}) {
			return nil, false
		}

		cache.Insert(key, foundValue) // dodavanje u cache
		return foundValue, true       // foundValue prazno nesto bukvalno nista sad prvi put kad sam gledao?
	}
	return nil, false

}

// funkcija koja pretvara iz interface (get u cache vraca) u niz bajtova
func convertToBytes(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return v, nil
	default:
		return []byte{}, fmt.Errorf("cannot convert %T to string", value)
	}
}

// trazenje elementa sa nekim kljucem u svim bloomfilterima
func SearchTroughBloomFiltersOneFile(key string, lsm *Config.LSMTree) (bool, int) {
	bf := BloomFilter.BloomFilter{}
	// kada se ponovo pokrene ne prepoznaje da postoji bilo sta u nizu
	for i := 0; i < len(lsm.OneFilesNames); i++ {
		file, _ := os.OpenFile(lsm.OneFilesNames[i], os.O_RDWR, 0777)
		defer file.Close()
		//skacemo na pocetak bloom zapisa
		file.Seek(3*Config.KEY_SIZE_SIZE, 0)
		bfSizeBytes := make([]byte, Config.KEY_SIZE_SIZE)
		_, err := file.Read(bfSizeBytes)
		bfSize := int64(binary.LittleEndian.Uint64(bfSizeBytes))
		bfBytes := make([]byte, bfSize)
		//citanje bf
		err = bf.FromBytes(bfBytes)
		//trazenje u bf
		found := bf.Check_elem([]byte(key))
		if found {
			return found, i
		}
		if err != nil {
			println("Nije moguce ucitati bf kod searchtroughbfonefile")
		}

	}
	return false, -1
}

// trazenje elementa sa nekim kljucem u svim bloomfilterima
func SearchTroughBloomFilters(key string, lsm *Config.LSMTree) (bool, int) {
	bf := BloomFilter.BloomFilter{}
	// kada se ponovo pokrene ne prepoznaje da postoji bilo sta u nizu
	for i := 0; i < len(lsm.BloomFilterFilesNames); i++ {
		err := bf.Deserialize(lsm.BloomFilterFilesNames[i])
		if err != nil && err != io.EOF {
			return false, -1
		}
		found := bf.Check_elem([]byte(key))
		if found {
			return found, i
		}

	}
	return false, -1
}

func RangeScan(memtable *Memtable.NMemtables, key1 string, key2 string, pageSize int, lsm *Config.LSMTree) {
	// sta se desava?
	// otvaramo sve sstable i ovu listu elem iz memtabele
	// kreiramo neku listu pokazivaca za sstable (inicijalno -1, komada koliko ima sstable-a)
	// pozicioniramo se u sstable pomocu index-a i summary-a, tako sto idemo do prvog kljuca elem koji pripada range
	// zatim najmanji dodajemo u listu za ispis i inkrementiramo pokazivac, ako je kljuc elem na tom pokazivacu veci od key2 (upper range), zatvaramo taj fajl, pokazivac na -1 itd
	// ponavljamo ovako nekakav proces i kad dodjemo u situaciju da smo ispunili pageSize elem, ispisemo ih?
	// ako korisnik kaze da nastavlja jos onda mu ispisemo jos, ako ne, onda je je to kraj ove fje
	// smisliti efikasan nacin za odrzavanje nekih stranica u cache (da li treba uopste?)

	// DODATI PRESKAKANJE ZA STRUKTURE KAD IH ZAVRSIMO
	// Kad nisu svi elementi na stranici, ispise elem, ali posle opet pri vracanju unapred pravi gresku, test3-test9 range scan 	RADI
	// Kada idemo test prefix	RADI
	// test3-test8 range scan	RADI
	// test3-test9 range iter	RADI

	if key1 > key2 {
		fmt.Println("Greska! Opseg nije moguc!")

	} else {
		mem_indexes := make([]int, memtable.N)

		for index, value := range memtable.Arr {
			memElems := value.GetSortedElems()
			mem_indexes[index] = -1

			for index2, value := range memElems {
				if value.Transaction.Key >= key1 && value.Transaction.Key <= key2 {
					mem_indexes[index] = index2
					break
				}
			}

			// fmt.Println(mem_indexes[index])
		}

		sstables := lsm.DataFilesNames
		indexes := make([]int, len(sstables))

		// postavljamo sve na -1 kako bi znali ako neki sstable nema range koji nama treba
		for index, _ := range indexes {
			indexes[index] = -1
		}

		// citanje pozicija u index fajlovima odakle treba da se krece skeniranje
		for index, _ := range sstables {
			sumarryFileName := lsm.SummaryFilesNames[index]
			indexFileName := lsm.IndexFilesNames[index]

			//iz summary citamo opseg kljuceva u sstable (prvi i poslendji)
			sumarryFile, _ := os.OpenFile(sumarryFileName, os.O_RDWR, 0777)
			summary := SSTable.LoadSummary(sumarryFile)
			defer sumarryFile.Close()

			// ako je trazeni kljuc u tom opsegu, podatak bi trebalo da se nalazi u ovom sstable
			if !(summary.FirstKey > key2 || summary.LastKey < key1) {

				var indexPosition = uint64(0)
				for {
					//citamo velicinu kljuca
					keySizeBytes := make([]byte, SSTable.KEY_SIZE_SIZE)
					_, err := sumarryFile.Read(keySizeBytes)
					keySize := int64(binary.LittleEndian.Uint64(keySizeBytes))

					//citamo keySize bajtova da bi dobili kljuc
					keyValue := make([]byte, keySize)
					_, err = sumarryFile.Read(keyValue)
					if err != nil {
						if err == io.EOF {
							file, err := os.OpenFile(indexFileName, os.O_RDWR, 0777)
							if err != nil {
								log.Fatal(err)
							}
							defer file.Close()
							// od date pozicije citamo
							_, err = file.Seek(int64(indexPosition), 0)
							if err != nil {
								log.Fatal(err)
							}

							for {
								currentKey, position := SSTable.ReadFromIndex(file)
								if position == -1 {
									break
								}
								if currentKey > key2 {
									break
								}
								if currentKey >= key1 && currentKey <= key2 {
									indexes[index] = int(position)
									break
								}
							}

							break
						}
						panic(err)
					}

					if string(keyValue) > key1 {
						file, err := os.OpenFile(indexFileName, os.O_RDWR, 0777)
						if err != nil {
							log.Fatal(err)
						}
						defer file.Close()
						// od date pozicije citamo
						_, err = file.Seek(int64(indexPosition), 0)
						if err != nil {
							log.Fatal(err)
						}

						for {
							currentKey, position := SSTable.ReadFromIndex(file)
							if currentKey > key2 {
								break
							}
							if currentKey >= key1 && currentKey <= key2 {
								indexes[index] = int(position)
								break
							}
						}

						break
					} else {
						// citanje pozicije za taj kljuc u indexFile
						positionBytes := make([]byte, SSTable.KEY_SIZE_SIZE)
						_, err = sumarryFile.Read(positionBytes)
						position := binary.LittleEndian.Uint64(positionBytes)
						indexPosition = position
						if err != nil {
							if err == io.EOF {
								file, err := os.OpenFile(indexFileName, os.O_RDWR, 0777)
								if err != nil {
									log.Fatal(err)
								}
								defer file.Close()
								// od date pozicije citamo
								_, err = file.Seek(int64(indexPosition), 0)
								if err != nil {
									log.Fatal(err)
								}

								for {
									currentKey, position := SSTable.ReadFromIndex(file)
									if position == -1 {
										break
									}

									if currentKey > key2 {
										break
									}

									if currentKey >= key1 && currentKey <= key2 {
										indexes[index] = int(position)
										break
									}
								}

								break
							}
							fmt.Println(err)
							break
						}
					}
				}
			}

			// fmt.Println(indexes[index])
		}

		forward := true
		works := true
		lastElemsTables := make([]string, 0)
		lastElemsPos := make([]int, 0)
		lastIter := 0
		lastElem := "{"

		for works {
			keys := make([]string, pageSize)
			vals := make([][]byte, pageSize)

			if forward {
				numLastElems := 0

				for _, val := range lastElemsTables {
					if val != "" {
						numLastElems++
					}
				}

				if lastIter == len(lastElemsTables) && (lastElem != "{" || lastIter == 0) {
					lastElemsTables = append(lastElemsTables, make([]string, pageSize)...)
					lastElemsPos = append(lastElemsPos, make([]int, pageSize)...)

					for in := 0; in < pageSize; in++ {
						if lastIter != 0 && lastElem == "{" {
							break
						}

						// ovde se magija desava, sve okej validne elem dodati u elems
						// prolaziti i kroz memtable u pravilnom redosledu od najnovije do najstarije
						// prolazak kroz sve elemente na pozicijama, vidimo najmanji i dodajmeo i inkrementiramo (pomeramo napred), ako je previse napred -1
						// ovo je sve ako je forward
						// back bez neke cache strukture, problem?

						// pretraga za najmanjim kljucem koji nije obrisan i nije izmenjen
						// KAKO PROVERITI DA NIJE IZMENJEN? Pregledati prethodni zabelezeni? Pregledati prethodni zabelezeni u lastElems itd...
						// mozda odradjeno pomocu provere jel poslednji kljuc isti kao ovaj?
						keys[in] = "{"

						for i := memtable.R + memtable.N; i > memtable.R; i-- {
							memElems := memtable.Arr[i%memtable.N].GetSortedElems()

							if len(memElems) == 0 {
								break
							}

							if mem_indexes[i%memtable.N] == -1 {
								continue
							}

							for {
								if mem_indexes[i%memtable.N] == len(memElems) {
									mem_indexes[i%memtable.N] = -1
									break
								}

								keyHelp := memElems[mem_indexes[i%memtable.N]].Transaction.Key

								if keyHelp != lastElem && keyHelp < keys[in] && keyHelp <= key2 && !memElems[mem_indexes[i%memtable.N]].Tombstone && !(keyHelp[0:3] == "bf_" || keyHelp[0:4] == "cms_" || keyHelp[0:4] == "hll_" || keyHelp[0:3] == "sh_" || keyHelp[0:3] == "tb_") {
									keys[in] = keyHelp
									vals[in] = memElems[mem_indexes[i%memtable.N]].Transaction.Value
									lastElemsTables[lastIter+in] = ("M" + strconv.Itoa(i%memtable.N))
									lastElemsPos[lastIter+in] = mem_indexes[i%memtable.N]
									break
								} else if keyHelp > key2 {
									mem_indexes[i%memtable.N] = -1
									break
								} else if keyHelp <= keys[in] {
									mem_indexes[i%memtable.N]++
								} else if keyHelp > keys[in] {
									break
								}
							}
						}

						for index, value := range sstables {
							if indexes[index] == -1 {
								continue
							}

							for {
								keyHelp, valHelp, end := SSTable.ReadAnyData(int64(indexes[index]), value)

								if end {
									indexes[index] = -1
									break
								}

								if string(keyHelp) != lastElem && string(keyHelp) < keys[in] && string(keyHelp) <= key2 && !bytes.Equal(keyHelp, []byte{}) && !(string(keyHelp)[0:3] == "bf_" || string(keyHelp)[0:4] == "cms_" || string(keyHelp)[0:4] == "hll_" || string(keyHelp)[0:3] == "sh_" || string(keyHelp)[0:3] == "tb_") {
									keys[in] = string(keyHelp)
									vals[in] = valHelp
									lastElemsTables[lastIter+in] = ("S" + strconv.Itoa(index))
									lastElemsPos[lastIter+in] = indexes[index]
									break
								} else if string(keyHelp) > key2 {
									indexes[index] = -1
									break
								} else if string(keyHelp) <= keys[in] {
									file, err := os.OpenFile(value, os.O_RDWR, 0777)
									if err != nil {
										log.Fatal(err)
									}
									defer file.Close()
									// pomeramo se na poziciju u dataFile gde je nas podatak
									_, err = file.Seek(int64(indexes[index]), 0)
									if err != nil {
										log.Fatal(err)
									}
									// cita bajtove podatka DO key i value u info
									// CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B)
									info := make([]byte, SSTable.KEY_SIZE_START)
									_, err = file.Read(info)
									if err != nil { // pregledati
										if err == io.EOF {
											indexes[index] = -1
											break
										}

										panic(err)
									}

									tombstone := info[SSTable.TOMBSTONE_START] // jel ovo sad prepoznaje obrisane

									//ako je tombstone 1 ne citaj dalje
									if tombstone == 1 {
										indexes[index] += int(SSTable.KEY_START)
									} else {
										info2 := make([]byte, SSTable.KEY_START-SSTable.KEY_SIZE_START)
										_, err = file.Read(info2)
										if err != nil {
											if err == io.EOF {
												indexes[index] = -1
												break
											}

											panic(err)
										}

										key_size := binary.LittleEndian.Uint64(info2[:SSTable.KEY_SIZE_SIZE])
										value_size := binary.LittleEndian.Uint64(info2[SSTable.KEY_SIZE_SIZE:])

										indexes[index] += int(SSTable.KEY_START) + int(key_size) + int(value_size)
									}
								} else if string(keyHelp) > keys[in] {
									break
								}
							}
						}

						lastElem = keys[in]
					}

					lastIter += pageSize

				} else if lastIter < numLastElems {
					for i := 0; i < pageSize; i++ {
						if lastElemsTables[lastIter+i] != "" && lastElemsTables[lastIter+i][0] == 'M' {
							pos, _ := strconv.Atoi(lastElemsTables[lastIter+i][1:])
							memElems := memtable.Arr[pos].GetSortedElems()

							keys[i] = memElems[lastElemsPos[lastIter+i]].Transaction.Key
							vals[i] = memElems[lastElemsPos[lastIter+i]].Transaction.Value
						} else if lastElemsTables[lastIter+i] != "" {
							pos, _ := strconv.Atoi(lastElemsTables[lastIter+i][1:])
							keyHelp, valHelp, _ := SSTable.ReadAnyData(int64(lastElemsPos[lastIter+i]), sstables[pos])

							keys[i] = string(keyHelp)
							vals[i] = valHelp
						} else {
							keys[i] = ""
							vals[i] = []byte{0}
						}
					}

					lastIter += pageSize
				} else {
					fmt.Println("Nema podataka unapred!")
				}

			} else if lastIter > pageSize {
				lastIter -= pageSize

				for i := 0; i < pageSize; i++ {
					if lastElemsTables[lastIter-pageSize+i][0] == 'M' {
						pos, _ := strconv.Atoi(lastElemsTables[lastIter-pageSize+i][1:])
						memElems := memtable.Arr[pos].GetSortedElems()

						keys[i] = memElems[lastElemsPos[lastIter-pageSize+i]].Transaction.Key
						vals[i] = memElems[lastElemsPos[lastIter-pageSize+i]].Transaction.Value
					} else {
						pos, _ := strconv.Atoi(lastElemsTables[lastIter-pageSize+i][1:])
						keyHelp, valHelp, _ := SSTable.ReadAnyData(int64(lastElemsPos[lastIter-pageSize+i]), sstables[pos])

						keys[i] = string(keyHelp)
						vals[i] = valHelp
					}
				}

			} else {
				fmt.Println("Nema podataka nazad!")
			}

			for index, value := range keys {
				if value == "{" {
					if index == 0 {
						fmt.Println("Nema podataka!")
					}
					break
				}

				if value != "" {
					fmt.Printf("%d. %s: %s\n", index+1, value, vals[index])
				}
			}

			var option string = "0"

			for true {
				fmt.Printf("1. Napred\n2. Nazad\n3. Kraj\nOpcija: ")
				fmt.Scanf("%s", &option)

				if option == "1" {
					forward = true
				} else if option == "2" {
					forward = false
				} else if option == "3" {
					works = false
				} else {
					fmt.Println("Nepostojeca opcija!")
					continue
				}

				break
			}

		}
	}
}

func PrefixScan(memtable *Memtable.NMemtables, prefix string, pageSize int, lsm *Config.LSMTree) {
	RangeScan(memtable, prefix, prefix+string(255), pageSize, lsm)
}

func RangeIter(memtable *Memtable.NMemtables, key1 string, key2 string, lsm *Config.LSMTree) {
	RangeScan(memtable, key1, key2, 1, lsm)
}

func PrefixIter(memtable *Memtable.NMemtables, prefix string, lsm *Config.LSMTree) {
	PrefixScan(memtable, prefix, 1, lsm)
}

func Put(WAL *WriteAheadLog.WAL, memtable *Memtable.NMemtables, cache *Cache.LRUCache, key string, value []byte, tb *TokenBucket.TokenBucket) bool {

	ok := tb.ConsumeToken()
	if !ok {
		fmt.Print("\nGreska! Previse obavljenih requestova u odredjenom vremenskom rasponu!\n")
		return false
	}

	//prvo staviti podatak WAL
	//potom u memtable
	//dodati u kes?
	//provera da li je memtable popunjen?
	//nakon toga ako je memtable popunjen, sortirati memtable po kljucu
	// zatim zapisati na disk formirajuci sstable
	// isprazniti memtable ili napraviti novi
	// ---> ovo ne treba? izbrisan je za sada transaction := Config.NewTransaction(key, value)

	succesful := WriteAheadLog.Put(WAL, memtable, key, value)
	// if successful{

	// 	cache.Insert(key, value)

	// }else{
	// 	fmt.Printf("Neuspesan unos.")
	// }
	return succesful
}

func Delete(WAL *WriteAheadLog.WAL, memtable *Memtable.NMemtables, cache *Cache.LRUCache, key string, tb *TokenBucket.TokenBucket, lsm *Config.LSMTree) ([]byte, bool) {

	ok := tb.ConsumeToken()
	if !ok {
		fmt.Print("\nGreska! Previse obavljenih requestova u odredjenom vremenskom rasponu!\n")
		return nil, false
	}

	// nasli smo ga u memtable
	data, found, _ := memtable.Get(key)
	if found {
		fmt.Println("Pronađeno u memtable.")
		WriteAheadLog.Delete(WAL, memtable, key) // da li ovo samo ako radi sa memtable? ima mi smisla ali eto nek bude note
		return data, true
	}

	// ako je u cache, onda je i na disku, brisemo u cache ako postoji
	value := cache.Get(key)
	if value != nil {
		fmt.Println("Pronađeno u cache.")
		cache.Delete(key)
		valueByte := value.([]byte)
		memtable.AddAndDelete(key, valueByte)
		return valueByte, true
	}

	// provjeravamo disk
	foundBF, fileBF := SearchTroughBloomFilters(key, lsm) // trazi u disku
	if foundBF {
		fmt.Println("Mozda postoji na disku.")
		//ucitavamo summary i index fajlove za sstable u kojem je mozda element (saznali preko bloomfiltera)
		summaryFileName := lsm.SummaryFilesNames[fileBF]
		indexFileName := lsm.IndexFilesNames[fileBF]
		foundValue := SSTable.Get(key, summaryFileName, indexFileName, lsm.DataFilesNames[fileBF])

		if reflect.DeepEqual(foundValue, []byte{}) {
			return nil, false
		}

		memtable.AddAndDelete(key, foundValue)
		// *** da li treba dodati pa obrisati u memtable, ako je tombstone pronadjenog u sstable = true?
		// kako provjeriti tombstone iz sstable Get()?

		return foundValue, true
	}

	return nil, false
}

func CreateBF(expectedElements int, falsePositiveRate float64) ([]byte, bool) {
	bf := BloomFilter.BloomFilter{}
	bf.Init(expectedElements, falsePositiveRate)

	return EncodeBF(&bf)
}

func EncodeBF(bf *BloomFilter.BloomFilter) ([]byte, bool) {
	bytes, err := bf.ToBytes()

	if err != nil {
		return nil, true
	}

	return bytes, false
}

func DecodeBF(bytes []byte) (*BloomFilter.BloomFilter, bool) {
	bf := BloomFilter.BloomFilter{}
	err := bf.FromBytes(bytes)

	if err != nil {
		fmt.Println(err)
		return nil, true
	}

	return &bf, false
}

func CreateHLL(precision uint8) ([]byte, bool) {
	hll := HyperLogLog.HLL{}
	hll.Init(precision)
	return EncodeHLL(&hll)
}

func EncodeHLL(hll *HyperLogLog.HLL) ([]byte, bool) {
	bytes, err := hll.ToBytes()
	if err != nil {
		return nil, true
	}

	return bytes, false
}

func DecodeHLL(bytes []byte) (*HyperLogLog.HLL, bool) {
	hll := HyperLogLog.HLL{}
	err := hll.FromBytes(bytes)

	if err != nil {
		fmt.Println(err)
		return nil, true
	}

	return &hll, false
}

func CreateCMS(width float64, depth float64) ([]byte, bool) {
	cms := CountMinSketch.CMS{}
	cms.NewCMS(depth, width)
	return EncodeCMS(&cms)
}

func EncodeCMS(cms *CountMinSketch.CMS) ([]byte, bool) {
	bytes, err := cms.ToBytes()

	if err != nil {
		return nil, true
	}

	return bytes, false
}

func DecodeCMS(bytes []byte) (*CountMinSketch.CMS, bool) {
	cms := CountMinSketch.CMS{}
	err := cms.FromBytes(bytes)

	if err != nil {
		fmt.Println(err)
		return nil, true
	}

	return &cms, false
}
