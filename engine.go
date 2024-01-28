package main

import (
	"NASPprojekat/BloomFilter"
	"NASPprojekat/Cache"
	"NASPprojekat/Config"
	"NASPprojekat/CountMinSketch"
	"NASPprojekat/Memtable"
	"NASPprojekat/SSTable"
	"NASPprojekat/TokenBucket"
	"NASPprojekat/WriteAheadLog"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"bytes"
)

func Get(memtable *Memtable.NMemtables, cache *Cache.LRUCache, key string, tb *TokenBucket.TokenBucket, lsm *Config.LSMTree) ([]byte, bool) {

	ok := tb.ConsumeToken()
	if !ok {
		fmt.Println("\nGreska! Previse obavljenih requestova u odredjenom vremenskom rasponu!\n")
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
		summaryFileName := fileBF[0:14] + "summaryFile" + fileBF[22:]
		indexFileName := fileBF[0:14] + "indexFile" + fileBF[22:]
		foundValue := SSTable.Get(key, summaryFileName, indexFileName, fileBF)

		if foundValue == nil { // slucaj kada je bf dao false positive
			return nil, false
		}

		cache.Insert(key, foundValue) // dodavanje u cache
		return foundValue, true
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
func SearchTroughBloomFilters(key string, lsm *Config.LSMTree) (bool, string) {
	bf := BloomFilter.BloomFilter{}
	// dodaj putanje u bloomFilterFilesNames...
	for i := 0; i < len(lsm.BloomFilterFilesNames); i++ {
		err := bf.Deserialize(lsm.BloomFilterFilesNames[i])
		if err != nil {
			return false, ""
		}
		found := bf.Check_elem([]byte(key))
		if found {
			return found, lsm.BloomFilterFilesNames[i]
		}

	}
	return false, ""
}

func RangeScan(memtable *Memtable.NMemtables, key1 string, key2 string, pageSize int) {
	// sta se desava?
	// otvaramo sve sstable i ovu listu elem iz memtabele
	// kreiramo neku listu pokazivaca za sstable (inicijalno -1, komada koliko ima sstable-a)
	// pozicioniramo se u sstable pomocu index-a i summary-a, tako sto idemo do prvog kljuca elem koji pripada range
	// zatim najmanji dodajemo u listu za ispis i inkrementiramo pokazivac, ako je kljuc elem na tom pokazivacu veci od key2 (upper range), zatvaramo taj fajl, pokazivac na -1 itd
	// ponavljamo ovako nekakav proces i kad dodjemo u situaciju da smo ispunili pageSize elem, ispisemo ih?
	// ako korisnik kaze da nastavlja jos onda mu ispisemo jos, ako ne, onda je je to kraj ove fje
	// smisliti efikasan nacin za odrzavanje nekih stranica u cache (da li treba uopste?)

	// DODATI PRESKAKANJE ZA STRUKTURE KAD IH ZAVRSIMO

	if key1 > key2 {
		fmt.Println("Greska! Opseg nije moguc!")
	} else {
		mem_indexes := make([]int, memtable.N)
		
		for index, value := range memtable.Arr {
			memElems := value.GetSortedElems()
			mem_indexes[index] = -1

			for index, value := range memElems {
				if value.Transaction.Key >= key1 && value.Transaction.Key <= key2 {
					mem_indexes[index] = index
					break
				}
			}
		}

		sstables := [10]string{} // za sada samo placeholder naziva fajlova (smisliti kako cemo ih ucitati inace)
		indexes := [len(sstables)]int{}

		// postavljamo sve na -1 kako bi znali ako neki sstable nema range koji nama treba
		for index, _ := range indexes {
			indexes[index] = -1
		}

		// citanje pozicija u index fajlovima odakle treba da se krece skeniranje
		for index, value := range sstables {
			sumarryFileName := value[0:14] + "summaryFile" + value[22:]
			indexFileName := value[0:14] + "indexFile" + value[22:]

			//iz summary citamo opseg kljuceva u sstable (prvi i poslendji)
			sumarryFile, _ := os.OpenFile(sumarryFileName, os.O_RDWR, 0777)
			summary := SSTable.LoadSummary(sumarryFile)
			defer sumarryFile.Close()

			// ako je trazeni kljuc u tom opsegu, podatak bi trebalo da se nalazi u ovom sstable
			if summary.FirstKey <= key2 && summary.LastKey >= key1 {

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
								sumarryFile.Close()
								break
							}
							fmt.Println(err)
							sumarryFile.Close()
							break
						}
					}
				}
			}
		}

		// pretraga elemenata u range (prvo verzija bez cuvanja elem, pa onda sa cuvanjem)

		forward := true
		works := true
		lastElemsTables := make([]string, pageSize)
		lastElemsPos := make([]int, pageSize)
		lastIter := 0

		for works {
			keys := make([]string, pageSize)
			vals := make([][]byte, pageSize)

			if forward {
				lastIter += pageSize

				for i := 0; i < pageSize; i++ {
					// ovde se magija desava, sve okej validne elem dodati u elems
					// prolaziti i kroz memtable u pravilnom redosledu od najnovije do najstarije
					// prolazak kroz sve elemente na pozicijama, vidimo najmanji i dodajmeo i inkrementiramo (pomeramo napred), ako je previse napred -1
					// ovo je sve ako je forward
					// back bez neke cache strukture, problem?
					
					// pretraga za najmanjim kljucem koji nije obrisan i nije izmenjen
					// KAKO PROVERITI DA NIJE IZMENJEN? Pregledati prethodni zabelezeni? Pregledati prethodni zabelezeni u lastElems itd...
					keys[i] = "{"

					for i := memtable.R + memtable.N; i > memtable.R; i-- {
						memElems := value.GetSortedElems()
						
						if len(memElems) == 0 {
							break
						}
						
						if mem_indexes[i % memtable.N] == -1 {
							continue
						}

						for {
							keyHelp := memElems[mem_indexes[i % memtable.N]].Transaction.Key

							if keyHelp < keys[i] && keyHelp <= key2 && !memElems[mem_indexes[i % memtable.N]].Tombstone {
								keys[i] = keyHelp
								vals[i] = memElems[mem_indexes[i % memtable.N]].Transaction.Value
								break
							} else if keyHelp > key2 {
								mem_indexes[i % memtable.N] = -1
								break
							} else if keyHelp < keys[i] {
								mem_indexes[i % memtable.N]++;
							}
						}
					}

					for index, value := range sstables {
						if indexes[index] == -1 {
							continue
						}

						indexFileName := value[0:14] + "indexFile" + value[22:]

						for {
							keyHelp, valHelp := SSTable.ReadData(indexes[index], value) // u get se poziva nad ovim value?
							// sta ako dodjemo do kraja fajla?
							if keyHelp < keys[i] && keyHelp <= key2 && !bytes.Equal(keyHelp, []byte{}) {
								keys[i] = keyHelp
								vals[i] = valHelp
								break
							} else if keyHelp > key2 {
								indexes[index] = -1
								break
							} else if keyHelp < keys[i] {
								file, err := os.OpenFile(value, os.O_RDWR, 0777)
								if err != nil {
									log.Fatal(err)
								}
								defer file.Close()
								// pomeramo se na poziciju u dataFile gde je nas podatak
								_, err = file.Seek(indexes[index], 0)
								if err != nil {
									log.Fatal(err)
								}
								// cita bajtove podatka DO key i value u info
								// CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B)
								info := make([]byte, SSTable.KEY_SIZE_START)
								_, err = file.Read(info)
								if err != nil {
									panic(err)
								}
								
								info2 := make([]byte, SSTable.KEY_START-SSTable.KEY_SIZE_START)
								_, err = file.Read(info2)
								if err != nil {
									panic(err)
								}
							
								key_size := binary.LittleEndian.Uint64(info2[:SSTable.KEY_SIZE_SIZE])
								value_size := binary.LittleEndian.Uint64(info2[SSTable.KEY_SIZE_SIZE:])

								indexes[index] += SSTable.KEY_START + key_size + value_size
							}
						}
					}
				}

			} else if lastIter > 0 {
				lastIter -= pageSize

				for i := 0; i < pageSize; i++ {
					// ucitavanje sa lastElemsPos[lastIter + i]
				}

			} else {
				fmt.Println("Nema podataka nazad!")
			}

			for index, value := range keys {
				fmt.Printf("%i. %s: %s\n", index + 1, value, vals[index]) // kako ispisati niz bajtova? kao string?
			}

			var option char = '0'

			for option < 1 || option > 3 {
				fmt.Printf("1. Napred\n2. Nazad\n3. Kraj\nOpcija: ")
				fmt.Scanf("%c", &option) // ako se unese karakter greska? nesto oko ovoga?

				if option == '1' {
					forward = true
				} else if option == '2' {
					forward = false
				} else if option == '3' {
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

func PrefixScan(memtable *Memtable.NMemtables, prefix string, pageSize int) {
	RangeScan(memtable, prefix, prefix+string('z'+1), pageSize)
}

func RangeIter(memtable *Memtable.NMemtables, key1 string, key2 string) {
	RangeScan(memtable, key1, key2, 1)
}

func PrefixIter(memtable *Memtable.NMemtables, prefix string) {
	PrefixScan(memtable, prefix, 1)
}

func Put(WAL *WriteAheadLog.WAL, memtable *Memtable.NMemtables, cache *Cache.LRUCache, key string, value []byte, tb *TokenBucket.TokenBucket) bool {

	ok := tb.ConsumeToken()
	if !ok {
		fmt.Println("\nGreska! Previse obavljenih requestova u odredjenom vremenskom rasponu!\n")
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
		fmt.Println("\nGreska! Previse obavljenih requestova u odredjenom vremenskom rasponu!\n")
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
		summaryFileName := fileBF[0:14] + "summaryFile" + fileBF[22:]
		indexFileName := fileBF[0:14] + "indexFile" + fileBF[22:]
		foundValue := SSTable.Get(key, summaryFileName, indexFileName, fileBF)

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

/*
func MenuCMS() {
	fmt.Println("1)	Kreiranje nove instance")
	fmt.Println("2) Brisanje postojeće instance")
	fmt.Println("3) Dodavanje novog događaja u neku instancu")
	fmt.Println("4) Provera učestalosti događaja u nekoj instanci")

	for {
		fmt.Print("Izaberite radnju: ")
		var choice string
		fmt.Scanln(&choice)

		switch choice {
		case "1":
			fmt.Println("Kreiranje nove instance CMS-a")
			fmt.Println("--------------")
			fmt.Println("Unesite parametar sirine: ")
			var choice string
			fmt.Scanln(&choice)
			cms = CountMinSketch.NewCMS(int)

		case "2":
			fmt.Println("Brisanje postojeće instance")
			fmt.Println("--------------")

		case "3":
			fmt.Println("Dodavanje novog događaja u neku instancu.")
			fmt.Println("--------------")
		case "4":
			fmt.Println("Provera učestalosti događaja u nekoj instanci")
			fmt.Println("--------------")

		default:
			fmt.Println("Unesite validnu radnju.")
		}
	}
}

// Assuming t is a time.Timer
//var t = time.NewTimer(60 * time.Second)

// Define Wal, memtable, lru, Get, and Mem.WriteFileNames() accordingly


*/
