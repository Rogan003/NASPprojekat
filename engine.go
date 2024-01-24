package main

import (
	"NASPprojekat/BloomFilter"
	"NASPprojekat/Cache"
	"NASPprojekat/Memtable"
	"NASPprojekat/SSTable"
	"NASPprojekat/WriteAheadLog"

	"fmt"
	"time"
)

func Get(memtable *Memtable.Memtable, cache *Cache.LRUCache, key string) ([]byte, bool) {
	data, found := memtable.GetElement(key)
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

	foundBF, fileBF := SearchTroughBloomFilters(key) // trazi u disku
	if foundBF {
		fmt.Println("Mozda postoji na disku.")
		//ucitavamo summary i index fajlove za sstable u kojem je mozda element (saznali preko bloomfiltera)
		summaryFileName := fileBF[0:14] + "summaryFile" + fileBF[22:]
		indexFileName := fileBF[0:14] + "indexFile" + fileBF[22:]
		foundValue := SSTable.Get(key, summaryFileName, indexFileName, fileBF)
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
func SearchTroughBloomFilters(key string) (bool, string) {
	bf := BloomFilter.BloomFilter{}
	for i := 0; i < len(bloomFilterFilesNames); i++ {
		err := bf.Deserialize(bloomFiltersFilesNames[i])
		if err != nil {
			return false, ""
		}
		found := bf.Check_elem(key)
		if found {
			return found, bloomFiltersFilesNames[i]
		}

	}
	return false, ""
}

func RangeScan(memtable *Memtable.Memtable, key1 string, key2 string, pageSize int) {
	// sta se desava?
	// otvaramo sve sstable i ovu listu elem iz memtabele
	// kreiramo neku listu pokazivaca za sstable (inicijalno -1, komada koliko ima sstable-a)
	// pozicioniramo se u sstable pomocu index-a i summary-a, tako sto idemo do prvog kljuca elem koji pripada range
	// zatim najmanji dodajemo u listu za ispis i inkrementiramo pokazivac, ako je kljuc elem na tom pokazivacu veci od key2 (upper range), zatvaramo taj fajl, pokazivac na -1 itd
	// ponavljamo ovako nekakav proces i kad dodjemo u situaciju da smo ispunili pageSize elem, ispisemo ih?
	// ako korisnik kaze da nastavlja jos onda mu ispisemo jos, ako ne, onda je je to kraj ove fje
	// smisliti efikasan nacin za odrzavanje nekih stranica u cache (da li treba uopste?)

	if key1 > key2 {
		fmt.Println("Greska! Opseg nije moguc!")
	} else {
		memElems := memtable.GetSortedElems()
		memIter := -1
	
		for index, value := range memElems {
			if value.Transaction.Key > key1 {
				memIter = index
				break
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
			summary := loadSummary(sumarryFile)
			defer sumarryFile.Close()
	
			// ako je trazeni kljuc u tom opsegu, podatak bi trebalo da se nalazi u ovom sstable
			if summary.FirstKey <= key2 && summary.LastKey >= key1 {
	
				var indexPosition = uint64(0)
				for {
					//citamo velicinu kljuca
					keySizeBytes := make([]byte, KEY_SIZE_SIZE)
					_, err := sumarryFile.Read(keySizeBytes)
					keySize := int64(binary.LittleEndian.Uint64(keySizeBytes))
	
					//citamo keySize bajtiva da bi dobili kljuc
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
							currentKey, position := readFromIndex(file)
							if currentKey >= key1 {
								indexes[index] = uint64(position)
								break
							}
						}

						break
					} else {
						// citanje pozicije za taj kljuc u indexFile
						positionBytes := make([]byte, KEY_SIZE_SIZE)
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

		for works {
			elems := [pageSize]WAL.Entry{}

			for i := 0; i < 10; i++ {
				// ovde se magija desava, sve okej validne elem dodati
				// back bez neke cache strukture, problem?
			}

			for index, value := range elems {
				fmt.Printf("%i. %s: %s\n", index + 1, value.Transaction.Key, value.Transaction.Value) // kako ispisati niz bajtova? kao string?
			}

			var option int = -1

			for option < 1 || option > 3{
				fmt.Printf("1. Napred\n2. Nazad\n3. Kraj\nOpcija: ")
				fmt.Scanf("%v",&option) // ako se unese karakter greska? nesto oko ovoga?

				if option == 1 {
					forward = true
				} else if option == 2 {
					forward = false
				} else if option == 3{
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

func PrefixScan(memtable *Memtable.Memtable, prefix string, pageSize int) {
	RangeScan(memtable, prefix, prefix + string('z' + 1), pageSize)
}

func RangeIter(memtable *Memtable.Memtable, key1 string, key2 string) {
	RangeScan(memtable, key1, key2, 1)
}

func PrefixIter(memtable *Memtable.Memtable, prefix string) {
	PrefixScan(memtable, prefix, 1)
}

func Put(WAL *WriteAheadLog.WAL, memtable *Memtable.NMemtables, key string, value []byte) bool{
	//prvo staviti podatak WAL
	//potom u memtable
	//dodati u kes?
	//provera da li je memtable popunjen?
	//nakon toga ako je memtable popunjen, sortirati memtable po kljucu
	// zatim zapisati na disk formirajuci sstable
	// isprazniti memtable ili napraviti novi
	transaction:= WAL.NewTransaction(key,value)
	
	succesful = WriteAheadLog.Put(WAL, memtable, key, value)
	// if successful{

	// 	cache.Insert(key, value)

	// }else{
	// 	fmt.Printf("Neuspesan unos.")
	// }
	return succesful
}