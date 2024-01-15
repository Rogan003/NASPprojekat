package main

import (
	"NASPprojekat/BloomFilter"
	"NASPprojekat/Cache"
	"NASPprojekat/Memtable"
	"NASPprojekat/SSTable"

	"fmt"
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
