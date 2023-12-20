package SSTable

import (
	"NASPprojekat/BloomFilter"
)



// funkcija za kreaciju bloom filtera za sstable
// elems jer niz kljuceva (lako cu modifikovati ako saljete podakte), numElems jer broj podataka u memTable,
// filePath je gde i pod kojim nazivom cuvamo bloom filter
func make_filter(elems [][]byte, numElems int, filePath string) {
	bf := BloomFilter.BloomFilter{}
	bf.Init(numElems, 0.01)
	
	for _, val := range elems {
		bf.Add(val)
	}

	bf.Serialize(filePath)
}

// funkcija get za sstable
func get(key []byte) {
	where := -1

	for true { // ovde zapravo treba da se ucini da se prodje kroz sve bloom filtere sstable-a 
		filePath := "ovo treba da se izvlaci iz petlje?"
		bf := BloomFilter.BloomFilter{}

		bf.Deserialize(filePath)
		if bf.Check_elem(key) {
			where = 0 // ovde zapravo treba da ide indeks u kojoj sstable je element
			break
		}
	}

	if where != -1 {
		// ovo znaci da se element nalazi u bloom filteru sa indeksom where, tj u where sstable
		// dalja pretraga u where summary, pa u index, pa u data deo
	}
}