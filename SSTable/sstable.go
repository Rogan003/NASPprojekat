package SSTable

import (
	"NASPprojekat/BloomFilter"
	"encoding/binary"
	"log"
	"os"
)

type SStableSummary struct {
	FirstKey string //prvi kljuc
	LastKey  string //poslednji kljuc
}

type SSTableIndex struct {
	mapIndex map[string]int64
}

func NewIndex() *SSTableIndex {
	return &SSTableIndex{
		mapIndex: map[string]int64{},
	}
}

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

func FileLength(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// u Index fajlu kljuc u bajtovima + pozicija (binarna) tog podatka u DataFile
// u fajlu sa indexima (IndexFileName) se cuva kljuc cvora iz skip liste i pozicija bajta tog cvora (podatka) sa tim kljucem u fajlu sa podacima (DataFileName)
func AddToIndex(offset int64, key string, indexFile *os.File) int64 {
	data := []byte{}
	keyBytes := []byte(key)
	offsetBytes := make([]byte, size)
	binary.LittleEndian.PutUint64(offsetBytes, uint64(offset))
	data = append(data, keyBytes...)
	data = append(data, offsetBytes...)
	//pokazuje na kojoj poziciji u fajlu IndexFile se nalaze podaci (kljuc + poz u DataFile) o cvoru
	indexLength, _ := FileLength(indexFile)
	_, err := indexFile.Write(data)
	if err != nil {
		return 0
	}
	return indexLength
}

// u fajlu summary (SummaryFileName) cuva kljuc cvora iz skip liste i poziciju bajta tog cvora (podatka) sa tim kljucem u fajlu sa podacima (DataFileName)
func AddToSummary(position int64, key string, summary *os.File) {
	data := []byte{}
	//vrednost kljuca u bajtovima
	keyb := []byte(key)
	//bajtovi u kojima se cuva vrednost kljuca
	keybs := make([]byte, size) //size upitan
	binary.LittleEndian.PutUint64(keybs, uint64(len(keyb)))

	//pozicija u indexFile gde se cuva ovaj node
	positionb := make([]byte, size)
	binary.LittleEndian.PutUint64(positionb, uint64(position))

	data = append(data, keybs...)
	data = append(data, keyb...)
	data = append(data, positionb...)
	_, err := summary.Write(data)
	if err != nil {
		return
	}
}

// potrebna funkcija za koverziju cvora u bajtove, za upis u fajl podataka
//func NodeToBytes(node *SkipListNode) []byte {}

// ucitavanje iz summary-ja u SSTableSummary
// prvi bajtovi summary fajla:
// sizeb(vel1) | sizeb(vel2) | vel1(k1) | vel2(k2) - podaci o najmanjem i najvecem kljucu
// ostali bajtovi:
// sizeb(velk) | velk(k) | sizeb(pozicija u index) - za jedan podatak
func loadSummary(summary *os.File) *SStableSummary {
	lenFirst := make([]byte, size) //size upitan
	lenLast := make([]byte, size)

	_, _ = summary.Read(lenFirst)
	sizeFirst := int64(binary.LittleEndian.Uint64(lenFirst))
	_, _ = summary.Read(lenLast)
	sizeLast := int64(binary.LittleEndian.Uint64(lenLast))
	first := make([]byte, sizeFirst)
	last := make([]byte, sizeLast)

	_, _ = summary.Read(first)
	_, _ = summary.Read(last)

	return &SStableSummary{
		FirstKey: string(first),
		LastKey:  string(last),
	}
}

// dobija poziciju sa koje treba da procita gde se u DataFile nalazi podatak
func readOffsetFromIndex(position int64, IndexFileName string) int64 {
	file, err := os.OpenFile(IndexFileName, os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	// od date pozicije citamo
	_, err = file.Seek(position, 0)
	if err != nil {
		log.Fatal(err)
	}
	//citamo SIZE bajta koji nam govore poziciju podatka u dataFile
	positionBytes := make([]byte, size)
	_, err = file.Read(positionBytes)
	if err != nil {
		panic(err)
	}
	return int64(binary.LittleEndian.Uint64(positionBytes))
}
