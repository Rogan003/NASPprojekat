package SSTable

import (
	"NASPprojekat/BloomFilter"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

// potrebna funkcija za koverziju cvora u bajtove, za upis u fajl podataka
// func NodeToBytes(node *SkipListNode) []byte {}

type SStableSummary struct {
	FirstKey string //prvi kljuc
	LastKey  string //poslednji kljuc
}

type SSTableIndex struct {
	mapIndex map[string]int64
}

// konstruktori
func NewSummary(nodes []*SkipNode) *SStableSummary {
	first := nodes[0].Key()
	last := nodes[len(nodes)-1].Key()
	return &SStableSummary{
		FirstKey: first,
		LastKey:  last,
	}
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
func Get(key []byte) {
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

// preko kljuca trazimo u summaryFile poziciju za nas klju u indexFile iz kog kasnije dobijamo poziicju naseg kljuca i vrednosti u dataFile
func get(key string, SummaryFileName string, IndexFileName string, DataFileName string) string {

	//iz summary citamo opseg kljuceva u sstable (prvi i poslendji)
	sumarryFile, _ := os.OpenFile(SummaryFileName, os.O_RDWR, 0777)
	summary := loadSummary(sumarryFile)
	defer sumarryFile.Close()

	// ako je trazeni kljuc u tom opsegu, podatak bi trebalo da se nalazi u ovom sstable
	if summary.FirstKey <= key && key <= summary.LastKey {

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

			if string(keyValue) > key {
				dataPosition := findInIndex(indexPosition, key, IndexFileName)
				notFound := -1
				if dataPosition == uint64(notFound) {
					panic("sstable: Nije pronadjen key u indexFile")
				}
				return readData(int64(dataPosition), DataFileName)
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
					return ""
				}
				continue
			}
		}
	}
	return ""
}

// vraca offset za dataFile, nakon sto nadje u indexFile
func findInIndex(startPosition uint64, key string, IndexFileName string) uint64 {

	file, err := os.OpenFile(IndexFileName, os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	// od date pozicije citamo
	_, err = file.Seek(int64(startPosition), 0)
	if err != nil {
		log.Fatal(err)
	}

	for {
		currentKey, position := readFromIndex(file)
		if currentKey > key {
			notFound := -1
			return uint64(notFound)
		}
		if currentKey == key {
			return uint64(position)
		}
	}
}

func FileLength(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// cita jedan key sa svojom velicinom i positionom iz indexFile
func readFromIndex(file *os.File) (string, int64) {
	keyLenBytes := make([]byte, KEY_SIZE_SIZE)
	_, err := file.Read(keyLenBytes)
	if err != nil {
		panic(err)
	}
	keySize := int64(binary.LittleEndian.Uint64(keyLenBytes))
	keyBytes := make([]byte, keySize)
	_, err = file.Read(keyBytes)
	if err != nil {
		panic(err)
	}
	key := string(keyBytes)
	positonBytes := make([]byte, KEY_SIZE_SIZE)
	_, err = file.Read(positonBytes)
	if err != nil {
		panic(err)
	}
	position := int64(binary.LittleEndian.Uint64(positonBytes))
	return key, position
}

// u Index fajlu: velicina kljuca + kljuc u bajtovima + pozicija (binarna) tog podatka u DataFile
// u fajlu sa indexima (IndexFileName) se cuva kljuc cvora iz skip liste i pozicija bajta tog cvora (podatka) sa tim kljucem u fajlu sa podacima (DataFileName)
func AddToIndex(offset int64, key string, indexFile *os.File) int64 {
	data := []byte{}
	keyBytes := []byte(key)
	keySizeBytes := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(keySizeBytes, uint64(len(keyBytes)))
	// position je uint64 a najvise zauzima 8 bajtova (KEY_SIZE_SIZE)
	offsetBytes := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(offsetBytes, uint64(offset))
	data = append(data, keySizeBytes...)
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

// u fajlu summary (SummaryFileName) cuva kljuc cvora iz skip liste i poziciju bajta tog cvora (podatka) sa tim kljucem u indexFile
func AddToSummary(position int64, key string, summary *os.File) {
	data := []byte{}
	//vrednost kljuca u bajtovima
	keyValBytes := []byte(key)
	//bajtovi u kojima se cuva duzina kljuca
	keyBytes := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(keyBytes, uint64(len(keyValBytes)))

	//pozicija u indexFile gde se cuva ovaj node
	positionBytes := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(positionBytes, uint64(position))

	data = append(data, keyBytes...)
	data = append(data, keyValBytes...)
	data = append(data, positionBytes...)
	_, err := summary.Write(data)
	if err != nil {
		return
	}
}

// ucitavanje iz summary-ja u SSTableSummary
// prvi bajtovi summary fajla:
// sizeb(vel1) | sizeb(vel2) | vel1(k1) | vel2(k2) - podaci o najmanjem i najvecem kljucu
// ostali bajtovi:
// sizeb(velk) | velk(k) | sizeb(pozicija u index) - za jedan podatak
func loadSummary(summary *os.File) *SStableSummary {
	lenFirst := make([]byte, KEY_SIZE_SIZE) //size upitan
	lenLast := make([]byte, KEY_SIZE_SIZE)

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

func readData(position int64, DataFileName string) string {
	file, err := os.OpenFile(DataFileName, os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	// pomeramo se na poziciju u dataFile gde je nas podatak
	_, err = file.Seek(position, 0)
	if err != nil {
		log.Fatal(err)
	}
	// cita bajtove podatka DO key i value u info
	// CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B)
	info := make([]byte, KEY_START)
	_, err = file.Read(info)
	if err != nil {
		panic(err)
	}

	key_size := binary.LittleEndian.Uint64(info[KEY_SIZE_START:VALUE_SIZE_START])
	value_size := binary.LittleEndian.Uint64(info[VALUE_SIZE_START:KEY_START])

	// cita bajtove podatka, odnosno key i value u data
	//| Key | Value |
	data := make([]byte, key_size+value_size)
	_, err = file.Read(data)
	if err != nil {
		panic(err)
	}
	val := string(data[key_size : key_size+value_size])
	return val
}

func MakeData(nodes []*SkipNode, DataFileName string, IndexFileName string, SummaryFileName string, BloomFileName string) {
	indexFile, err := os.OpenFile(IndexFileName, os.O_RDWR|os.O_APPEND, 0777)
	if err != nil {
		panic(err)
	}
	defer indexFile.Close()

	summaryFile, err := os.OpenFile(SummaryFileName, os.O_RDWR|os.O_APPEND, 0777)
	if err != nil {
		panic(err)
	}
	defer summaryFile.Close()
	// uzima najmanji i najveci kljuc iz nodes iz skiplist
	first := make([]byte, KEY_SIZE_SIZE)
	last := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(first, uint64(len([]byte(nodes[0].Key()))))
	binary.LittleEndian.PutUint64(last, uint64(len([]byte(nodes[len(nodes)-1].Key()))))
	// upisuje ih u summary
	summaryFile.Write(first)
	summaryFile.Write(last)
	summaryFile.Write([]byte(nodes[0].Key()))
	summaryFile.Write([]byte(nodes[len(nodes)-1].Key()))

	// pravi se bloom filter
	make_filter(nodes, len(nodes), BloomFileName)

	file, err := os.OpenFile(DataFileName, os.O_RDWR|os.O_APPEND, 0777)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var n = 0 // krece se od 0 da bi prvi kljuc u summaryFile bio prvi i u indexFile
	for _, node := range nodes {
		position, _ := FileLength(file)
		// cvor se upisuje u DataFile
		_, err = file.Write(NodeToBytes(node))
		if err != nil {
			return
		}
		// upisivanje u index fajl
		positionSum := AddToIndex(position, node.Key(), indexFile)
		// upisuje svaki peti u summary file
		if n%5 == 0 {
			AddToSummary(positionSum, node.Key(), summaryFile)
		}
		n += 1
	}
	err = file.Sync()
	if err != nil {
		return
	}
}
