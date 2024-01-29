package SSTable

import (
	"NASPprojekat/BloomFilter"
	"NASPprojekat/Config"
	"hash/crc32"
	//"io/ioutil"
	"strconv"
	"strings"

	"encoding/binary"
	//"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"
	"math"
	"sort"
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
// func NodeToBytes(node iz memetable koji je pretvoren u Entry) []byte {}

type SStableSummary struct {
	FirstKey string //prvi kljuc
	LastKey  string //poslednji kljuc
}

type SSTableIndex struct {
	mapIndex map[string]int64
}

// konstruktori
func NewSummary(nodes []*Config.Entry, counter int) *SStableSummary {
	first := nodes[0].Transaction.Key
	last := nodes[len(nodes)-1].Transaction.Key
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
func make_filter(elems []*Config.Entry, numElems int, filePath string) {
	bf := BloomFilter.BloomFilter{}
	bf.Init(numElems, 0.01)

	for _, val := range elems {
		bf.Add(val.ToByte()) // mozda key?
	}

	bf.Serialize(filePath)
}

// preko kljuca trazimo u summaryFile poziciju za nas klju u indexFile iz kog kasnije dobijamo poziicju naseg kljuca i vrednosti u dataFile
func Get(key string, SummaryFileName string, IndexFileName string, DataFileName string) []byte {

	//iz summary citamo opseg kljuceva u sstable (prvi i poslendji)
	sumarryFile, _ := os.OpenFile(SummaryFileName, os.O_RDWR, 0777)
	summary := LoadSummary(sumarryFile)
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
				_, data := ReadData(int64(dataPosition), DataFileName)
				return data
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
					return []byte{}
				}
				continue
			}
		}
	}
	return []byte{}
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
		currentKey, position := ReadFromIndex(file)
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
func ReadFromIndex(file *os.File) (string, int64) {
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
// u fajlu sa indexima (IndexFileName) se cuva kljuc cvora iz memtable i pozicija bajta tog cvora (podatka) sa tim kljucem u fajlu sa podacima (DataFileName)
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

// u fajlu summary (SummaryFileName) cuva kljuc cvora iz  memtable i poziciju bajta tog cvora (podatka) sa tim kljucem u indexFile
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
func LoadSummary(summary *os.File) *SStableSummary {
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

func ReadData(position int64, DataFileName string) ([]byte, []byte) {
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
	info := make([]byte, KEY_SIZE_START)
	_, err = file.Read(info)
	if err != nil {
		panic(err)
	}

	tombstone := binary.LittleEndian.Uint64(info[TOMBSTONE_START:KEY_SIZE_START])
	//ako je tombstone 1 ne citaj dalje
	if tombstone == 1 {
		return []byte{}, []byte{}
	}
	//ako je tombstone 0 onda citaj sve
	info2 := make([]byte, KEY_START-KEY_SIZE_START)
	_, err = file.Read(info2)
	if err != nil {
		panic(err)
	}

	key_size := binary.LittleEndian.Uint64(info2[:KEY_SIZE_SIZE])
	value_size := binary.LittleEndian.Uint64(info2[KEY_SIZE_SIZE:])

	// cita bajtove podatka, odnosno key i value u data
	//| Key | Value |
	data := make([]byte, key_size+value_size)
	_, err = file.Read(data)
	if err != nil {
		panic(err)
	}
	key := data[:key_size]
	val := data[key_size : key_size+value_size]
	return key, val
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// funkcija koja pretvara node tj entry u bajtove
func NodeToBytes(node Config.Entry) []byte { //pretvara node u bajtove
	var data []byte

	crcb := make([]byte, CRC_SIZE)
	binary.LittleEndian.PutUint32(crcb, CRC32(node.Transaction.Value))
	data = append(data, crcb...) //dodaje se CRC

	sec := time.Now().Unix()
	secb := make([]byte, TIMESTAMP_SIZE)
	binary.LittleEndian.PutUint64(secb, uint64(sec))
	data = append(data, secb...) //dodaje se Timestamp

	//1 - deleted; 0 - not deleted
	//dodaje se Tombstone
	if node.Tombstone {
		//ako je tombstone 1 onda bez value size i value
		var delb byte = 1
		data = append(data, delb)
		keyb := []byte(node.Transaction.Key)
		keybs := make([]byte, KEY_SIZE_SIZE)
		binary.LittleEndian.PutUint64(keybs, uint64(len(keyb)))
		//upisujemo key size i key
		data = append(data, keybs...)
		data = append(data, keyb...)

	} else {
		//ako je tombstone 0 onda sa svim kao u WALu
		var delb byte = 0
		data = append(data, delb)
		keyb := []byte(node.Transaction.Key)
		keybs := make([]byte, KEY_SIZE_SIZE)
		binary.LittleEndian.PutUint64(keybs, uint64(len(keyb)))
		valuebs := make([]byte, VALUE_SIZE_SIZE)
		binary.LittleEndian.PutUint64(valuebs, uint64(len(node.Transaction.Value)))

		//dodaju se Key Size i Value Size
		data = append(data, keybs...)
		data = append(data, valuebs...)
		//dodaju se Key i Value
		data = append(data, keyb...)
		data = append(data, node.Transaction.Value...)
	}

	return data
}


func MakeData(nodes []*Config.Entry, DataFileName string, IndexFileName string, SummaryFileName string, BloomFileName string) {
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
	// uzima najmanji i najveci kljuc iz nodes iz memtable
	first := make([]byte, KEY_SIZE_SIZE)
	last := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(first, uint64(len([]byte(nodes[0].Transaction.Key))))
	binary.LittleEndian.PutUint64(last, uint64(len([]byte(nodes[len(nodes)-1].Transaction.Key))))
	// upisuje ih u summary
	summaryFile.Write(first)
	summaryFile.Write(last)
	summaryFile.Write([]byte(nodes[0].Transaction.Key))
	summaryFile.Write([]byte(nodes[len(nodes)-1].Transaction.Key))

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
		_, err = file.Write(NodeToBytes(*node))
		if err != nil {
			return
		}
		// upisivanje u index fajl
		positionSum := AddToIndex(position, node.Transaction.Key, indexFile)
		// upisuje svaki peti u summary file
		if n%5 == 0 {
			AddToSummary(positionSum, node.Transaction.Key, summaryFile)
		}
		n += 1
	}
	err = file.Sync()
	if err != nil {
		return
	}
}

// sizeTierdCompaction
func SizeTieredCompaction(lsm Config.LSMTree) {
	if lsm.Levels[0] == lsm.MaxSSTables {
		merge(1, lsm)
	}
}

func merge(level int, lsm Config.LSMTree) {
	br := lsm.Levels[level] + 1
	dataFile, _ := os.Create("SSTable/files/dataFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".txt")
	indexFile, _ := os.Create("SSTable/files/indexFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".txt")
	summaryFile, _ := os.Create("SSTable/files/summaryFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".txt")
	bloomFile, _ := os.Create("SSTable/files/bloomFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".txt")
	merkleFile, _ := os.Create("SSTable/files/merkleFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".txt")

	lsm.DataFilesNames = append(lsm.DataFilesNames, dataFile.Name())
	mergeFiles(level, dataFile, indexFile, summaryFile, bloomFile, merkleFile, lsm)
	lsm.Levels[level-1] = 0
	lsm.Levels[level]++
	if lsm.Levels[level] == lsm.MaxSSTables && level != lsm.CountOfLevels { // proverava broj fajlova na sledećem nivou, i ne treba da pozove merge ako je na 3. nivou tj ako je nivo 2
		merge(level+1, lsm)
	}
}

func openFiles(fileNames []string) ([]*os.File, error) {
	var openedFiles []*os.File

	for _, fileName := range fileNames {
		file, err := os.Open(fileName)
		if err != nil {
			// ako je doslo do greske pri otvaranju nekog fajla, zatvori one vec otvorene
			for _, openedFile := range openedFiles {
				openedFile.Close()
			}
			return nil, err
		}
		openedFiles = append(openedFiles, file)
	}

	return openedFiles, nil
}

func closeFiles(files []*os.File) {
	for _, file := range files {
		file.Close()
	}
}

func levelFileNames(dataFileNames []string, substring string) []string {
	var filteredFilenames []string

	for _, filename := range dataFileNames {
		if strings.Contains(filename, substring) {
			filteredFilenames = append(filteredFilenames, filename)
		}
	}

	return filteredFilenames
}

// cita iz fajla za merge, vraca procutani entry ili nil ako smo dosli do kraja fajla
func readMerge(file *os.File) *Config.Entry {

	// cita bajtove podatka DO key i value u info
	// CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B)
	info := make([]byte, KEY_SIZE_START)
	_, err := file.Read(info)
	if err != nil {
		return nil
	}

	tombstone := binary.LittleEndian.Uint64(info[TOMBSTONE_START:KEY_SIZE_START])
	//ako je tombstone 1 procitaj odmah sledeci
	if tombstone == 1 {
		return readMerge(file)
	}
	//ako je tombstone 0 onda citaj sve podatke entrya
	info2 := make([]byte, KEY_START-KEY_SIZE_START)
	_, err = file.Read(info2)
	if err != nil {
		return nil
	}

	key_size := binary.LittleEndian.Uint64(info2[:KEY_SIZE_SIZE])
	value_size := binary.LittleEndian.Uint64(info2[KEY_SIZE_SIZE:])

	// cita bajtove podatka, odnosno key i value u data
	//| Key | Value |
	data := make([]byte, key_size+value_size)
	_, err = file.Read(data)
	if err != nil {
		return nil
	}
	info = append(info, info2...)
	info = append(info, data...)

	entry := Config.ToEntry(info)
	return &entry
}

// vraca true ako su svi elementi niza nil, false kao je bar jedan razlicit od nil
func areAllNil(arr []*Config.Entry) bool {
	for _, element := range arr {
		if element != nil {
			return false
		}
	}
	return true
}

func findMinKeyEntry(entries []*Config.Entry) ([]int, *Config.Entry) {

	if len(entries) == 0 {
		return nil, nil
	}

	// Initialize minKeyEntry with the first element
	var minKeyArray []int
	var minKey string
	//postavljanje pocetnih vrednosti za min
	for index, entry := range entries {
		if entry == nil {
			continue
		}
		minKeyArray = append(minKeyArray, index)
		minKey = entry.Transaction.Key
		break
	}

	// Iterate through the array and find the entry with the minimum key
	for index, entry := range entries {
		if entry == nil {
			continue
		}
		if entry.Transaction.Key < minKey {
			//menjamo min key i praznimo niz sa indeksima
			minKeyArray := make([]int, 0)
			minKeyArray = append(minKeyArray, index)
			minKey = entry.Transaction.Key
		} else if entry.Transaction.Key == minKey {
			//ako smo naisli na jos jedna entry sa min key dodamo ga u listu tj njegov indeks
			minKeyArray = append(minKeyArray, index)
		}
	}
	var mostRecentEntry *Config.Entry
	//ako imamo jedan entry sa min key onda vrati njega za najnoviji
	if len(minKeyArray) == 1 {
		return minKeyArray, entries[minKeyArray[0]]
	}

	//ako ima vise entya sa istim key, nadji najnoviji
	mostRecentEntry = entries[minKeyArray[0]]
	for _, index := range minKeyArray {
		if entries[index].Timestamp > mostRecentEntry.Timestamp {
			mostRecentEntry = entries[index]
		}
	}

	return minKeyArray, mostRecentEntry
}
func removeFileName(lsm Config.LSMTree, name string) {
	slice1 := []string{}
	slice2 := []string{}
	for i, n := range lsm.DataFilesNames {
		if n == name {
			slice1 = append(lsm.DataFilesNames[:i], lsm.DataFilesNames[i+1:]...)
			break
		}
	}

	lsm.DataFilesNames = append(slice2, slice1...)
}

func mergeFiles(level int, dataFile *os.File, indexFile *os.File, summaryFile *os.File, bloomFile *os.File, merkleFile *os.File, lsm Config.LSMTree) {

	//otvorimo sve fajlove
	//procitamo prvi podatak (tombstone) iz svakog od njih i njih stvaimo u listu
	//ako je dosao do rkaja fajla vraca nil
	//trazimo min kljuc i njega dodajemo u novu skiplistu
	//ako imamo iste kljuceve onda nadji najnoviji i njega dodaj u skiplist a ostale prekosci

	//uzimamo imena svih data fajlova ovog nivoa
	levelSubstring := "SSTable/files/dataFile_" + strconv.Itoa(level) + "_"
	levelFileNames := levelFileNames(lsm.DataFilesNames, levelSubstring)
	//otvaranje data fajlova na ovom nivou
	levelFiles, err := openFiles(levelFileNames)
	if err != nil {
		//ako ima greske pri otvaranju nekog fajla
		panic(err)
	}

	var entries []*Config.Entry
	var sortedAllEntries []*Config.Entry
	//u entries cuvamo trenutne entie na kojim smo iz svakog sstablea sa ovog nivoa
	for _, file := range levelFiles {
		entry := readMerge(file)
		entries = append(entries, entry)
	}
	for {
		//procitali smo sve fajlove do kraja
		if areAllNil(entries) {
			break
		}
		minKeyArray, minEntry := findMinKeyEntry(entries)
		sortedAllEntries = append(sortedAllEntries, minEntry)
		//citamo naredne entye za fajlove koji su bili na min entry
		for _, index := range minKeyArray {
			newEntry := readMerge(levelFiles[index])
			entries[index] = newEntry
		}

	}
	closeFiles(levelFiles)

	//pravljenje novog sstablea od svih sstableova ovog nivoa koji su sada spojeni
	MakeData(sortedAllEntries, dataFile.Name(), indexFile.Name(), summaryFile.Name(), bloomFile.Name())

	for i := 1; i <= lsm.MaxSSTables; i++ {
		err = os.Remove("SSTable/files/dataFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + "txt")
		if err != nil {
			log.Fatal(err)
		}
		err = os.Remove("SSTable/files/indexFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + "txt")
		if err != nil {
			log.Fatal(err)
		}
		err = os.Remove("SSTable/files/summaryFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + "txt")
		if err != nil {
			log.Fatal(err)
		}
		err = os.Remove("SSTable/files/bloomFilterFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + "txt")
		if err != nil {
			log.Fatal(err)
		}
		err = os.Remove("SSTable/files/merkleTreeFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + "txt")
		if err != nil {
			log.Fatal(err)
		}

		removeFileName(lsm, "SSTable/files/dataFile_"+strconv.Itoa(level)+"_"+strconv.Itoa(i)+"txt")
		removeFileName(lsm, "SSTable/files/indexFile_"+strconv.Itoa(level)+"_"+strconv.Itoa(i)+"txt")
		removeFileName(lsm, "SSTable/files/summaryFile_"+strconv.Itoa(level)+"_"+strconv.Itoa(i)+"txt")
		removeFileName(lsm, "SSTable/files/bloomFilterFile_"+strconv.Itoa(level)+"_"+strconv.Itoa(i)+"txt")
		removeFileName(lsm, "SSTable/files/merkleTreeFile_"+strconv.Itoa(level)+"_"+strconv.Itoa(i)+"txt")
	}
}



//---------------------------LEVEL TIERED COMPACTION--------------------------------
// kod level tiered kompakcije svaki nivo (run) je T puta veci od prethodnog. T je uglavnom 10. Kriterijum za kompakciju ce biti broj tabela po run-u.
// Uzima se tabela iz nivoa na kom se vrsi kompakcija i traze se odgovarajuce tabele u narednom nivou. Spajaju se i nova tabela se dodaje u nizi nivo.
// Imenuju se kao level_brojulevelu.

// utvrditi nivo na kom se kompakcija desava
// znaci kada se flushuje
func LevelTieredCompaction(lsm Config.LSMTree) {
	if lsm.Levels[0] == lsm.MaxSSTables {
		levelMerge(0, lsm)
	}
}

func levelMerge(level int, lsm Config.LSMTree) {
	//treba da se izabere tabela koja se merguje
	//pa da se potraze ostale tabele u sledecem nivou

	//za file na visem nivou-uzimamo prvu tabelu jer eto??
	br := lsm.Levels[level]

	dataFile := "SSTable/files/dataFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(br) + ".txt"
	indexFile := "SSTable/files/indexFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(br) + ".txt"
	summaryFile := "SSTable/files/summaryFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(br) + ".txt"
	bloomFile := "SSTable/files/bloomFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(br) + ".txt"
	merkleFile := "SSTable/files/merkleFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(br) + ".txt"

	//trazimo opseg indeksa
	sumarry, _ := os.OpenFile(summaryFile, os.O_RDWR, 0777)
	SummaryContent := LoadSummary(sumarry)
	//SummaryContent := LoadSummary(summaryFile)

	bottomIdx := SummaryContent.FirstKey
	topIdx := SummaryContent.LastKey


	//nizovi putanja SSTable-ova kojima odgovaraju indeksi
	dataFiles, indexFiles, summaryFiles, bloomFiles, merkleFiles, _ := findOtherTables(level + 1, bottomIdx, topIdx, lsm)
	num := len(dataFiles)

	dataFiles = append(dataFiles, dataFile)
	indexFiles = append(indexFiles, indexFile)
	summaryFiles = append(summaryFiles, summaryFile)
	bloomFiles = append(bloomFiles, bloomFile)
	merkleFiles = append(merkleFiles, merkleFile)
		

	//Sada treba mergovati tabele
		// num - ne treba ovde, prebacila sam ga gore jer odatle isto mozemo naci broj svakako
	levelMergeFiles(level, dataFiles, indexFiles, summaryFiles, bloomFiles, merkleFiles, lsm, num)

	
	// oduzmi jednu iz levela sto smo prebacili dole
	lsm.Levels[level]--
	// (dodaj tu jednu iz levela na [level + 1], i oduzmi num merge-ovanih)
	lsm.Levels[level + 1] += 1     // dodaj spojenu koju smo prebacili tu
	lsm.Levels[level + 1] -= num   // oduzmi sve koje smo spojili sa tog nivoa


	// ** T: provjeriti da li je okej uslov za level tiered?
	if lsm.Levels[level + 1] == int(float64(lsm.MaxSSTables) * math.Pow( float64(lsm.T) ,float64(level + 1))) && level != lsm.CountOfLevels { 
		// proverava broj fajlova na sledećem nivou, i ne treba da pozove merge ako je na 3. nivou tj ako je nivo 2
		levelMerge(level + 1, lsm)
	}
}



func findOtherTables(level int, bottomIdx string, topIdx string, lsm Config.LSMTree) ([]string, []string, []string, []string, []string, []*Config.Entry) {

	var dataFiles []string
	var indexFiles []string
	var summaryFiles []string
	var bloomFiles []string
	var merkleFiles []string

	var entriesAdd []*Config.Entry   // za pocetak je nil


	for i := 1; i <= lsm.Levels[level]; i++ {
		summaryFile := "SSTable/files/summaryFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".txt"

		sumarry, _ := os.OpenFile(summaryFile, os.O_RDWR, 0777)
		SummaryContent := LoadSummary(sumarry)

		FirstKey := SummaryContent.FirstKey
		LastKey := SummaryContent.LastKey

		var dataFile = "SSTable/files/dataFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".txt"
		var indexFile = "SSTable/files/indexFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".txt"
		//var summaryFile = "SSTable/files/summaryFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".txt"   // vec ima gore deklarisano
		var bloomFile = "SSTable/files/bloomFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".txt"
		var merkleFile = "SSTable/files/merkleFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".txt"

		if FirstKey >= bottomIdx && FirstKey <= topIdx && LastKey >= bottomIdx && LastKey <= topIdx {
			dataFiles = append(dataFiles, dataFile)
			indexFiles = append(indexFiles, indexFile)
			summaryFiles = append(summaryFiles, summaryFile)
			bloomFiles = append(bloomFiles, bloomFile)
			merkleFiles = append(merkleFiles, merkleFile)
		} else {  

			// NPR. ukupan prvi opseg = [30 - 80] ---> [bottomIdx - topIdx]

			// u ovoj sstabeli je npr. [60 - 95] ---> [FirstKey - LastKey] 
			// prvi se nalazi u opsegu
			if FirstKey >= bottomIdx && FirstKey <= topIdx {
				// k1, k2       k2, k3
				// 60  80		 80 95
				k1 := FirstKey   
				k2 := topIdx
				k3 := LastKey

				// mogu poslati u jednu funckiju npr. splitSSTable(1, 30, 40)
				// ona vrati entriesRewrite, entriesAdd
				// (entriesRewrite - entry za rewrite samo)							 //  80 - 95
				// (entriesAdd     - entry za dodati u novu, veliku sstaeblu)   //  60 - 80
					// saljemo "false" jer nije "lower" (uporedjujemo sa "višim" kljucem iz opsega - topIdx ( == 80 u ovom slucaju))
			   entriesAdd = splitSSTable(k1, k2, k3, false, dataFile, indexFile, summaryFile, bloomFile, merkleFile, lsm)
			}


			// NPR. ukupan prvi opseg = [30 - 80] ---> [bottomIdx - topIdx]

			// u ovoj sstabeli je npr. [1 - 40] ---> [FirstKey - LastKey] 
			// drugi se nalazi u opsegu
			if LastKey >= bottomIdx && LastKey <= topIdx {
				// k1, k2       k2, k3
				// 1   30		 30, 40
				k1 := FirstKey   
				k2 := bottomIdx
				k3 := LastKey

				// mogu poslati u jednu funckiju npr. splitSSTable(1, 30, 40)
				// ona vrati entriesRewrite, entriesAdd
				// (entriesRewrite - entry za rewrite samo)							  //  1 - 30
				// (entriesAdd     - entry za dodati u novu, veliku sstaeblu)    // 30 - 40
					// saljemo "true" jer je "lower" (uporedjujemo sa "nižim" kljucem iz opsega - topIdx ( == 30 u ovom slucaju))
			   entriesAdd = splitSSTable(k1, k2, k3, true, dataFile, indexFile, summaryFile, bloomFile, merkleFile, lsm)
				
			}
		}
	}

			// ***** TREBA VRATITI PRAZAN entriesAdd ako nema preklapanja!! (nil)
	return dataFiles, indexFiles, summaryFiles, bloomFiles, merkleFiles, entriesAdd
}



func splitSSTable(k1 string, k2 string, k3 string, lower bool, dataFile string, indexFile string, summaryFile string, bloomFile string, merkleFile string,  lsm Config.LSMTree) ([]*Config.Entry) {
	// k1 = 1               1 - 30
	// k2 = 30             30 - 40
	// k3 = 40
	// otvaramo samo taj jedan fajl koji treba rewrite  // onaj od   1 - 40 

	entriesRewrite, entriesAdd := GetSplitEntries(dataFile, k2, lower)
	// sada napravimo nove splitovane SSTabele od ovih entrija koje smo izdvojili
	// koji ne idu u veliku SSTabelu (ovi od  1 - 30) ,  (od 30 - 40 bi trebali ici u veliki SSTable)


	// prije return treba UPISATI ove REWRITE ENTRIES (to su dijelovi kkljuceva koji nisu u izabranom opsegu)
	err := os.Remove(dataFile)
	if err != nil {
		log.Fatal(err)
	}
	err = os.Remove(indexFile)
	if err != nil {
		log.Fatal(err)
	}
	err = os.Remove(summaryFile)
	if err != nil {
		log.Fatal(err)
	}
	err = os.Remove(bloomFile)
	if err != nil {
		log.Fatal(err)
	}
	err = os.Remove(merkleFile)
	if err != nil {
		log.Fatal(err)
	}

	// takodje brisemo i iz lsm ovo isto kao gore
	removeFileName(lsm, dataFile)
	removeFileName(lsm, indexFile)
	removeFileName(lsm, summaryFile)
	removeFileName(lsm, bloomFile)
	removeFileName(lsm, merkleFile)


	//pravljenje novog sstablea od svih sstableova ovog nivoa koji su sada spojeni
	//fali merkle???
	MakeData(entriesRewrite, dataFile, indexFile, summaryFile, bloomFile)

	return entriesAdd
}


// vraca sve entrije, ali splitovane u dva dijela prema kljucu, zbog preklapanja opsega
	// filename  - naziv fajla nad kojim radimo
	// borderKey - kljuc nakon koga splitujemo
	// lower     - true: firstKey NE UPADA u opseg,   false: firstKey UPADA u opseg
func GetSplitEntries(dataFile string, borderKey string, lower bool) ([]*Config.Entry, []*Config.Entry)  {

	var entriesRewrite []*Config.Entry   // svi procitani entry koji ce se rewrite u novu malu SSTabelu
	var entriesAdd     []*Config.Entry   // svi procitani entry koji ce se spojiti sa velikom tabelom

	file, err := os.OpenFile(dataFile, os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	for {
		// cita bajtove podatka DO key i value u info
		// CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B)
		info := make([]byte, KEY_SIZE_START)
		_, err = file.Read(info)
		if err != nil {
			break  // kraj fajla?
		}

		tombstone2 := binary.LittleEndian.Uint64(info[TOMBSTONE_START:KEY_SIZE_START])
		tombstone := true
		if (tombstone2 == 0) {
			tombstone = false
		}

		info2 := make([]byte, KEY_START-KEY_SIZE_START)
		_, err = file.Read(info2)
		if err != nil {
			panic(err)
		}

		key_size := binary.LittleEndian.Uint64(info2[:KEY_SIZE_SIZE])
		value_size := binary.LittleEndian.Uint64(info2[KEY_SIZE_SIZE:])

		// cita bajtove podatka, odnosno key i value u data
		//| Key | Value |
		data := make([]byte, key_size+value_size)
		_, err = file.Read(data)
		if err != nil {
			panic(err)
		}

		entry := Config.Entry {
			// **** OVO CRC I TIMESTAMP PROVJERITI OBAEVEZNO JEL DOBRO CITA UOPSTE??? 
			//      (posto nije postojalo u funckiji iz koje sam prepisala ovo sve)
			Crc:       	 binary.LittleEndian.Uint32(info[CRC_START : CRC_START+CRC_SIZE]),
			Timestamp:   binary.LittleEndian.Uint64(info[TIMESTAMP_START : TIMESTAMP_START+TIMESTAMP_SIZE]),
			Tombstone:   tombstone, 
			Transaction: Config.Transaction {
				Key:    string(data[:key_size]),
				Value:  data[key_size:],
			},
		}

		if (lower) {   // manji kljuc od border kljuca ide u rewrite 
			if (entry.Transaction.Key >= borderKey) {
				entriesAdd = append(entriesAdd, &entry)           // 30 - 40
			} else {
				entriesRewrite = append(entriesRewrite, &entry)   // 1 - 30
			}
		} else {       // veci kljuc od border kljuca ide u rewrite 
			if (entry.Transaction.Key <= borderKey) {
				entriesAdd = append(entriesAdd, &entry)           // 60 - 80
			} else {
				entriesRewrite = append(entriesRewrite, &entry)   // 80 - 96
			}
		}
	}

	return entriesRewrite, entriesAdd
}


func levelMergeFiles(level int, dataFiles []string, indexFiles []string, summaryFiles []string, bloomFiles []string, merkleFiles []string, lsm Config.LSMTree, num int) {

	//levelFiles su SStabele iz narednog nivoa + tabela iz prethodnog
	levelFiles, err := openFiles(dataFiles)
	if err != nil {
		panic(err)
	}

	var entries []*Config.Entry
	var sortedAllEntries []*Config.Entry
	//u entries cuvamo trenutne entie na kojim smo iz svakog sstablea sa ovog nivoa

	for _, file := range levelFiles {
		entry := readMerge(file)
		entries = append(entries, entry)
	}

	for {
		//procitali smo sve fajlove do kraja
		if areAllNil(entries) {
			break
		}
		minKeyArray, minEntry := findMinKeyEntry(entries)
		sortedAllEntries = append(sortedAllEntries, minEntry)
		//citamo naredne entye za fajlove koji su bili na min entry
		for _, index := range minKeyArray {
			newEntry := readMerge(levelFiles[index])
			entries[index] = newEntry
		}
	}
	closeFiles(levelFiles)

	dataFileName := dataFiles[0]
	indexFileName := indexFiles[0]
	summaryFileName := summaryFiles[0]
	bloomFileName := bloomFiles[0]
	merkleFileName := merkleFiles[0]
	

	// uklanjamo sve vezano za pocetnu odabranu SStabelu, jer
	// cemo kasnije na ove putanje postaviti sve za nasu novu, spojenu, veliku SSTabelu
	err = os.Remove(dataFileName)
	if err != nil {
		log.Fatal(err)
	}
	err = os.Remove(indexFileName)
	if err != nil {
		log.Fatal(err)
	}
	err = os.Remove(summaryFileName)
	if err != nil {
		log.Fatal(err)
	}
	err = os.Remove(bloomFileName)
	if err != nil {
		log.Fatal(err)
	}
	err = os.Remove(merkleFileName)
	if err != nil {
		log.Fatal(err)
	}

	// takodje brisemo i iz lsm ovo isto kao gore
	removeFileName(lsm, dataFileName)
	removeFileName(lsm, indexFileName)
	removeFileName(lsm, summaryFileName)
	removeFileName(lsm, bloomFileName)
	removeFileName(lsm, merkleFileName)


	//pravljenje novog sstablea od svih sstableova ovog nivoa koji su sada spojeni
	//fali merkle???
	MakeData(sortedAllEntries, dataFileName, indexFileName, summaryFileName, bloomFileName)


	// brisemo sve fajlove za ostale SSTabele, jer su spojene u veliku i ne trebaju nam vise
	for i := 1; i < len(dataFiles); i++ {
		err = os.Remove(dataFiles[i])
		if err != nil {
			log.Fatal(err)
		}
		err = os.Remove(indexFiles[i])
		if err != nil {
			log.Fatal(err)
		}
		err = os.Remove(summaryFiles[i])
		if err != nil {
			log.Fatal(err)
		}
		err = os.Remove(bloomFiles[i])
		if err != nil {
			log.Fatal(err)
		}
		err = os.Remove(merkleFiles[i])
		if err != nil {
			log.Fatal(err)
		}

		//nisam sigurna da li ovo zapravo izbaci 
		removeFileName(lsm, dataFiles[i])
		removeFileName(lsm, indexFiles[i])
		removeFileName(lsm, summaryFiles[i])
		removeFileName(lsm, bloomFiles[i])
		removeFileName(lsm, merkleFiles[i])
	}

	//msm da bi trebalo da stoji umesto dataFiles lsm.DataFilesNames itd ali treba pogledati
	sort.Slice(lsm.DataFilesNames, func(i, j int) bool {
		return compareFilenames(i, j, lsm.DataFilesNames)
	})
	sort.Slice(lsm.IndexFilesNames, func(i, j int) bool {
		return compareFilenames(i, j, lsm.IndexFilesNames)
	})
	sort.Slice(lsm.SummaryFilesNames, func(i, j int) bool {
		return compareFilenames(i, j, lsm.SummaryFilesNames)
	})
	sort.Slice(lsm.BloomFilterFilesNames, func(i, j int) bool {
		return compareFilenames(i, j, lsm.BloomFilterFilesNames)
	})
	sort.Slice(lsm.MerkleTreeFilesNames, func(i, j int) bool {
		return compareFilenames(i, j, lsm.MerkleTreeFilesNames)
	})


	renameFiles(lsm.DataFilesNames, dataFileName, num)
	renameFiles(lsm.IndexFilesNames, indexFileName, num)
	renameFiles(lsm.SummaryFilesNames, summaryFileName, num)
	renameFiles(lsm.BloomFilterFilesNames, bloomFileName, num)
	renameFiles(lsm.MerkleTreeFilesNames, merkleFileName, num)


	//Da li treba rename ostalih fajlova??
	// pretpostavljam da sada kada se napravi nova SSTabela velika (spojena od vise)

	// recimo da gledamo ovako da ima
	//  L2  |   2_1.txt   > 2_2.txt     2_3.txt    2_4.txt     
	//  L3  |   3_1.txt   > 3_2.txt   > 3_3.txt    3_4.txt    3_5.txt    3_6.txt

	// spajamo 2_2, 3_2 i 3_3
	// dobijamo novu SSTabelu sa pathName: 3_2.txt (tako smo izabrali, da path bude prva u narednom nivou)


	// dobijamo sledece:
	//  L2  |   2_1.txt    2_3.txt    2_4.txt   
	//  L3  |   3_1.txt   *3_2.txt    3_4.txt    3_5.txt    3_6.txt

	// gdje je *3_2.txt nova spojena SSTabela


// PAR PITANJA: 
	// 1. Da li treba rename preostale?
	// - 99% da treba, jer nekima pristupamo preko i = 1; i < n; i++, pa cemo propustiti neke

	// 2. Sta ako se *3_2.txt ustv appenduje na kraj, pa izgleda ovako:
	//  L2  |   2_1.txt    2_3.txt    2_4.txt   
	//  L3  |   3_1.txt    3_4.txt    3_5.txt    3_6.txt    *3_2.txt 
	 
	// - onda trebamo sortirati sve prvo? pa onda rename uraditi?
	// - za ovo 2. nisam sigurna tacno kako je predstavljeno, to je navodno taj LSM tree, a kako to sve izgleda tu?
	// - kako se appenduje?
}

func compareFilenames(i, j int,fileNames []string) bool {
	
	//trazenje poslednjeg i pretposlednjeg broja u imenu
	//npr ako stoji 3_11 , pretposlednji je 3, poslednji je 11
	numI, subNumI := extractNumbers(fileNames[i])
	numJ, subNumJ := extractNumbers(fileNames[j])

	// Poređenje pretposlednjih brojeva
	if subNumI != subNumJ {
		return subNumI < subNumJ
	}

	// Ako su pretposlednji brojevi jednaki, poređenje poslednjih brojeva
	return numI < numJ
}

func extractNumbers(fileName string) (int, int) {
	// Razdvajanje naziva fajla na osnovu donje crte
	parts := strings.Split(fileName, "_")

	// Konverzija poslednjeg dela u broj, uzimajući u obzir ekstenziju
	lastPartWithExt := parts[len(parts)-1]
	lastPartWithoutExt := strings.TrimSuffix(lastPartWithExt, "")
	lastNum, _ := strconv.Atoi(lastPartWithoutExt)

	// Konverzija pretposlednjeg dela u broj
	preLastPart := parts[len(parts)-2]
	preLastNum, _ := strconv.Atoi(preLastPart)

	return lastNum, preLastNum
}

func renameFiles(files []string, targetFile string, num int){

	lastNum, preLastNum := extractNumbers(targetFile)

	var firstIdx int = -1
	var lastIdx int = -1

	
	//npr. ako je niz 2_2, 3_1, 3_2, 3_4, 3_5, 4_1, 4_6
	//firstIdx = 3
	//lastIdx = 5
	for i, path := range files{
		tempLastNum, tempPreLastNum := extractNumbers(path)

		if tempPreLastNum < preLastNum{
			continue
		}

		//ako je prosao svoj level, uzima indeks elementa kao indikator za kraj prethodnog levela
		if tempPreLastNum > preLastNum{
			lastIdx = i
			break
		}

		//ako je dosao do ovde to znaci da je dosao do putanja trazenog levela

		//pronasli smo indeks na kom se nalazi prvi element nakon targetFile u nizu putanja
		if lastNum == tempLastNum{
			firstIdx = i+1
		}
	}

	for i:=firstIdx ; i<lastIdx ;i++{

		path := files[i]
		tempLastNum, _ := extractNumbers(path)
		
		tempLastNum-=num
		newPath := strings.Replace(path, fmt.Sprintf("_%d.", lastNum), fmt.Sprintf("_%d.", tempLastNum), 1)

		err := os.Rename(path, newPath)
		if err != nil {
			fmt.Println("Greška pri preimenovanju fajla:", err)
			return
		}
	}
}
