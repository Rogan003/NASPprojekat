package SSTable

import (
	"NASPprojekat/BloomFilter"
	"NASPprojekat/Config"
	"NASPprojekat/MerkleTree"
	"hash/crc32"

	//"io/ioutil"
	"strconv"
	"strings"

	"encoding/binary"
	//"encoding/json"
	"fmt"
	"io"
	"log"

	"math"
	"os"
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
		bf.Add([]byte(val.Transaction.Key))
	}

	bf.Serialize(filePath)
}

func make_merkle(elems []*Config.Entry, filePath string, comp bool, dict1 *map[int]string, dict2 *map[string]int) {
	// flag: Tamara
	// elems = nodes
	// pretvoriti niz elems u niz bajtova
	var data [][]byte

	for _, entry := range elems {
		bytesData := NodeToBytes(*entry, comp, dict1, dict2)
		data = append(data, bytesData)
	}

	mt := MerkleTree.MerkleTree{}
	mt.Init(data)

	mt.Serialize(filePath)
}

func DataFileToBytes(DataFileName string, comp bool) [][]byte {

	retVal := make([][]byte, 0)

	file, err := os.OpenFile(DataFileName, os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(err)
		return [][]byte{}
	}
	defer file.Close()

	var position int64 = 0

	_, err = file.Seek(position, 0)
	if err != nil {
		log.Fatal(err)
		return [][]byte{}
	}

	for {
		byteData := make([]byte, 0)

		if comp {
			_, err = file.Seek(position, 0)
			if err != nil {
				break
			}

			info := make([]byte, KEY_SIZE_START)
			_, err = file.Read(info)
			if err != nil {
				break
			}

			num, n := binary.Uvarint(info[TIMESTAMP_START:])

			tombstone := info[TIMESTAMP_START+n]

			byteData = append(byteData, info[:TIMESTAMP_START+n+1]...)

			if tombstone == 1 {
				info = make([]byte, 10)

				position += int64(TIMESTAMP_START + n + 1)
				_, err = file.Seek(position, 0)
				if err != nil {
					break
				}

				_, err = file.Read(info)
				if err != nil {
					break
				}

				_, n := binary.Uvarint(info)

				byteData = append(byteData, info[:n]...)
				retVal = append(retVal, byteData)

				position += int64(n)

				continue
			}

			position += int64(TIMESTAMP_START + n + 1)
			_, err = file.Seek(position, 0)
			if err != nil {
				break
			}

			info = make([]byte, VALUE_SIZE_SIZE)

			_, err = file.Read(info)
			if err != nil {
				break
			}

			num, n = binary.Uvarint(info)
			value_size := num

			byteData = append(byteData, info[:n]...)

			position += int64(n)
			_, err = file.Seek(position, 0)
			if err != nil {
				break
			}

			info = make([]byte, 10)

			_, err = file.Read(info)
			if err != nil {
				break
			}

			num, n = binary.Uvarint(info)

			byteData = append(byteData, info[:n]...)

			position += int64(n)
			_, err = file.Seek(position, 0)
			if err != nil {
				break
			}

			data := make([]byte, value_size)
			_, err = file.Read(data)
			if err != nil {
				break
			}

			byteData = append(byteData, data...)

			position += int64(value_size)

		} else {
			// cita bajtove podatka DO key i value u info
			// CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B)
			info := make([]byte, KEY_SIZE_START)
			_, err = file.Read(info)
			if err != nil {
				if err == io.EOF {
					break
				}

				break
			}

			byteData = append(byteData, info...)

			tombstone := info[TOMBSTONE_START] // jel ovo sad prepoznaje obrisane

			//ako je tombstone 1 ne citaj dalje
			if tombstone == 1 {
				info3 := make([]byte, KEY_SIZE_SIZE)
				_, err = file.Read(info3)
				if err != nil {
					break
				}

				byteData = append(byteData, info3...)

				key_size := binary.LittleEndian.Uint64(info3)

				// cita bajtove podatka, odnosno key data
				data := make([]byte, key_size)
				_, err = file.Read(data)
				if err != nil {
					break
				}

				byteData = append(byteData, data...)
				retVal = append(retVal, byteData)

				continue
			}
			//ako je tombstone 0 onda citaj sve
			info2 := make([]byte, KEY_START-KEY_SIZE_START)
			_, err = file.Read(info2)
			if err != nil {
				break
			}

			byteData = append(byteData, info2...)

			key_size := binary.LittleEndian.Uint64(info2[:KEY_SIZE_SIZE])
			value_size := binary.LittleEndian.Uint64(info2[KEY_SIZE_SIZE:])

			// cita bajtove podatka, odnosno key i value u data
			//| Key | Value |
			data := make([]byte, key_size+value_size)
			_, err = file.Read(data)
			if err != nil {
				break
			}

			byteData = append(byteData, data...)

		}

		retVal = append(retVal, byteData)
	}

	return retVal

}

func OneFileDataToBytes(OneFileName string, comp bool) [][]byte {

	retVal := make([][]byte, 0)

	file, _ := os.OpenFile(OneFileName, os.O_RDONLY, 0777)
	file.Seek(KEY_SIZE_SIZE, 0)

	indexOffsetBytes := make([]byte, KEY_SIZE_SIZE)

	_, _ = file.Read(indexOffsetBytes)

	indexOffset := binary.LittleEndian.Uint64(indexOffsetBytes)

	dataOffsetBytes := make([]byte, KEY_SIZE_SIZE)

	_, _ = file.Read(dataOffsetBytes)

	dataOffset := binary.LittleEndian.Uint64(dataOffsetBytes)

	position := int64(dataOffset)

	for position < int64(indexOffset) {
		file.Seek(int64(position), 0)

		byteData := make([]byte, 0)

		if comp { // zakomentarisati dok ne budu svuda primenjene
			_, err := file.Seek(position, 0)
			if err != nil {
				break
			}

			info := make([]byte, KEY_SIZE_START)
			_, err = file.Read(info)
			if err != nil {
				break
			}

			num, n := binary.Uvarint(info[TIMESTAMP_START:])

			tombstone := info[TIMESTAMP_START+n]

			byteData = append(byteData, info[:TIMESTAMP_START+n+1]...)

			if tombstone == 1 {
				info = make([]byte, 10)

				position += int64(TIMESTAMP_START + n + 1)
				_, err = file.Seek(position, 0)
				if err != nil {
					break
				}

				_, err = file.Read(info)
				if err != nil {
					break
				}

				_, n := binary.Uvarint(info)

				byteData = append(byteData, info[:n]...)
				retVal = append(retVal, byteData)

				position += int64(n)

				continue
			}

			position += int64(TIMESTAMP_START + n + 1)
			_, err = file.Seek(position, 0)
			if err != nil {
				break
			}

			info = make([]byte, VALUE_SIZE_SIZE)

			_, err = file.Read(info)
			if err != nil {
				break
			}

			num, n = binary.Uvarint(info)
			value_size := num

			byteData = append(byteData, info[:n]...)

			position += int64(n)
			_, err = file.Seek(position, 0)
			if err != nil {
				break
			}

			info = make([]byte, 10)

			_, err = file.Read(info)
			if err != nil {
				break
			}

			num, n = binary.Uvarint(info)

			byteData = append(byteData, info[:n]...)

			position += int64(n)
			_, err = file.Seek(position, 0)
			if err != nil {
				break
			}

			data := make([]byte, value_size)
			_, err = file.Read(data)
			if err != nil {
				break
			}

			byteData = append(byteData, data...)

			position += int64(value_size)

		} else {
			// cita bajtove podatka DO key i value u info
			// CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B)
			info := make([]byte, KEY_SIZE_START)
			_, err := file.Read(info)
			if err != nil {
				if err == io.EOF {
					break
				}

				break
			}

			byteData = append(byteData, info...)

			tombstone := info[TOMBSTONE_START] // jel ovo sad prepoznaje obrisane

			//ako je tombstone 1 ne citaj dalje
			if tombstone == 1 {
				info3 := make([]byte, KEY_SIZE_SIZE)
				_, err = file.Read(info3)
				if err != nil {
					break
				}

				byteData = append(byteData, info3...)

				key_size := binary.LittleEndian.Uint64(info3)

				// cita bajtove podatka, odnosno key data
				data := make([]byte, key_size)
				_, err = file.Read(data)
				if err != nil {
					break
				}

				byteData = append(byteData, data...)
				retVal = append(retVal, byteData)

				position += int64(VALUE_SIZE_START) + int64(key_size)

				continue
			}
			//ako je tombstone 0 onda citaj sve
			info2 := make([]byte, KEY_START-KEY_SIZE_START)
			_, err = file.Read(info2)
			if err != nil {
				break
			}

			byteData = append(byteData, info2...)

			key_size := binary.LittleEndian.Uint64(info2[:KEY_SIZE_SIZE])
			value_size := binary.LittleEndian.Uint64(info2[KEY_SIZE_SIZE:])

			// cita bajtove podatka, odnosno key i value u data
			//| Key | Value |
			data := make([]byte, key_size+value_size)
			_, err = file.Read(data)
			if err != nil {
				break
			}

			byteData = append(byteData, data...)

			position += int64(KEY_START) + int64(key_size) + int64(value_size)

		}

		retVal = append(retVal, byteData)
	}

	return retVal

}

func OneFileMerkle(OneFileName string) *MerkleTree.MerkleTree {
	file, _ := os.OpenFile(OneFileName, os.O_RDONLY, 0777)
	file.Seek(3*KEY_SIZE_SIZE, 0)

	bfSizeBytes := make([]byte, KEY_SIZE_SIZE)

	_, _ = file.Read(bfSizeBytes)

	bfSize := binary.LittleEndian.Uint64(bfSizeBytes)

	file.Seek(int64(bfSize), 1)

	merkleSizeBytes := make([]byte, KEY_SIZE_SIZE)
	_, _ = file.Read(merkleSizeBytes)

	merkleSize := binary.LittleEndian.Uint64(merkleSizeBytes)

	merkleBytes := make([]byte, merkleSize)
	_, _ = file.Read(merkleBytes)

	mt := MerkleTree.MerkleTree{}

	err := mt.FromBytes(merkleBytes)

	if err != nil {
		return nil
	}

	return &mt
}

func GetFromOneFile(key string, FileName string, comp bool, dict *map[int]string) ([]byte, bool) {

	//iz summary citamo opseg kljuceva u sstable (prvi i poslendji)
	file, _ := os.OpenFile(FileName, os.O_RDONLY, 0777)
	file.Seek(0, 0)
	summaryOffsetBytes := make([]byte, KEY_SIZE_SIZE)
	indexOffsetBytes := make([]byte, KEY_SIZE_SIZE)

	_, _ = file.Read(summaryOffsetBytes)
	_, _ = file.Read(indexOffsetBytes)

	summaryOffset := binary.LittleEndian.Uint64(summaryOffsetBytes)
	indexOffset := binary.LittleEndian.Uint64(indexOffsetBytes)

	//skace na summary deo
	file.Seek(int64(summaryOffset), 0)
	summary := LoadSummary(file)
	// ako je trazeni kljuc u tom opsegu, podatak bi trebalo da se nalazi u ovom sstable
	if summary.FirstKey <= key && key <= summary.LastKey {

		var indexPosition = indexOffset
		for {
			//citamo velicinu kljucax
			keySizeBytes := make([]byte, KEY_SIZE_SIZE)
			_, err := file.Read(keySizeBytes)
			if err == io.EOF {
				dataPosition := findInIndexInOneFile(indexPosition, summaryOffset, key, file)
				notFound := -1
				if dataPosition == uint64(notFound) {
					fmt.Println("sstable: Nije pronadjen key u indexFile")
					file.Close()
					break
				}
				_, data, _, del := ReadDataOneFile(int64(dataPosition), int64(indexOffset), FileName, key, comp, dict)
				return data, del
			}

			keySize := int64(binary.LittleEndian.Uint64(keySizeBytes))

			//citamo keySize bajtiva da bi dobili kljuc
			keyValue := make([]byte, keySize)
			_, err = file.Read(keyValue)

			if err != nil {
				panic(err)
			}

			if string(keyValue) > key {
				dataPosition := findInIndexInOneFile(indexPosition, summaryOffset, key, file)
				notFound := -1
				if dataPosition == uint64(notFound) {
					fmt.Println("sstable: Nije pronadjen key")
					break
				}
				_, data, _, del := ReadDataOneFile(int64(dataPosition), int64(indexOffset), FileName, key, comp, dict)
				return data, del
			} else {
				// citanje pozicije za taj kljuc u indexFile
				positionBytes := make([]byte, KEY_SIZE_SIZE)
				_, err = file.Read(positionBytes)
				position := binary.LittleEndian.Uint64(positionBytes)
				indexPosition = position
				if err != nil {
					if err == io.EOF {
						dataPosition := findInIndexInOneFile(indexPosition, summaryOffset, key, file)
						notFound := -1
						if dataPosition == uint64(notFound) {
							fmt.Println("sstable: Nije pronadjen key u indexFile")
							file.Close()
							break
						}
						_, data, _, del := ReadDataOneFile(int64(dataPosition), int64(indexOffset), FileName, key, comp, dict)
						return data, del
					}
					fmt.Println(err)
					file.Close()
					return []byte{}, false
				}
				continue
			}
		}
	}
	return []byte{}, false
}

// preko kljuca trazimo u summaryFile poziciju za nas klju u indexFile iz kog kasnije dobijamo poziicju naseg kljuca i vrednosti u dataFile
func Get(key string, SummaryFileName string, IndexFileName string, DataFileName string, comp bool, dict *map[int]string) ([]byte, bool) {

	//iz summary citamo opseg kljuceva u sstable (prvi i poslendji)
	sumarryFile, _ := os.OpenFile(SummaryFileName, os.O_RDWR, 0777)
	sumarryFile.Seek(0, 0)
	summary := LoadSummary(sumarryFile)
	defer sumarryFile.Close()

	// ako je trazeni kljuc u tom opsegu, podatak bi trebalo da se nalazi u ovom sstable
	if summary.FirstKey <= key && key <= summary.LastKey {
		var indexPosition = uint64(0)
		for {
			//citamo velicinu kljuca
			keySizeBytes := make([]byte, KEY_SIZE_SIZE)
			_, err := sumarryFile.Read(keySizeBytes)

			if err == io.EOF {
				dataPosition := findInIndex(indexPosition, key, IndexFileName)
				notFound := -1
				if dataPosition == uint64(notFound) {
					fmt.Println("sstable: Nije pronadjen key u indexFile")
					sumarryFile.Close()
					break
				}
				_, data, _, del := ReadData(int64(dataPosition), DataFileName, key, comp, dict)
				return data, del
			}
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
					fmt.Println("sstable: Nije pronadjen key")
					break
				}
				_, data, _, del := ReadData(int64(dataPosition), DataFileName, key, comp, dict)
				return data, del
			} else {
				// citanje pozicije za taj kljuc u indexFile
				positionBytes := make([]byte, KEY_SIZE_SIZE)
				_, err = sumarryFile.Read(positionBytes)
				position := binary.LittleEndian.Uint64(positionBytes)
				indexPosition = position
				if err != nil {
					if err == io.EOF {
						dataPosition := findInIndex(indexPosition, key, IndexFileName)
						notFound := -1
						if dataPosition == uint64(notFound) {
							fmt.Println("sstable: Nije pronadjen key u indexFile")
							sumarryFile.Close()
							break
						}
						_, data, _, del := ReadData(int64(dataPosition), DataFileName, key, comp, dict)
						return data, del
					}
					fmt.Println(err)
					sumarryFile.Close()
					return []byte{}, false
				}
				continue
			}
		}
	}
	return []byte{}, false
}

func findInIndexInOneFile(startPosition uint64, endPosition uint64, key string, file *os.File) uint64 {
	// od date pozicije citamo
	_, err := file.Seek(int64(startPosition), 0)
	if err != nil {
		log.Fatal(err)
	}
	offset, err := file.Seek(0, io.SeekCurrent)
	var lastPos int64 = -1
	for {
		currentKey, position := ReadFromIndex(file)
		if position == -1 {
			if lastPos == -1 {
				notFound := -1
				return uint64(notFound)
			}

			return uint64(lastPos)
		}
		offset, err = file.Seek(0, io.SeekCurrent)

		if currentKey == key {
			return uint64(position)
		}

		if currentKey > key { // valjda nece nikada biti da je lastPos prazan?
			if lastPos == -1 {
				notFound := -1
				return uint64(notFound)
			}
			return uint64(lastPos)
		}
		lastPos = position
		if offset == int64(endPosition) {
			return uint64(lastPos)
		}

		//lastPos = position
	}
}

func ScanIndexInOneFile(startPosition uint64, endPosition uint64, key string, file *os.File) int64 {
	// od date pozicije citamo
	_, err := file.Seek(int64(startPosition), 0)
	if err != nil {
		log.Fatal(err)
	}
	offset, err := file.Seek(0, io.SeekCurrent)

	var lastPos int64 = -1
	for {
		currentKey, position := ReadFromIndex(file)
		offset, err = file.Seek(0, io.SeekCurrent)
		if offset >= int64(endPosition) {
			return int64(lastPos)
		}

		if currentKey > key { // valjda nece nikada biti da je lastPos prazan?
			if lastPos == -1 {
				notFound := -1
				return int64(notFound)
			}
			return int64(lastPos)
		}

		lastPos = position
	}

	return int64(-1)
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
	var lastPos int64 = -1
	for {
		currentKey, position := ReadFromIndex(file)
		if position == -1 {
			if lastPos == -1 {
				notFound := -1
				return uint64(notFound)
			}

			return uint64(lastPos)
		}
		if currentKey == key {
			return uint64(position)
		}

		if currentKey > key { // valjda nece nikada biti da je lastPos prazan?
			if lastPos == -1 {
				notFound := -1
				return uint64(notFound)
			}
			return uint64(lastPos)
		}

		lastPos = position
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
		if err == io.EOF {
			return "", -1
		}
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

func ReadAnyData(position int64, DataFileName string, comp bool, dict1 *map[int]string) ([]byte, []byte, bool, bool) {
	file, err := os.OpenFile(DataFileName, os.O_RDWR, 0777)
	if err != nil {
		return []byte{}, []byte{}, false, false
	}
	defer file.Close()
	// pomeramo se na poziciju u dataFile gde je nas podatak
	_, err = file.Seek(position, 0)
	if err != nil {
		return []byte{}, []byte{}, false, false
	}

	if comp {
		info := make([]byte, KEY_SIZE_START)
		_, err = file.Read(info)
		if err != nil && err != io.EOF {
			return []byte{}, []byte{}, true, false
		}

		num, n := binary.Uvarint(info[TIMESTAMP_START:])

		tombstone := info[TIMESTAMP_START+n]

		if tombstone == 1 {
			info = make([]byte, 10)

			position += int64(TIMESTAMP_START + n + 1)
			_, err = file.Seek(position, 0)
			if err != nil {
				return []byte{}, []byte{}, false, true
			}

			_, err = file.Read(info)
			if err != nil {
				return []byte{}, []byte{}, true, true
			}

			num, _ := binary.Uvarint(info)
			key := (*dict1)[int(num)]

			return []byte(key), []byte{}, false, true
		}

		position += int64(TIMESTAMP_START + n + 1)
		_, err = file.Seek(position, 0)
		if err != nil {
			return []byte{}, []byte{}, false, false
		}

		info = make([]byte, VALUE_SIZE_SIZE)

		_, err = file.Read(info)
		if err != nil {
			return []byte{}, []byte{}, true, false
		}

		num, n = binary.Uvarint(info)
		value_size := num

		position += int64(n)
		_, err = file.Seek(position, 0)
		if err != nil {
			return []byte{}, []byte{}, false, false
		}

		info = make([]byte, 10)

		_, err = file.Read(info)
		if err != nil {
			return []byte{}, []byte{}, true, false
		}

		num, n = binary.Uvarint(info)
		key := (*dict1)[int(num)]

		position += int64(n)
		_, err = file.Seek(position, 0)
		if err != nil {
			return []byte{}, []byte{}, false, false
		}

		data := make([]byte, value_size)
		_, err = file.Read(data)
		if err != nil {
			return []byte{}, []byte{}, false, false // mozda neka prijava gresaka npr za kraj fajla?
		}

		return []byte(key), data, false, false

	} else {
		// cita bajtove podatka DO key i value u info
		// CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B)
		info := make([]byte, KEY_SIZE_START)
		_, err = file.Read(info)
		if err != nil && err != io.EOF {
			return []byte{}, []byte{}, true, false // mozda neka prijava gresaka npr za kraj fajla?
		}

		tombstone := info[TOMBSTONE_START] // jel ovo sad prepoznaje obrisane

		//ako je tombstone 1 ne citaj dalje
		if tombstone == 1 {
			info3 := make([]byte, KEY_SIZE_SIZE)
			_, err = file.Read(info3)
			if err != nil {
				return []byte{}, []byte{}, false, true // mozda neka prijava gresaka npr za kraj fajla?
			}

			key_size := binary.LittleEndian.Uint64(info3)

			// cita bajtove podatka, odnosno key data
			data := make([]byte, key_size)
			_, err = file.Read(data)
			if err != nil {
				return []byte{}, []byte{}, false, true // mozda neka prijava gresaka npr za kraj fajla?
			}

			return []byte(data), []byte{}, false, true
		}
		//ako je tombstone 0 onda citaj sve
		info2 := make([]byte, KEY_START-KEY_SIZE_START)
		_, err = file.Read(info2)
		if err != nil {
			return []byte{}, []byte{}, false, false // mozda neka prijava gresaka npr za kraj fajla?
		}

		key_size := binary.LittleEndian.Uint64(info2[:KEY_SIZE_SIZE])
		value_size := binary.LittleEndian.Uint64(info2[KEY_SIZE_SIZE:])

		// cita bajtove podatka, odnosno key i value u data
		//| Key | Value |
		data := make([]byte, key_size+value_size)
		_, err = file.Read(data)
		if err != nil {
			return []byte{}, []byte{}, false, false // mozda neka prijava gresaka npr za kraj fajla?
		}
		key := data[:key_size]
		val := data[key_size : key_size+value_size]
		return key, val, false, false
	}
}

func ReadDataOneFile(position int64, endPosition int64, DataFileName string, realKey string, comp bool, dict1 *map[int]string) ([]byte, []byte, bool, bool) {
	file, err := os.OpenFile(DataFileName, os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(err)
		return []byte{}, []byte{}, false, false
	}
	defer file.Close()
	// pomeramo se na poziciju u dataFile gde je nas podatak
	_, err = file.Seek(position, 0)
	if err != nil {
		log.Fatal(err)
		return []byte{}, []byte{}, false, false
	}

	for {

		if comp {
			_, err = file.Seek(position, 0)
			if err != nil {
				log.Fatal(err)
				return []byte{}, []byte{}, false, false
			}

			currentOffset, err := file.Seek(0, io.SeekCurrent)
			if err != nil {
				fmt.Println("Error getting file offset:", err)
			}
			if endPosition == currentOffset {
				return []byte{}, []byte{}, true, false // mozda neka prijava gresaka npr za kraj fajla?
			}

			info := make([]byte, KEY_SIZE_START)
			_, err = file.Read(info)
			if err != nil {
				return []byte{}, []byte{}, true, false
			}

			num, n := binary.Uvarint(info[TIMESTAMP_START:])

			tombstone := info[TIMESTAMP_START+n]

			if tombstone == 1 {
				info = make([]byte, 10)

				position += int64(TIMESTAMP_START + n + 1)
				_, err = file.Seek(position, 0)
				if err != nil {
					log.Fatal(err)
					return []byte{}, []byte{}, false, true
				}

				_, err = file.Read(info)
				if err != nil {
					return []byte{}, []byte{}, true, true
				}

				num, n := binary.Uvarint(info)
				position += int64(n)
				key := (*dict1)[int(num)]

				if key >= realKey {
					return []byte{}, []byte{}, false, true
				}

				continue
			}

			position += int64(TIMESTAMP_START + n + 1)
			_, err = file.Seek(position, 0)
			if err != nil {
				log.Fatal(err)
				return []byte{}, []byte{}, false, false
			}

			info = make([]byte, VALUE_SIZE_SIZE)

			_, err = file.Read(info)
			if err != nil {
				return []byte{}, []byte{}, true, false
			}

			num, n = binary.Uvarint(info)
			value_size := num

			position += int64(n)
			_, err = file.Seek(position, 0)
			if err != nil {
				log.Fatal(err)
				return []byte{}, []byte{}, false, false
			}

			info = make([]byte, 10)

			_, err = file.Read(info)
			if err != nil {
				return []byte{}, []byte{}, true, false
			}

			num, n = binary.Uvarint(info)
			key := (*dict1)[int(num)]

			position += int64(n)
			_, err = file.Seek(position, 0)
			if err != nil {
				log.Fatal(err)
				return []byte{}, []byte{}, false, false
			}

			data := make([]byte, value_size)
			_, err = file.Read(data)
			if err != nil {
				return []byte{}, []byte{}, false, false
			}

			if key == realKey {
				return []byte(key), data, false, false
			} else if realKey < key {
				return []byte{}, []byte{}, false, false
			}

			position += int64(value_size)

		} else {
			currentOffset, err := file.Seek(0, io.SeekCurrent)
			if err != nil {
				fmt.Println("Error getting file offset:", err)
			}
			if endPosition == currentOffset {
				return []byte{}, []byte{}, true, false // mozda neka prijava gresaka npr za kraj fajla?
			}
			// cita bajtove podatka DO key i value u info
			// CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B)
			info := make([]byte, KEY_SIZE_START)
			_, err = file.Read(info)
			if err != nil {
				if err == io.EOF {
					return []byte{}, []byte{}, true, false // mozda neka prijava gresaka npr za kraj fajla?
				}

				return []byte{}, []byte{}, true, false
			}

			tombstone := info[TOMBSTONE_START] // jel ovo sad prepoznaje obrisane

			//ako je tombstone 1 ne citaj dalje
			if tombstone == 1 {
				info3 := make([]byte, KEY_SIZE_SIZE)
				_, err = file.Read(info3)
				if err != nil {
					return []byte{}, []byte{}, false, false // mozda neka prijava gresaka npr za kraj fajla?
				}

				key_size := binary.LittleEndian.Uint64(info3)

				// cita bajtove podatka, odnosno key data
				data := make([]byte, key_size)
				_, err = file.Read(data)
				if err != nil {
					return []byte{}, []byte{}, false, false // mozda neka prijava gresaka npr za kraj fajla?
				}
				key := data[:key_size]

				if string(key) < realKey {
					return []byte{}, []byte{}, false, false
				}
				if string(key) == realKey {
					return []byte{}, []byte{}, false, true
				}
				continue
			}
			//ako je tombstone 0 onda citaj sve
			info2 := make([]byte, KEY_START-KEY_SIZE_START)
			_, err = file.Read(info2)
			if err != nil {
				return []byte{}, []byte{}, false, false // mozda neka prijava gresaka npr za kraj fajla?
			}

			key_size := binary.LittleEndian.Uint64(info2[:KEY_SIZE_SIZE])
			value_size := binary.LittleEndian.Uint64(info2[KEY_SIZE_SIZE:])

			// cita bajtove podatka, odnosno key i value u data
			//| Key | Value |
			data := make([]byte, key_size+value_size)
			_, err = file.Read(data)
			if err != nil {
				return []byte{}, []byte{}, false, false // mozda neka prijava gresaka npr za kraj fajla?
			}
			key := data[:key_size]
			val := data[key_size : key_size+value_size]

			if string(key) == realKey {
				return key, val, false, false
			} else if realKey < string(key) {
				return []byte{}, []byte{}, false, false
			}
		}
	}
}

func ReadData(position int64, DataFileName string, realKey string, comp bool, dict1 *map[int]string) ([]byte, []byte, bool, bool) {
	file, err := os.OpenFile(DataFileName, os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(err)
		return []byte{}, []byte{}, false, false
	}
	defer file.Close()
	// pomeramo se na poziciju u dataFile gde je nas podatak
	_, err = file.Seek(position, 0)
	if err != nil {
		log.Fatal(err)
		return []byte{}, []byte{}, false, false
	}

	for {
		if comp {
			_, err = file.Seek(position, 0)
			if err != nil {
				log.Fatal(err)
				return []byte{}, []byte{}, false, false
			}

			info := make([]byte, KEY_SIZE_START)
			_, err = file.Read(info)
			if err != nil {
				return []byte{}, []byte{}, true, false
			}

			num, n := binary.Uvarint(info[TIMESTAMP_START:])

			tombstone := info[TIMESTAMP_START+n]

			if tombstone == 1 {
				info = make([]byte, 10)

				position += int64(TIMESTAMP_START + n + 1)
				_, err = file.Seek(position, 0)
				if err != nil {
					log.Fatal(err)
					return []byte{}, []byte{}, false, true
				}

				_, err = file.Read(info)
				if err != nil {
					return []byte{}, []byte{}, true, true
				}

				num, n := binary.Uvarint(info)
				position += int64(n)
				key := (*dict1)[int(num)]

				if key >= realKey {
					return []byte{}, []byte{}, false, true
				}

				continue
			}

			position += int64(TIMESTAMP_START + n + 1)
			_, err = file.Seek(position, 0)
			if err != nil {
				log.Fatal(err)
				return []byte{}, []byte{}, false, false
			}

			info = make([]byte, VALUE_SIZE_SIZE)

			_, err = file.Read(info)
			if err != nil {
				return []byte{}, []byte{}, true, false
			}

			num, n = binary.Uvarint(info)
			value_size := num

			position += int64(n)
			_, err = file.Seek(position, 0)
			if err != nil {
				log.Fatal(err)
				return []byte{}, []byte{}, false, false
			}

			info = make([]byte, 10)

			_, err = file.Read(info)
			if err != nil {
				return []byte{}, []byte{}, true, false
			}

			num, n = binary.Uvarint(info)
			key := (*dict1)[int(num)]

			position += int64(n)
			_, err = file.Seek(position, 0)
			if err != nil {
				log.Fatal(err)
				return []byte{}, []byte{}, false, false
			}

			data := make([]byte, value_size)
			_, err = file.Read(data)
			if err != nil {
				return []byte{}, []byte{}, false, false
			}

			if key == realKey {
				return []byte(key), data, false, false
			} else if realKey < key {
				return []byte{}, []byte{}, false, false
			}

			position += int64(value_size)

		} else {
			// cita bajtove podatka DO key i value u info
			// CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B)
			info := make([]byte, KEY_SIZE_START)
			_, err = file.Read(info)
			if err != nil {
				if err == io.EOF {
					return []byte{}, []byte{}, true, false // mozda neka prijava gresaka npr za kraj fajla?
				}

				return []byte{}, []byte{}, false, false
			}

			tombstone := info[TOMBSTONE_START] // jel ovo sad prepoznaje obrisane

			//ako je tombstone 1 ne citaj dalje
			if tombstone == 1 {
				info3 := make([]byte, KEY_SIZE_SIZE)
				_, err = file.Read(info3)
				if err != nil {
					return []byte{}, []byte{}, false, true // mozda neka prijava gresaka npr za kraj fajla?
				}

				key_size := binary.LittleEndian.Uint64(info3)

				// cita bajtove podatka, odnosno key data
				data := make([]byte, key_size)
				_, err = file.Read(data)
				if err != nil {
					return []byte{}, []byte{}, false, true // mozda neka prijava gresaka npr za kraj fajla?
				}
				key := data[:key_size]

				if string(key) >= realKey {
					return []byte{}, []byte{}, false, true
				}

				continue
			}
			//ako je tombstone 0 onda citaj sve
			info2 := make([]byte, KEY_START-KEY_SIZE_START)
			_, err = file.Read(info2)
			if err != nil {
				return []byte{}, []byte{}, false, false // mozda neka prijava gresaka npr za kraj fajla?
			}

			key_size := binary.LittleEndian.Uint64(info2[:KEY_SIZE_SIZE])
			value_size := binary.LittleEndian.Uint64(info2[KEY_SIZE_SIZE:])

			// cita bajtove podatka, odnosno key i value u data
			//| Key | Value |
			data := make([]byte, key_size+value_size)
			_, err = file.Read(data)
			if err != nil {
				return []byte{}, []byte{}, false, false // mozda neka prijava gresaka npr za kraj fajla?
			}
			key := data[:key_size]
			val := data[key_size : key_size+value_size]

			if string(key) == realKey {
				return key, val, false, false
			} else if realKey < string(key) {
				return []byte{}, []byte{}, false, false
			}

		}

	}
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// funkcija koja pretvara node tj entry u bajtove
func NodeToBytes(node Config.Entry, comp bool, dict1 *map[int]string, dict2 *map[string]int) []byte { //pretvara node u bajtove
	var data []byte

	crcb := make([]byte, CRC_SIZE)
	binary.LittleEndian.PutUint32(crcb, node.Crc)
	data = append(data, crcb...) //dodaje se CRC

	if comp {
		buf := make([]byte, TIMESTAMP_SIZE)
		n := binary.PutUvarint(buf, uint64(node.Timestamp))
		data = append(data, buf[:n]...)

		//1 - deleted; 0 - not deleted
		//dodaje se Tombstone
		if node.Tombstone {
			//ako je tombstone 1 onda bez value size i value
			var delb byte = 1
			data = append(data, delb)

			val, ok := (*dict2)[node.Transaction.Key]

			if ok {
				buf = make([]byte, 10)
				n := binary.PutUvarint(buf, uint64(val))
				data = append(data, buf[:n]...)

			} else {
				val = len(*dict1)
				(*dict1)[len(*dict1)] = node.Transaction.Key
				(*dict2)[node.Transaction.Key] = val

				buf = make([]byte, 10)
				n := binary.PutUvarint(buf, uint64(val))
				data = append(data, buf[:n]...)

			}

		} else {
			// zapisme tomb, pa valsize, pa key, pa value
			var delb byte = 0
			data = append(data, delb)

			valuebs := make([]byte, VALUE_SIZE_SIZE)
			n := binary.PutUvarint(valuebs, uint64(len(node.Transaction.Value)))

			data = append(data, valuebs[:n]...)

			val, ok := (*dict2)[node.Transaction.Key]

			if ok {
				buf = make([]byte, 10)
				n := binary.PutUvarint(buf, uint64(val))
				data = append(data, buf[:n]...)

			} else {
				val = len(*dict1)
				(*dict1)[len(*dict1)] = node.Transaction.Key
				(*dict2)[node.Transaction.Key] = val

				buf = make([]byte, 10)
				n := binary.PutUvarint(buf, uint64(val))
				data = append(data, buf[:n]...)

			}

			data = append(data, node.Transaction.Value...)

		}

	} else {
		sec := node.Timestamp
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

	}

	return data
}

func MakeDataOneFile(nodes []*Config.Entry, FileName string, dil_s int, dil_i int, comp bool, dict1 *map[int]string, dict2 *map[string]int) error {
	file, err := os.OpenFile(FileName, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	//zauzimanje prostoa za offsete koji ce pokazivati na pocetak summary,index i data dela
	summaryOffsetSize := make([]byte, KEY_SIZE_SIZE)
	indexOffsetSize := make([]byte, KEY_SIZE_SIZE)
	dataOffsetSize := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(dataOffsetSize, uint64(1))
	binary.LittleEndian.PutUint64(indexOffsetSize, uint64(1))
	binary.LittleEndian.PutUint64(summaryOffsetSize, uint64(1))
	file.Write(summaryOffsetSize)
	file.Write(indexOffsetSize)
	file.Write(dataOffsetSize)

	//bloomfilter
	bfsize := make([]byte, KEY_SIZE_SIZE)
	bf := BloomFilter.BloomFilter{}
	bf.Init(len(nodes), 0.01)

	for _, val := range nodes {
		bf.Add([]byte(val.Transaction.Key))
	}
	//serijalizacija
	bfbytes, err := bf.ToBytes()
	if err != nil {
		return err
	}
	//upis za bf
	binary.LittleEndian.PutUint64(bfsize, uint64(len(bfbytes)))
	file.Write(bfsize)
	file.Write(bfbytes)
	//merkle
	var data [][]byte

	for _, entry := range nodes {
		bytesData := NodeToBytes(*entry, comp, dict1, dict2)
		data = append(data, bytesData)
	}

	mt := MerkleTree.MerkleTree{}
	mt.Init(data)

	//FALI SERIJALIZACIJA ZA MERKLE
	// trebalo bi da radi?? - nije isprobano
	merkleBytes, err := mt.ToBytes()
	if err != nil {
		return err
	}
	merkleSize := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(merkleSize, uint64(len(merkleBytes)))
	file.Write(merkleSize)
	file.Write(merkleBytes)

	//DATA
	dataOffset, err := FileLength(file)

	var offsetList []int64
	//upisivanje data dela
	for _, node := range nodes {
		position, _ := FileLength(file)
		offsetList = append(offsetList, position)
		// cvor se upisuje u Data deo fajla
		_, err = file.Write(NodeToBytes(*node, comp, dict1, dict2))
		if err != nil {
			return err
		}
	}

	//PRAVLJENJE INDEX DELA
	var offsetIndexList []int64
	indexOffset, err := FileLength(file)
	for i, el := range offsetList {
		if i%dil_i == 0 {
			indexOff := AddToIndex(el, nodes[i].Transaction.Key, file)
			offsetIndexList = append(offsetIndexList, indexOff)
		}
	}
	//PRAVLJENJE SUMMARY DELA
	summaryOffset, err := FileLength(file)

	// uzima najmanji i najveci kljuc iz nodes iz memtable
	first := make([]byte, KEY_SIZE_SIZE)
	last := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(first, uint64(len([]byte(nodes[0].Transaction.Key))))
	binary.LittleEndian.PutUint64(last, uint64(len([]byte(nodes[len(nodes)-1].Transaction.Key))))
	// upisuje najmanji i najveci kljuc na pocetak summary dela
	file.Write(first)
	file.Write(last)
	file.Write([]byte(nodes[0].Transaction.Key))
	file.Write([]byte(nodes[len(nodes)-1].Transaction.Key))
	for i, el := range offsetIndexList {
		if i%dil_s == 0 {
			AddToSummary(el, nodes[i*dil_i].Transaction.Key, file)
		}
	}

	//UPISIVANJE OFFSETA DATA,SUMMARY I INDEX DELA FAJLA

	summaryOffsetSize = make([]byte, KEY_SIZE_SIZE)
	indexOffsetSize = make([]byte, KEY_SIZE_SIZE)
	dataOffsetSize = make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(dataOffsetSize, uint64(dataOffset))
	binary.LittleEndian.PutUint64(indexOffsetSize, uint64(indexOffset))
	binary.LittleEndian.PutUint64(summaryOffsetSize, uint64(summaryOffset))

	file.Seek(0, 0)
	_, err = file.WriteAt(summaryOffsetSize, 0)
	_, err = file.WriteAt(indexOffsetSize, KEY_SIZE_SIZE)
	_, err = file.WriteAt(dataOffsetSize, 2*KEY_SIZE_SIZE)
	if err != nil {
		return err
	}

	return nil

}

func MakeData(nodes []*Config.Entry, DataFileName string, IndexFileName string, SummaryFileName string, BloomFileName string, MerkleFileName string, dil_sum int, dil_ind int, comp bool, dict1 *map[int]string, dict2 *map[string]int) {
	indexFile, err := os.OpenFile(IndexFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}
	defer indexFile.Close()

	summaryFile, err := os.OpenFile(SummaryFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0777)
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

	// pravi se merkle tree
	// flag: Tamara

	//fmt.Print("\n\n", MerkleFileName, "\n\n")

	make_merkle(nodes, MerkleFileName, comp, dict1, dict2)

	file, err := os.OpenFile(DataFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var n = 0 // krece se od 0 da bi prvi kljuc u summaryFile bio prvi i u indexFile
	var n2 = -1
	for _, node := range nodes {
		position, _ := FileLength(file)
		// cvor se upisuje u DataFile
		_, err = file.Write(NodeToBytes(*node, comp, dict1, dict2))
		if err != nil {
			return
		}
		// upisivanje u index fajl
		if n%dil_ind == 0 {
			positionSum := AddToIndex(position, node.Transaction.Key, indexFile)
			n2++

			// upisuje svaki peti u summary file
			if n2%dil_sum == 0 {
				AddToSummary(positionSum, node.Transaction.Key, summaryFile)
			}
		}

		n += 1
	}
	err = file.Sync()
	if err != nil {
		return
	}
}

// sizeTierdCompaction
func SizeTieredCompaction(lsm *Config.LSMTree, dil_s int, dil_i int, oneFile bool, comp bool, dict1 *map[int]string, dict2 *map[string]int) {
	if lsm.Levels[0] == lsm.MaxSSTables {
		mergeAll(1, lsm, dil_s, dil_i, comp, dict1, dict2, oneFile)
	}
	/*if lsm.Levels[0] == lsm.MaxSSTables {
		if oneFile {
			mergeOneFile(1, lsm, dil_s, dil_i, comp, dict1, dict2)
		} else {
			merge(1, lsm, dil_s, dil_i, comp, dict1, dict2)
		}
	}*/
}

func mergeAll(level int, lsm *Config.LSMTree, dil_s int, dil_i int, comp bool, dict1 *map[int]string, dict2 *map[string]int, oneFile bool) {
	br := lsm.Levels[level] + 1
	var fileList []*os.File
	if oneFile {
		sstableFile, _ := os.Create("files_SSTable/oneFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".db")
		lsm.OneFilesNames = append([]string{sstableFile.Name()}, lsm.OneFilesNames...)
		fileList = append(fileList, sstableFile)
	} else {
		dataFile, _ := os.Create("files_SSTable/dataFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".db")
		indexFile, _ := os.Create("files_SSTable/indexFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".db")
		summaryFile, _ := os.Create("files_SSTable/summaryFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".db")
		bloomFile, _ := os.Create("files_SSTable/bloomFilterFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".db")
		merkleFile, _ := os.Create("files_SSTable/merkleTreeFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".db")

		lsm.DataFilesNames = append([]string{dataFile.Name()}, lsm.DataFilesNames...)
		lsm.IndexFilesNames = append([]string{indexFile.Name()}, lsm.IndexFilesNames...)
		lsm.SummaryFilesNames = append([]string{summaryFile.Name()}, lsm.SummaryFilesNames...)
		lsm.BloomFilterFilesNames = append([]string{bloomFile.Name()}, lsm.BloomFilterFilesNames...)
		lsm.MerkleTreeFilesNames = append([]string{merkleFile.Name()}, lsm.MerkleTreeFilesNames...)

		fileList = append(fileList, dataFile)
		fileList = append(fileList, indexFile)
		fileList = append(fileList, summaryFile)
		fileList = append(fileList, bloomFile)
		fileList = append(fileList, merkleFile)
	}
	mergeAllFiles(fileList, level, lsm, dil_s, dil_i, comp, dict1, dict2, oneFile)
	lsm.Levels[level-1] = 0
	lsm.Levels[level]++
	if lsm.Levels[level] == lsm.MaxSSTables && level != lsm.CountOfLevels-1 { // proverava broj fajlova na sledećem nivou, i ne treba da pozove merge ako je na 3. nivou tj ako je nivo 2
		mergeAll(level+1, lsm, dil_s, dil_i, comp, dict1, dict2, oneFile)
	}
}

func mergeAllFiles(lista []*os.File, level int, lsm *Config.LSMTree, dil_s int, dil_i int, comp bool, dict1 *map[int]string, dict2 *map[string]int, oneFile bool) {
	levelSubstring1 := "files_SSTable/oneFile_" + strconv.Itoa(level) + "_"
	oneFileNames := levelFileNames(lsm.OneFilesNames, levelSubstring1)
	oneLevelFiles, err1 := openFiles(oneFileNames)
	if err1 != nil {
		//ako ima greske pri otvaranju nekog fajla
		panic(err1)
	}
	levelSubstring2 := "files_SSTable/dataFile_" + strconv.Itoa(level) + "_"
	levelFileNames := levelFileNames(lsm.DataFilesNames, levelSubstring2)
	levelFiles, err2 := openFiles(levelFileNames)
	if err2 != nil {
		//ako ima greske pri otvaranju nekog fajla
		panic(err2)
	}
	var entries []*Config.Entry
	var sortedAllEntries []*Config.Entry
	var eofList []int

	//prolazimo kroz one fajlove i pozicioniramo se na pocetak data segmenta
	for _, file := range oneLevelFiles {
		dataOffsetBytes := make([]byte, KEY_SIZE_SIZE)
		indexOffsetBytes := make([]byte, KEY_SIZE_SIZE)
		//preskace summary offset
		file.Seek(Config.KEY_SIZE_SIZE, 0)

		_, _ = file.Read(indexOffsetBytes)
		_, _ = file.Read(dataOffsetBytes)

		dataOffset := binary.LittleEndian.Uint64(dataOffsetBytes)
		indexOffset := binary.LittleEndian.Uint64(indexOffsetBytes)
		eofList = append(eofList, int(indexOffset))
		file.Seek(int64(dataOffset), 0)
	}
	//cita prve entije iz one fajlova
	for i, file := range oneLevelFiles {
		entry := readMergeOneFile(file, eofList[i], comp, dict1)
		entries = append(entries, entry)
	}
	// cita prve entryje iz obicnih fajlova
	for _, file := range levelFiles {
		entry := readMerge(file, comp, dict1)
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
			if index < len(oneLevelFiles) {
				newEntry := readMergeOneFile(oneLevelFiles[index], eofList[index], comp, dict1)
				entries[index] = newEntry
			} else {
				newEntry := readMerge(levelFiles[index-len(oneLevelFiles)], comp, dict1)
				entries[index] = newEntry
			}
		}

	}

	closeFiles(levelFiles)
	closeFiles(oneLevelFiles)
	//pravljenje novog sstablea od svih sstableova ovog nivoa koji su sada spojeni
	if oneFile {
		MakeDataOneFile(sortedAllEntries, lista[0].Name(), dil_s, dil_i, comp, dict1, dict2)
	} else {
		MakeData(sortedAllEntries, lista[0].Name(), lista[1].Name(), lista[2].Name(), lista[3].Name(), lista[4].Name(), dil_s, dil_i, comp, dict1, dict2)
	}

	for _, path := range levelFileNames {
		err := os.Remove(path)
		if err != nil {
			log.Fatal(err)
		}
		lsm.DataFilesNames = removeFileName(lsm.DataFilesNames, path)

		idxPath := strings.Replace(path, "dataFile", "indexFile", -1)
		err = os.Remove(idxPath)
		if err != nil {
			log.Fatal(err)
		}
		lsm.IndexFilesNames = removeFileName(lsm.IndexFilesNames, idxPath)

		bloomPath := strings.Replace(path, "dataFile", "bloomFilterFile", -1)
		err = os.Remove(bloomPath)
		if err != nil {
			log.Fatal(err)
		}
		lsm.BloomFilterFilesNames = removeFileName(lsm.BloomFilterFilesNames, bloomPath)

		merklePath := strings.Replace(path, "dataFile", "merkleTreeFile", -1)
		err = os.Remove(merklePath)
		if err != nil {
			log.Fatal(err)
		}
		lsm.MerkleTreeFilesNames = removeFileName(lsm.MerkleTreeFilesNames, merklePath)

		summaryPath := strings.Replace(path, "dataFile", "summaryFile", -1)
		err = os.Remove(summaryPath)
		if err != nil {
			log.Fatal(err)
		}
		lsm.SummaryFilesNames = removeFileName(lsm.SummaryFilesNames, summaryPath)
	}

	for _, path := range oneFileNames {
		err := os.Remove(path)
		if err != nil {
			log.Fatal(err)
		}
		lsm.OneFilesNames = removeFileName(lsm.OneFilesNames, path)
	}

}

func mergeOneFile(level int, lsm *Config.LSMTree, dil_s int, dil_i int, comp bool, dict1 *map[int]string, dict2 *map[string]int) {
	br := lsm.Levels[level] + 1
	sstableFile, _ := os.Create("files_SSTable/oneFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".db")
	lsm.OneFilesNames = append([]string{sstableFile.Name()}, lsm.OneFilesNames...)
	mergeOneFiles(level, sstableFile, lsm, dil_s, dil_i, comp, dict1, dict2)
	lsm.Levels[level-1] = 0
	lsm.Levels[level]++
	if lsm.Levels[level] == lsm.MaxSSTables && level != lsm.CountOfLevels-1 { // proverava broj fajlova na sledećem nivou, i ne treba da pozove merge ako je na 3. nivou tj ako je nivo 2
		mergeOneFile(level+1, lsm, dil_s, dil_i, comp, dict1, dict2)
	}
}

func mergeOneFiles(level int, sstableFile *os.File, lsm *Config.LSMTree, dil_s int, dil_i int, comp bool, dict1 *map[int]string, dict2 *map[string]int) {

	//otvorimo sve fajlove
	//procitamo prvi podatak (tombstone) iz svakog od njih i njih stvaimo u listu
	//ako je dosao do rkaja fajla vraca nil
	//trazimo min kljuc i njega dodajemo u novu skiplistu
	//ako imamo iste kljuceve onda nadji najnoviji i njega dodaj u skiplist a ostale prekosci

	//uzimamo imena svih data fajlova ovog nivoa
	levelSubstring := "files_SSTable/oneFile_" + strconv.Itoa(level) + "_"
	fileNames := levelFileNames(lsm.OneFilesNames, levelSubstring)
	//otvaranje data fajlova na ovom nivou
	levelFiles, err := openFiles(fileNames)
	if err != nil {
		//ako ima greske pri otvaranju nekog fajla
		panic(err)
	}
	var entries []*Config.Entry
	var sortedAllEntries []*Config.Entry
	//niz koji pamti krajeve data segmenata svih fajlova
	var eofList []int
	//prolazimo kroz dajlove i pozicioniramo se na pocetak data segmenta
	for _, file := range levelFiles {
		dataOffsetBytes := make([]byte, KEY_SIZE_SIZE)
		indexOffsetBytes := make([]byte, KEY_SIZE_SIZE)
		//preskace summary offset
		file.Seek(Config.KEY_SIZE_SIZE, 0)

		_, _ = file.Read(indexOffsetBytes)
		_, _ = file.Read(dataOffsetBytes)

		dataOffset := binary.LittleEndian.Uint64(dataOffsetBytes)
		indexOffset := binary.LittleEndian.Uint64(indexOffsetBytes)
		eofList = append(eofList, int(indexOffset))
		file.Seek(int64(dataOffset), 0)
	}

	//u entries cuvamo trenutne entie na kojim smo iz svakog sstablea sa ovog nivoa
	for i, file := range levelFiles {
		entry := readMergeOneFile(file, eofList[i], comp, dict1)
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
			newEntry := readMergeOneFile(levelFiles[index], eofList[index], comp, dict1)
			entries[index] = newEntry
		}

	}
	closeFiles(levelFiles)

	//pravljenje novog sstablea od svih sstableova ovog nivoa koji su sada spojeni
	MakeDataOneFile(sortedAllEntries, sstableFile.Name(), dil_s, dil_i, comp, dict1, dict2)

	for i := 1; i <= lsm.MaxSSTables; i++ {
		err = os.Remove("files_SSTable/oneFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".db")
		if err != nil {
			log.Fatal(err)
		}

		lsm.OneFilesNames = removeFileName(lsm.OneFilesNames, "files_SSTable/oneFile_"+strconv.Itoa(level)+"_"+strconv.Itoa(i)+".db")
	}
}

// cita iz fajla za merge, vraca procutani entry ili nil ako smo dosli do kraja fajla
func readMergeOneFile(file *os.File, endposition int, comp bool, dict1 *map[int]string) *Config.Entry {
	// cita bajtove podatka DO key i value u info
	// CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B)

	currentOffset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		fmt.Println("Error getting file offset:", err)
		return nil
	}

	if currentOffset == int64(endposition) {
		return nil
	}

	if comp {
		position := currentOffset

		entry := Config.Entry{}

		info := make([]byte, KEY_SIZE_START)
		_, err = file.Read(info)
		if err != nil {
			return nil
		}

		entry.Crc = binary.LittleEndian.Uint32(info[:TIMESTAMP_START])

		num, n := binary.Uvarint(info[TIMESTAMP_START:])

		entry.Timestamp = num

		tombstone := info[TIMESTAMP_START+n]

		if tombstone == 1 {
			info = make([]byte, 10)

			position += int64(TIMESTAMP_START + n + 1)
			_, err = file.Seek(position, 0)
			if err != nil {
				log.Fatal(err)
				return nil
			}

			_, err = file.Read(info)
			if err != nil {
				return nil
			}
			entry.Tombstone = true
			num, n := binary.Uvarint(info)
			key := (*dict1)[int(num)]
			entry.Transaction.Key = key
			position += int64(n)
			_, err = file.Seek(position, 0)
			if err != nil {
				log.Fatal(err)
				return nil
			}

			return &entry
		}

		entry.Tombstone = false

		position += int64(TIMESTAMP_START + n + 1)
		_, err = file.Seek(position, 0)
		if err != nil {
			log.Fatal(err)
			return nil
		}

		info = make([]byte, VALUE_SIZE_SIZE)

		_, err = file.Read(info)
		if err != nil {
			return nil
		}

		num, n = binary.Uvarint(info)
		value_size := num

		position += int64(n)
		_, err = file.Seek(position, 0)
		if err != nil {
			log.Fatal(err)
			return nil
		}

		info = make([]byte, 10)

		_, err = file.Read(info)
		if err != nil {
			return nil
		}

		num, n = binary.Uvarint(info)
		key := (*dict1)[int(num)]

		position += int64(n)
		_, err = file.Seek(position, 0)
		if err != nil {
			log.Fatal(err)
			return nil
		}

		data := make([]byte, value_size)
		_, err = file.Read(data)
		if err != nil {
			return nil
		}

		entry.Transaction.Key = key
		entry.Transaction.Value = data

		return &entry

	} else {
		info := make([]byte, KEY_SIZE_START)
		_, err = file.Read(info)
		if err != nil {
			return nil
		}
		tombstone := info[TOMBSTONE_START:KEY_SIZE_START]
		//ako je tombstone 1 procitaj odmah sledeci

		if tombstone[0] == 1 {
			info2 := make([]byte, KEY_SIZE_SIZE)
			_, err = file.Read(info2)
			if err != nil {
				return nil
			}
			key_size := binary.LittleEndian.Uint64(info2)
			key := make([]byte, key_size)
			_, err = file.Read(key)
			if err != nil {
				return nil
			}
			info = append(info, info2...)
			info3 := make([]byte, VALUE_SIZE_SIZE)
			info = append(info, info3...)
			info = append(info, key...)
			entry := Config.ToEntry(info)
			return &entry
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
}

func merge(level int, lsm *Config.LSMTree, dil_s int, dil_i int, comp bool, dict1 *map[int]string, dict2 *map[string]int) {
	br := lsm.Levels[level] + 1
	dataFile, _ := os.Create("files_SSTable/dataFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".db")
	indexFile, _ := os.Create("files_SSTable/indexFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".db")
	summaryFile, _ := os.Create("files_SSTable/summaryFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".db")
	bloomFile, _ := os.Create("files_SSTable/bloomFilterFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".db")
	merkleFile, _ := os.Create("files_SSTable/merkleTreeFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".db")

	lsm.DataFilesNames = append([]string{dataFile.Name()}, lsm.DataFilesNames...)
	lsm.IndexFilesNames = append([]string{indexFile.Name()}, lsm.IndexFilesNames...)
	lsm.SummaryFilesNames = append([]string{summaryFile.Name()}, lsm.SummaryFilesNames...)
	lsm.BloomFilterFilesNames = append([]string{bloomFile.Name()}, lsm.BloomFilterFilesNames...)
	lsm.MerkleTreeFilesNames = append([]string{merkleFile.Name()}, lsm.MerkleTreeFilesNames...)
	mergeFiles(level, dataFile, indexFile, summaryFile, bloomFile, merkleFile, lsm, dil_s, dil_i, comp, dict1, dict2)
	lsm.Levels[level-1] = 0
	lsm.Levels[level]++
	if lsm.Levels[level] == lsm.MaxSSTables && level != lsm.CountOfLevels { // proverava broj fajlova na sledećem nivou, i ne treba da pozove merge ako je na 3. nivou tj ako je nivo 2
		merge(level+1, lsm, dil_s, dil_i, comp, dict1, dict2)
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
func readMerge(file *os.File, comp bool, dict1 *map[int]string) *Config.Entry {
	if comp {
		position, err := file.Seek(0, 1)
		if err != nil {
			return nil
		}

		entry := Config.Entry{}

		info := make([]byte, KEY_SIZE_START)
		_, err = file.Read(info)
		if err != nil {
			return nil
		}

		entry.Crc = binary.LittleEndian.Uint32(info[:TIMESTAMP_START])

		num, n := binary.Uvarint(info[TIMESTAMP_START:])

		entry.Timestamp = num

		tombstone := info[TIMESTAMP_START+n]

		if tombstone == 1 {
			info = make([]byte, 10)

			position += int64(TIMESTAMP_START + n + 1)
			_, err = file.Seek(position, 0)
			if err != nil {
				log.Fatal(err)
				return nil
			}

			_, err = file.Read(info)
			if err != nil {
				return nil
			}
			entry.Tombstone = true
			num, n := binary.Uvarint(info)
			key := (*dict1)[int(num)]
			entry.Transaction.Key = key
			position += int64(n)
			_, err = file.Seek(position, 0)
			if err != nil {
				log.Fatal(err)
				return nil
			}

			return &entry
		}

		entry.Tombstone = false

		position += int64(TIMESTAMP_START + n + 1)
		_, err = file.Seek(position, 0)
		if err != nil {
			log.Fatal(err)
			return nil
		}

		info = make([]byte, VALUE_SIZE_SIZE)

		_, err = file.Read(info)
		if err != nil {
			return nil
		}

		num, n = binary.Uvarint(info)
		value_size := num

		position += int64(n)
		_, err = file.Seek(position, 0)
		if err != nil {
			log.Fatal(err)
			return nil
		}

		info = make([]byte, 10)

		_, err = file.Read(info)
		if err != nil {
			return nil
		}

		num, n = binary.Uvarint(info)
		key := (*dict1)[int(num)]

		position += int64(n)
		_, err = file.Seek(position, 0)
		if err != nil {
			log.Fatal(err)
			return nil
		}

		data := make([]byte, value_size)
		_, err = file.Read(data)
		if err != nil {
			return nil
		}

		entry.Transaction.Key = key
		entry.Transaction.Value = data

		return &entry

	} else {
		// cita bajtove podatka DO key i value u info
		// CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B)
		info := make([]byte, KEY_SIZE_START)
		_, err := file.Read(info)
		if err != nil {
			return nil
		}
		tombstone := info[TOMBSTONE_START:KEY_SIZE_START]
		//ako je tombstone 1 procitaj odmah sledeci
		if tombstone[0] == 1 {
			info2 := make([]byte, KEY_SIZE_SIZE)
			_, err = file.Read(info2)
			if err != nil {
				return nil
			}
			key_size := binary.LittleEndian.Uint64(info2)
			key := make([]byte, key_size)
			_, err = file.Read(key)
			if err != nil {
				return nil
			}
			info = append(info, info2...)
			info3 := make([]byte, VALUE_SIZE_SIZE)
			info = append(info, info3...)
			info = append(info, key...)
			entry := Config.ToEntry(info)
			return &entry
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

	return nil
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
	var skip = false
	// Iterate through the array and find the entry with the minimum key
	for index, entry := range entries {
		if entry == nil {
			continue
		}
		skip = false
		for _, idx := range minKeyArray {
			if index == idx {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		if entry.Transaction.Key < minKey {
			//menjamo min key i praznimo niz sa indeksima
			minKeyArray = make([]int, 0)
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
func removeFileName(array []string, name string) []string {
	slice1 := []string{}
	slice2 := []string{}
	for i, n := range array {
		if n == name {
			slice1 = append(array[:i], array[i+1:]...)
			break
		}
	}

	return append(slice2, slice1...)
}

func mergeFiles(level int, dataFile *os.File, indexFile *os.File, summaryFile *os.File, bloomFile *os.File, merkleFile *os.File, lsm *Config.LSMTree, dil_s int, dil_i int, comp bool, dict1 *map[int]string, dict2 *map[string]int) {

	//otvorimo sve fajlove
	//procitamo prvi podatak (tombstone) iz svakog od njih i njih stvaimo u listu
	//ako je dosao do rkaja fajla vraca nil
	//trazimo min kljuc i njega dodajemo u novu skiplistu
	//ako imamo iste kljuceve onda nadji najnoviji i njega dodaj u skiplist a ostale prekosci

	//uzimamo imena svih data fajlova ovog nivoa
	levelSubstring := "files_SSTable/dataFile_" + strconv.Itoa(level) + "_"
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
		entry := readMerge(file, comp, dict1)
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
			newEntry := readMerge(levelFiles[index], comp, dict1)
			entries[index] = newEntry
		}

	}
	closeFiles(levelFiles)

	//pravljenje novog sstablea od svih sstableova ovog nivoa koji su sada spojeni
	MakeData(sortedAllEntries, dataFile.Name(), indexFile.Name(), summaryFile.Name(), bloomFile.Name(), merkleFile.Name(), dil_s, dil_i, comp, dict1, dict2)

	for i := 1; i <= lsm.MaxSSTables; i++ {
		err = os.Remove("files_SSTable/dataFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".db")
		if err != nil {
			log.Fatal(err)
		}
		err = os.Remove("files_SSTable/indexFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".db")
		if err != nil {
			log.Fatal(err)
		}
		err = os.Remove("files_SSTable/summaryFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".db")
		if err != nil {
			log.Fatal(err)
		}
		err = os.Remove("files_SSTable/bloomFilterFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".db")
		if err != nil {
			log.Fatal(err)
		}
		err = os.Remove("files_SSTable/merkleTreeFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".db")
		if err != nil {
			log.Fatal(err)
		}

		lsm.DataFilesNames = removeFileName(lsm.DataFilesNames, "files_SSTable/dataFile_"+strconv.Itoa(level)+"_"+strconv.Itoa(i)+".db")
		lsm.IndexFilesNames = removeFileName(lsm.IndexFilesNames, "files_SSTable/indexFile_"+strconv.Itoa(level)+"_"+strconv.Itoa(i)+".db")
		lsm.SummaryFilesNames = removeFileName(lsm.SummaryFilesNames, "files_SSTable/summaryFile_"+strconv.Itoa(level)+"_"+strconv.Itoa(i)+".db")
		lsm.BloomFilterFilesNames = removeFileName(lsm.BloomFilterFilesNames, "files_SSTable/bloomFilterFile_"+strconv.Itoa(level)+"_"+strconv.Itoa(i)+".db")
		lsm.MerkleTreeFilesNames = removeFileName(lsm.MerkleTreeFilesNames, "files_SSTable/merkleTreeFile_"+strconv.Itoa(level)+"_"+strconv.Itoa(i)+".db")
	}
}

//---------------------------LEVEL TIERED COMPACTION--------------------------------
// kod level tiered kompakcije svaki nivo (run) je T puta veci od prethodnog. T je uglavnom 10. Kriterijum za kompakciju ce biti broj tabela po run-u.
// Uzima se tabela iz nivoa na kom se vrsi kompakcija i traze se odgovarajuce tabele u narednom nivou. Spajaju se i nova tabela se dodaje u nizi nivo.
// Imenuju se kao level_brojulevelu.

// utvrditi nivo na kom se kompakcija desava
// znaci kada se flushuje
func LevelTieredCompaction(lsm *Config.LSMTree, dil_s int, dil_i int, oneFile bool, comp bool, dict1 *map[int]string, dict2 *map[string]int) {
	if lsm.Levels[0] > lsm.MaxSSTables { //mislim da je bolje ako je vece jer onda nikada npr u prvom levelu nece imati max tabela nego za jednu manje
		if oneFile {
			levelMergeOneFile(1, lsm, dil_s, dil_i, comp, dict1, dict2)
		} else {
			levelMerge(1, lsm, dil_s, dil_i, comp, dict1, dict2)
		}
	}
}

func levelMergeOneFile(level int, lsm *Config.LSMTree, dil_s int, dil_i int, comp bool, dict1 *map[int]string, dict2 *map[string]int) {

	for {
		br := lsm.Levels[level-1]

		//izabrali smo tabelu na visem nivou
		sstableFile := "files_SSTable/oneFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(br) + ".db"

		//treba dobaviti indekse iz sstableFile iz summarija
		SummaryContent := LoadSummaryOneFile(sstableFile, dil_s, dil_i, comp, dict1, dict2)

		bottomIdx := SummaryContent.FirstKey
		topIdx := SummaryContent.LastKey

		//naci tabele koje su sa sledeceg nivoa

		sstableFiles, entriesAdd := findOtherTablesOneFile(level+1, bottomIdx, topIdx, lsm, dil_s, dil_i, comp, dict1, dict2)

		//sve tabele dodati u neki niz koji ce se proslediti funkciji koja ce da merguje sve

		num := len(sstableFiles)

		sstableFiles = append(sstableFiles, sstableFile)

		levelMergeFilesOneFile(level, sstableFiles, lsm, num, entriesAdd, dil_s, dil_i, comp, dict1, dict2)

		lsm.Levels[level-1]--
		// (dodaj tu jednu iz levela na [level + 1], i oduzmi num merge-ovanih)
		lsm.Levels[level] += 1   // dodaj spojenu koju smo prebacili tu
		lsm.Levels[level] -= num // oduzmi sve koje smo spojili sa tog nivoa

		// ** T: provjeriti da li je okej uslov za level tiered?
		if lsm.Levels[level] > int(float64(lsm.MaxSSTables)*math.Pow(float64(lsm.T), float64(level))) && (level+1 < lsm.CountOfLevels) {
			// proverava broj fajlova na sledećem nivou, i ne treba da pozove merge ako je na 3. nivou tj ako je nivo 2
			level += 1
			continue
		} else {
			break
		}
	}
}

func levelMergeFilesOneFile(level int, sstableFiles []string, lsm *Config.LSMTree, num int, entriesAdd []*Config.Entry, dil_s int, dil_i int, comp bool, dict1 *map[int]string, dict2 *map[string]int) {

	files, err := openFiles(sstableFiles)
	if err != nil {
		panic(err)
	}

	var entries []*Config.Entry

	entries = entriesAdd

	var sortedAllEntries []*Config.Entry
	var eofList []int

	for _, file := range files {
		dataOffsetBytes := make([]byte, KEY_SIZE_SIZE)
		indexOffsetBytes := make([]byte, KEY_SIZE_SIZE)
		//preskace summary offset
		file.Seek(Config.KEY_SIZE_SIZE, 0)

		_, _ = file.Read(indexOffsetBytes)
		_, _ = file.Read(dataOffsetBytes)

		dataOffset := binary.LittleEndian.Uint64(dataOffsetBytes)
		indexOffset := binary.LittleEndian.Uint64(indexOffsetBytes)
		eofList = append(eofList, int(indexOffset))
		file.Seek(int64(dataOffset), 0)
	}

	for i, file := range files {
		entry := readMergeOneFile(file, eofList[i], comp, dict1)
		entries = append(entries, entry)
	}
	for {
		if areAllNil(entries) {
			break
		}
		minKeyArray, minEntry := findMinKeyEntry(entries)
		sortedAllEntries = append(sortedAllEntries, minEntry)
		//citamo naredne entye za fajlove koji su bili na min entry
		for _, index := range minKeyArray {
			newEntry := readMergeOneFile(files[index], eofList[index], comp, dict1)
			entries[index] = newEntry
		}
	}

	closeFiles(files)

	last := len(sstableFiles) - 1
	oneFileName := sstableFiles[last]

	err = os.Remove(oneFileName)
	if err != nil {
		log.Fatal(err)
	}

	lsm.OneFilesNames = removeFileName(lsm.OneFilesNames, oneFileName)

	if len(sstableFiles) == 1 {
		br := lsm.Levels[level] + 1
		oneFileName = "files_SSTable/oneFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".db"
	} else {
		oneFileName = sstableFiles[0]
	}

	MakeDataOneFile(sortedAllEntries, oneFileName, dil_s, dil_i, comp, dict1, dict2)

	if len(sstableFiles) == 1 {
		lsm.OneFilesNames = append(lsm.OneFilesNames, oneFileName)
	} else {
		for i := 1; i < len(sstableFiles); i++ {

			err = os.Remove(sstableFiles[i])
			if err != nil {
				log.Fatal(err)
			}
			lsm.OneFilesNames = removeFileName(lsm.DataFilesNames, sstableFiles[i])

			sort.Slice(lsm.OneFilesNames, func(i, j int) bool {
				return compareFilenames(i, j, lsm.OneFilesNames)
			})

			renameFiles(lsm.OneFilesNames, oneFileName, num)

		}
	}
}

func findOtherTablesOneFile(level int, bottomIdx string, topIdx string, lsm *Config.LSMTree, dil_s int, dil_i int, comp bool, dict1 *map[int]string, dict2 *map[string]int) ([]string, []*Config.Entry) {

	var otherFiles []string

	var entriesAdd []*Config.Entry

	if level < lsm.CountOfLevels {
		//for i := 1 ; i <= lsm.LevelNumber[level]; i++ {
		for i := 1; i <= lsm.Levels[level]; i++ {

			nextSSTableFile := "files_SSTable/oneFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".db"

			summary := LoadSummaryOneFile(nextSSTableFile, dil_s, dil_i, comp, dict1, dict2)

			FirstKey := summary.FirstKey
			LastKey := summary.LastKey

			if FirstKey >= bottomIdx && FirstKey <= topIdx && LastKey >= bottomIdx && LastKey <= topIdx {
				otherFiles = append(otherFiles, nextSSTableFile)
			} else {

				// NPR. ukupan prvi opseg = [30 - 80] ---> [bottomIdx - topIdx]

				// u ovoj sstabeli je npr. [60 - 95] ---> [FirstKey - LastKey]
				// prvi se nalazi u opsegu
				if FirstKey >= bottomIdx && FirstKey <= topIdx {
					k1 := FirstKey
					k2 := topIdx
					k3 := LastKey

					entriesAdd = splitSSTableOneFile(k1, k2, k3, false, nextSSTableFile, lsm, dil_s, dil_i, comp, dict1, dict2) //dodati entriesAddOneFile
				}

				// NPR. ukupan prvi opseg = [30 - 80] ---> [bottomIdx - topIdx]

				// u ovoj sstabeli je npr. [1 - 40] ---> [FirstKey - LastKey]
				// drugi se nalazi u opsegu
				if LastKey >= bottomIdx && LastKey <= topIdx {
					k1 := FirstKey
					k2 := bottomIdx
					k3 := LastKey
					//dodati entriesAddOneFile

					// mogu poslati u jednu funckiju npr. splitSSTable(1, 30, 40)
					// ona vrati entriesRewrite, entriesAdd
					// (entriesRewrite - entry za rewrite samo)							  //  1 - 30
					// (entriesAdd     - entry za dodati u novu, veliku sstaeblu)    // 30 - 40
					// saljemo "true" jer je "lower" (uporedjujemo sa "nižim" kljucem iz opsega - topIdx ( == 30 u ovom slucaju))
					entriesAdd = splitSSTableOneFile(k1, k2, k3, false, nextSSTableFile, lsm, dil_s, dil_i, comp, dict1, dict2)
				}
			}
		}
	}
	//proveriti kako vratiti entriesAdd ako nema preklapanja ??????
	return otherFiles, entriesAdd
}

func splitSSTableOneFile(k1 string, k2 string, k3 string, lower bool, currentOneFile string, lsm *Config.LSMTree, dil_s int, dil_i int, comp bool, dict1 *map[int]string, dict2 *map[string]int) []*Config.Entry {
	// k1 = 1               1 - 30
	// k2 = 30             30 - 40
	// k3 = 40
	// otvaramo samo taj jedan fajl koji treba rewrite  // onaj od   1 - 40

	entriesRewrite, entriesAdd := GetSplitEntriesOneFile(currentOneFile, k2, lower, comp, dict1)
	// sada napravimo nove splitovane oneFile SSTabele od ovih entrija koje smo izdvojili
	// koji ne idu u veliku oneFile SSTabelu (ovi od  1 - 30) ,  (od 30 - 40 bi trebali ici u veliki SSTable)

	// prije return treba UPISATI ove REWRITE ENTRIES (to su dijelovi kkljuceva koji nisu u izabranom opsegu)
	err := os.Remove(currentOneFile)
	if err != nil {
		log.Fatal(err)
	}

	// takodje brisemo i iz lsm ovo isto kao gore
	lsm.DataFilesNames = removeFileName(lsm.DataFilesNames, currentOneFile)

	//pravljenje novog oneFile sstablea od svih sstableova ovog nivoa koji su sada spojeni
	MakeDataOneFile(entriesRewrite, currentOneFile, dil_s, dil_i, comp, dict1, dict2)

	return entriesAdd
}

// vraca sve entrije, ali splitovane u dva dijela prema kljucu, zbog preklapanja opsega
// filename  - naziv fajla nad kojim radimo
// borderKey - kljuc nakon koga splitujemo
// lower     - true: firstKey NE UPADA u opseg,   false: firstKey UPADA u opseg
func GetSplitEntriesOneFile(OneFileName string, borderKey string, lower bool, comp bool, dict *map[int]string) ([]*Config.Entry, []*Config.Entry) {
	file, _ := os.OpenFile(OneFileName, os.O_RDONLY, 0777)
	file.Seek(KEY_SIZE_SIZE, 0)

	indexOffsetBytes := make([]byte, KEY_SIZE_SIZE)

	_, _ = file.Read(indexOffsetBytes)

	indexOffset := binary.LittleEndian.Uint64(indexOffsetBytes)

	dataOffsetBytes := make([]byte, KEY_SIZE_SIZE)

	_, _ = file.Read(dataOffsetBytes)

	//dataOffset := binary.LittleEndian.Uint64(dataOffsetBytes)
	//position := int64(dataOffset)

	var entriesRewrite []*Config.Entry // svi procitani entry koji ce se rewrite u novu malu SSTabelu
	var entriesAdd []*Config.Entry     // svi procitani entry koji ce se spojiti sa velikom tabelom

	for {
		entry := readMergeOneFile(file, int(indexOffset), comp, dict)

		if entry == nil {
			break
		}

		if lower { // manji kljuc od border kljuca ide u rewrite
			if entry.Transaction.Key >= borderKey {
				entriesAdd = append(entriesAdd, entry) // 30 - 40
			} else {
				entriesRewrite = append(entriesRewrite, entry) // 1 - 30
			}
		} else { // veci kljuc od border kljuca ide u rewrite
			if entry.Transaction.Key <= borderKey {
				entriesAdd = append(entriesAdd, entry) // 60 - 80
			} else {
				entriesRewrite = append(entriesRewrite, entry) // 80 - 96
			}
		}

	}

	return entriesRewrite, entriesAdd
}

func LoadSummaryOneFile(FileName string, dil_s int, dil_i int, comp bool, dict1 *map[int]string, dict2 *map[string]int) *SStableSummary {

	file, err := os.OpenFile(FileName, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	summaryOffsetBytes := make([]byte, KEY_SIZE_SIZE)
	_, _ = file.Read(summaryOffsetBytes)
	summaryOffset := binary.LittleEndian.Uint64(summaryOffsetBytes)
	file.Seek(int64(summaryOffset), 0)
	summary := LoadSummary(file)
	return summary
}

func levelMerge(level int, lsm *Config.LSMTree, dil_s int, dil_i int, comp bool, dict1 *map[int]string, dict2 *map[string]int) {
	//treba da se izabere tabela koja se merguje
	//pa da se potraze ostale tabele u sledecem nivou

	//za file na visem nivou-uzimamo poslednju tabelu jer eto??
	for {

		br := lsm.Levels[level-1]

		dataFile := "files_SSTable/dataFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(br) + ".db"
		indexFile := "files_SSTable/indexFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(br) + ".db"
		summaryFile := "files_SSTable/summaryFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(br) + ".db"
		bloomFile := "files_SSTable/bloomFilterFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(br) + ".db"
		merkleFile := "files_SSTable/merkleTreeFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(br) + ".db"

		//trazimo opseg indeksa
		summary, _ := os.OpenFile(summaryFile, os.O_RDWR, 0777)
		SummaryContent := LoadSummary(summary)
		summary.Close()
		//SummaryContent := LoadSummary(summaryFile)

		bottomIdx := SummaryContent.FirstKey
		topIdx := SummaryContent.LastKey

		//nizovi putanja SSTable-ova kojima odgovaraju indeksi
		dataFiles, indexFiles, summaryFiles, bloomFiles, merkleFiles, entriesAdd := findOtherTables(level+1, bottomIdx, topIdx, lsm, dil_s, dil_i, comp, dict1, dict2)
		fmt.Printf("datafiles: %v\n", dataFiles)
		// ---------------------------------------------------------------------------------
		// splitovali smo sve sstable koje se preklapaju u opsegu do ovde
		// one koje ne odgovaraju smo vec "prepisali" na ista mjesta
		// a ostale entryje sa odgovarajucim kljucevima smo pokupili i stavili u "entriesAdd"
		// -> sada trebam skontati kako dodati sve ove Entryje na vec izdvojene entryje
		// -> ajde da vidimo

		num := len(dataFiles)

		dataFiles = append(dataFiles, dataFile)
		indexFiles = append(indexFiles, indexFile)
		summaryFiles = append(summaryFiles, summaryFile)
		bloomFiles = append(bloomFiles, bloomFile)
		merkleFiles = append(merkleFiles, merkleFile)

		fmt.Printf("datafiles: %v\n", dataFiles)

		levelMergeFiles(level, dataFiles, indexFiles, summaryFiles, bloomFiles, merkleFiles, lsm, num, entriesAdd, dil_s, dil_i, comp, dict1, dict2)
		//}
		// oduzmi jednu iz levela sto smo prebacili dole
		lsm.Levels[level-1]--
		// (dodaj tu jednu iz levela na [level + 1], i oduzmi num merge-ovanih)
		lsm.Levels[level] += 1   // dodaj spojenu koju smo prebacili tu
		lsm.Levels[level] -= num // oduzmi sve koje smo spojili sa tog nivoa

		fmt.Printf("PRE REKURZIJE %v\n", lsm.DataFilesNames)
		// ** T: provjeriti da li je okej uslov za level tiered?
		if lsm.Levels[level] > int(float64(lsm.MaxSSTables)*math.Pow(float64(lsm.T), float64(level))) && (level+1 < lsm.CountOfLevels) {
			// proverava broj fajlova na sledećem nivou, i ne treba da pozove merge ako je na 3. nivou tj ako je nivo 2

			println("count of levels :", lsm.CountOfLevels)
			// levelMerge(level+1, lsm, dil_s, dil_i, comp, dict1, dict2)
			println("level : ", level)
			level += 1
			continue
		} else {

			break
		}
		fmt.Printf("NA KRAJU KOMPAKCIJA %v\n", lsm.DataFilesNames)
	}
}

func findOtherTables(level int, bottomIdx string, topIdx string, lsm *Config.LSMTree, dil_s int, dil_i int, comp bool, dict1 *map[int]string, dict2 *map[string]int) ([]string, []string, []string, []string, []string, []*Config.Entry) {

	var dataFiles []string
	var indexFiles []string
	var summaryFiles []string
	var bloomFiles []string
	var merkleFiles []string

	var entriesAdd []*Config.Entry // za pocetak je nil

	if level < lsm.CountOfLevels {

		for i := 1; i <= lsm.Levels[level]; i++ {
			summaryFile := "files_SSTable/summaryFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".db"

			sumarry, _ := os.OpenFile(summaryFile, os.O_RDWR, 0777)
			SummaryContent := LoadSummary(sumarry)

			FirstKey := SummaryContent.FirstKey
			LastKey := SummaryContent.LastKey

			var dataFile = "files_SSTable/dataFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".db"
			var indexFile = "files_SSTable/indexFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".db"
			//var summaryFile = "files_SSTable/summaryFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".db"   // vec ima gore deklarisano
			var bloomFile = "files_SSTable/bloomFilterFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".db"
			var merkleFile = "files_SSTable/merkleTreeFile_" + strconv.Itoa(level) + "_" + strconv.Itoa(i) + ".db"

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
					entriesAdd = splitSSTable(k1, k2, k3, false, dataFile, indexFile, summaryFile, bloomFile, merkleFile, lsm, dil_s, dil_i, comp, dict1, dict2)
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
					entriesAdd = splitSSTable(k1, k2, k3, true, dataFile, indexFile, summaryFile, bloomFile, merkleFile, lsm, dil_s, dil_i, comp, dict1, dict2)
				}
			}
		}
	}
	// ***** TREBA VRATITI PRAZAN entriesAdd ako nema preklapanja!! (nil)
	return dataFiles, indexFiles, summaryFiles, bloomFiles, merkleFiles, entriesAdd
}

func splitSSTable(k1 string, k2 string, k3 string, lower bool, dataFile string, indexFile string, summaryFile string, bloomFile string, merkleFile string, lsm *Config.LSMTree, dil_s int, dil_i int, comp bool, dict1 *map[int]string, dict2 *map[string]int) []*Config.Entry {
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
	lsm.DataFilesNames = removeFileName(lsm.DataFilesNames, dataFile)
	lsm.IndexFilesNames = removeFileName(lsm.IndexFilesNames, indexFile)
	lsm.SummaryFilesNames = removeFileName(lsm.SummaryFilesNames, summaryFile)
	lsm.BloomFilterFilesNames = removeFileName(lsm.BloomFilterFilesNames, bloomFile)
	lsm.MerkleTreeFilesNames = removeFileName(lsm.MerkleTreeFilesNames, merkleFile)

	//pravljenje novog sstablea od svih sstableova ovog nivoa koji su sada spojeni

	MakeData(entriesRewrite, dataFile, indexFile, summaryFile, bloomFile, merkleFile, dil_s, dil_i, comp, dict1, dict2)
	return entriesAdd
}

// vraca sve entrije, ali splitovane u dva dijela prema kljucu, zbog preklapanja opsega
// filename  - naziv fajla nad kojim radimo
// borderKey - kljuc nakon koga splitujemo
// lower     - true: firstKey NE UPADA u opseg,   false: firstKey UPADA u opseg
func GetSplitEntries(dataFile string, borderKey string, lower bool) ([]*Config.Entry, []*Config.Entry) {

	var entriesRewrite []*Config.Entry // svi procitani entry koji ce se rewrite u novu malu SSTabelu
	var entriesAdd []*Config.Entry     // svi procitani entry koji ce se spojiti sa velikom tabelom

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
			break // kraj fajla?
		}

		tombstone2 := binary.LittleEndian.Uint64(info[TOMBSTONE_START:KEY_SIZE_START])
		tombstone := true
		if tombstone2 == 0 {
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

		entry := Config.Entry{
			// **** OVO CRC I TIMESTAMP PROVJERITI OBAEVEZNO JEL DOBRO CITA UOPSTE???
			//      (posto nije postojalo u funckiji iz koje sam prepisala ovo sve)
			Crc:       binary.LittleEndian.Uint32(info[CRC_START : CRC_START+CRC_SIZE]),
			Timestamp: binary.LittleEndian.Uint64(info[TIMESTAMP_START : TIMESTAMP_START+TIMESTAMP_SIZE]),
			Tombstone: tombstone,
			Transaction: Config.Transaction{
				Key:   string(data[:key_size]),
				Value: data[key_size:],
			},
		}

		if lower { // manji kljuc od border kljuca ide u rewrite
			if entry.Transaction.Key >= borderKey {
				entriesAdd = append(entriesAdd, &entry) // 30 - 40
			} else {
				entriesRewrite = append(entriesRewrite, &entry) // 1 - 30
			}
		} else { // veci kljuc od border kljuca ide u rewrite
			if entry.Transaction.Key <= borderKey {
				entriesAdd = append(entriesAdd, &entry) // 60 - 80
			} else {
				entriesRewrite = append(entriesRewrite, &entry) // 80 - 96
			}
		}
	}

	return entriesRewrite, entriesAdd
}

func levelMergeFiles(level int, dataFiles []string, indexFiles []string, summaryFiles []string, bloomFiles []string, merkleFiles []string, lsm *Config.LSMTree, num int, entriesAdd []*Config.Entry, dil_s int, dil_i int, comp bool, dict1 *map[int]string, dict2 *map[string]int) {

	//levelFiles su SStabele iz narednog nivoa + tabela iz prethodnog
	levelFiles, err := openFiles(dataFiles)
	if err != nil {
		panic(err)
	}

	var entries []*Config.Entry

	// na pocetku entries = entriesAdd, tj. sadrzi sve prethodne izdvojene entryje
	// koji su se preklapali u nekim sstabelama sa nasim opsegom
	// ako je entriesAdd = nil, sve okej i dalje

	// jedini problem koji moze biti: da li je okej samo na radnom mjesta da ih appendujemo?
	// da li redoslijed entryja uopste utice na algoritam? -> to treba da se provjeri
	entries = entriesAdd

	var sortedAllEntries []*Config.Entry
	//u entries cuvamo trenutne entie na kojim smo iz svakog sstablea sa ovog nivoa

	for _, file := range levelFiles {
		entry := readMerge(file, comp, dict1)
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
			newEntry := readMerge(levelFiles[index], comp, dict1)
			entries[index] = newEntry
		}
	}
	closeFiles(levelFiles)

	last := len(dataFiles) - 1
	dataFileName := dataFiles[last]
	indexFileName := indexFiles[last]
	summaryFileName := summaryFiles[last]
	bloomFileName := bloomFiles[last]
	merkleFileName := merkleFiles[last]

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

	lsm.DataFilesNames = removeFileName(lsm.DataFilesNames, dataFileName)
	lsm.IndexFilesNames = removeFileName(lsm.IndexFilesNames, indexFileName)
	lsm.SummaryFilesNames = removeFileName(lsm.SummaryFilesNames, summaryFileName)
	lsm.BloomFilterFilesNames = removeFileName(lsm.BloomFilterFilesNames, bloomFileName)
	lsm.MerkleTreeFilesNames = removeFileName(lsm.MerkleTreeFilesNames, merkleFileName)

	//pravljenje novog sstablea od svih sstableova ovog nivoa koji su sada spojeni

	if len(dataFiles) == 1 { //ako nije nadjen ni jedan dodatni, dodaj ga na kraj
		br := lsm.Levels[level] + 1
		dataFileName = "files_SSTable/dataFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".db"
		indexFileName = "files_SSTable/indexFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".db"
		summaryFileName = "files_SSTable/summaryFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".db"
		bloomFileName = "files_SSTable/bloomFilterFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".db"
		merkleFileName = "files_SSTable/merkleTreeFile_" + strconv.Itoa(level+1) + "_" + strconv.Itoa(br) + ".db"
	} else {

		dataFileName = dataFiles[0]
		indexFileName = indexFiles[0]
		summaryFileName = summaryFiles[0]
		bloomFileName = bloomFiles[0]
		merkleFileName = merkleFiles[0]
	}

	MakeData(sortedAllEntries, dataFileName, indexFileName, summaryFileName, bloomFileName, merkleFileName, dil_s, dil_i, comp, dict1, dict2)
	// brisemo sve fajlove za ostale SSTabele, jer su spojene u veliku i ne trebaju nam vise
	if len(dataFiles) == 1 {
		//dodati u lsm novu
		lsm.DataFilesNames = append(lsm.DataFilesNames, dataFileName)
		lsm.IndexFilesNames = append(lsm.IndexFilesNames, indexFileName)
		lsm.SummaryFilesNames = append(lsm.SummaryFilesNames, summaryFileName)
		lsm.BloomFilterFilesNames = append(lsm.BloomFilterFilesNames, bloomFileName)
		lsm.MerkleTreeFilesNames = append(lsm.MerkleTreeFilesNames, merkleFileName)
	} else {

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

			lsm.DataFilesNames = removeFileName(lsm.DataFilesNames, dataFiles[i])
			lsm.IndexFilesNames = removeFileName(lsm.IndexFilesNames, indexFiles[i])
			lsm.SummaryFilesNames = removeFileName(lsm.SummaryFilesNames, summaryFiles[i])
			lsm.BloomFilterFilesNames = removeFileName(lsm.BloomFilterFilesNames, bloomFiles[i])
			lsm.MerkleTreeFilesNames = removeFileName(lsm.MerkleTreeFilesNames, merkleFiles[i])

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

		}
	}

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

func compareFilenames(i, j int, fileNames []string) bool {

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

func renameFiles(files []string, targetFile string, num int) {

	lastNum, preLastNum := extractNumbers(targetFile)

	var firstIdx int = -1
	var lastIdx int = -1

	//npr. ako je niz 2_2, 3_1, 3_2, 3_4, 3_5, 4_1, 4_6
	//firstIdx = 3
	//lastIdx = 5
	for i, path := range files {
		tempLastNum, tempPreLastNum := extractNumbers(path)

		if tempPreLastNum < preLastNum {
			continue
		}

		//ako je prosao svoj level, uzima indeks elementa kao indikator za kraj prethodnog levela
		if tempPreLastNum > preLastNum {
			lastIdx = i
			break
		}

		//ako je dosao do ovde to znaci da je dosao do putanja trazenog levela

		//pronasli smo indeks na kom se nalazi prvi element nakon targetFile u nizu putanja
		if lastNum == tempLastNum {
			firstIdx = i + 1
		}
	}

	for i := firstIdx; i < lastIdx; i++ {

		path := files[i]
		tempLastNum, _ := extractNumbers(path)

		tempLastNum -= num
		newPath := strings.Replace(path, fmt.Sprintf("_%d.", lastNum), fmt.Sprintf("_%d.", tempLastNum), 1)

		err := os.Rename(path, newPath)
		if err != nil {
			fmt.Println("Greška pri preimenovanju fajla:", err)
			return
		}
	}
}
