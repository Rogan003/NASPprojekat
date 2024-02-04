package Config

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	WalSize                 int64  `json:"wal_size"`
	MemtableSize            uint64 `json:"memtable_size"`
	MemtableStructure       string `json:"memtable_structure"`
	MemtableNumber          int    `json:"memtable_number"`
	CacheCapacity           uint64 `json:"cache_capacity"`
	LevelCount              int    `json:"level_count"` // broj nivoa
	LevelNumber             int    `json:"level_num"`   // maksimum sstabela po nivou
	T                       int    `json:"t"`           //kolikoo se povecava svaki level
	TokenBucketSize         int    `json:"token_bucket_maxsize"`
	TokenBucketInterval     string `json:"token_bucket_interval"`
	DegreeOfDilutionSummary int    `json:"degree_of_dilution_summary"` // stepen proredjenosti u summaryfile sstabla
	DegreeOfDilutionIndex   int    `json:"degree_of_dilution_index"`
	PageSize                int    `json:"page_size"`
	Compression             bool   `json:"compression"`
	SizedCompaction         bool   `json:"sized_compaction"`
	OneFile                 bool   `json:"one_file"`
}

type LSMTree struct {
	Levels                []int
	CountOfLevels         int
	MaxSSTables           int
	DataFilesNames        []string
	IndexFilesNames       []string
	SummaryFilesNames     []string
	BloomFilterFilesNames []string
	OneFilesNames         []string
	MerkleTreeFilesNames  []string
	T                     int
}

// cita parametre programa iz json fajla i pravi intsancu Configa
func ConfigInst() (Config, error) {
	var config Config
	configData, err := os.ReadFile("config.json") // ako ne postoji popuniti config default vrednostima i vratiti ga
	if err != nil {
		// Tamara: return Config{5000, 1000, "skiplist", 10, 100, 5, 10, 10, 15, "1m", 5, 4, 10, false, false}, nil // iako mozda ne treba nil bas
		return Config{5000, 1000, "skiplist", 10, 100, 5, 10, 10, 15, "1m", 5, 4, 10, false, true, false}, nil // iako mozda ne treba nil bas
		log.Fatal(err)
	}

	json.Unmarshal(configData, &config)
	fileContent, err := ioutil.ReadFile("config.json")
	if err != nil {
		fmt.Println("Error reading the file:", err)
		return config, err
	}

	err = json.Unmarshal(fileContent, &config)
	if err != nil {
		fmt.Println("Error unmarshalling JSON:", err)
		return config, err
	}
	return config, nil
}

func NewLMSTree(Config Config) *LSMTree {
	l := make([]int, Config.LevelCount)
	dataFile := make([]string, 0)
	indexFile := make([]string, 0)
	summaryFile := make([]string, 0)
	filterFile := make([]string, 0)
	merkleFile := make([]string, 0)
	oneFile := make([]string, 0)

	files, err := ioutil.ReadDir("files_SSTable") //ucitavanje svega sto je u folderu, vraca listu fajlova ili gresku
	if err != nil {                               //ako do nje dodje za slucaj da je doslo do greske
		fmt.Println("Greska pri citanju direktorijuma sa SSTabelama!")
		return nil
	}

	for _, file := range files { //_ jer nam ne treba indeks
		if file.IsDir() { //ako je direktorijum ignorisemo
			continue
		}

		if strings.HasSuffix(file.Name(), ".db") && strings.HasPrefix(file.Name(), "bloomFilterFile") {
			path := "files_SSTable/" + file.Name()

			filterFile = append(filterFile, path)

			lvlStr := strings.TrimPrefix(file.Name(), "bloomFilterFile_")

			indx := 0

			for {
				if lvlStr[indx] == '_' {
					break
				}

				indx++
			}

			lvl, err := strconv.Atoi(lvlStr[:indx])

			if err != nil {
				continue
			}

			l[lvl-1]++

		} else if strings.HasSuffix(file.Name(), ".db") && strings.HasPrefix(file.Name(), "dataFile") {
			path := "files_SSTable/" + file.Name()
			dataFile = append(dataFile, path)

		} else if strings.HasSuffix(file.Name(), ".db") && strings.HasPrefix(file.Name(), "indexFile") {
			path := "files_SSTable/" + file.Name()
			indexFile = append(indexFile, path)

		} else if strings.HasSuffix(file.Name(), ".db") && strings.HasPrefix(file.Name(), "summaryFile") {
			path := "files_SSTable/" + file.Name()
			summaryFile = append(summaryFile, path)

		} else if strings.HasSuffix(file.Name(), ".db") && strings.HasPrefix(file.Name(), "merkleTreeFile") {
			path := "files_SSTable/" + file.Name()
			merkleFile = append(merkleFile, path)

		} else if strings.HasSuffix(file.Name(), ".db") && strings.HasPrefix(file.Name(), "oneFile") {
			path := "files_SSTable/" + file.Name()
			oneFile = append(oneFile, path)

			lvlStr := strings.TrimPrefix(file.Name(), "oneFile_")

			indx := 0

			for {
				if lvlStr[indx] == '_' {
					break
				}

				indx++
			}

			lvl, err := strconv.Atoi(lvlStr[:indx])

			if err != nil {
				continue
			}

			l[lvl-1]++
		}
	}

	sort.Slice(filterFile, func(i, j int) bool {

		str1 := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(filterFile[i]), "bloomFilterFile_"), ".db")
		str2 := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(filterFile[j]), "bloomFilterFile_"), ".db")

		substrings1 := strings.Split(str1, "_")
		level1, _ := strconv.Atoi(substrings1[0])
		num1, _ := strconv.Atoi(substrings1[1])

		substrings2 := strings.Split(str2, "_")
		level2, _ := strconv.Atoi(substrings2[0])
		num2, _ := strconv.Atoi(substrings2[1])

		if level1 < level2 {
			return true
		} else if level1 > level2 {
			return false
		}

		return num1 > num2
	})

	sort.Slice(merkleFile, func(i, j int) bool {

		str1 := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(merkleFile[i]), "merkleTreeFile_"), ".db")
		str2 := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(merkleFile[j]), "merkleTreeFile_"), ".db")

		substrings1 := strings.Split(str1, "_")
		level1, _ := strconv.Atoi(substrings1[0])
		num1, _ := strconv.Atoi(substrings1[1])

		substrings2 := strings.Split(str2, "_")
		level2, _ := strconv.Atoi(substrings2[0])
		num2, _ := strconv.Atoi(substrings2[1])

		if level1 < level2 {
			return true
		} else if level1 > level2 {
			return false
		}

		return num1 > num2
	})

	sort.Slice(dataFile, func(i, j int) bool {

		str1 := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(dataFile[i]), "dataFile_"), ".db")
		str2 := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(dataFile[j]), "dataFile_"), ".db")

		substrings1 := strings.Split(str1, "_")
		level1, _ := strconv.Atoi(substrings1[0])
		num1, _ := strconv.Atoi(substrings1[1])

		substrings2 := strings.Split(str2, "_")
		level2, _ := strconv.Atoi(substrings2[0])
		num2, _ := strconv.Atoi(substrings2[1])

		if level1 < level2 {
			return true
		} else if level1 > level2 {
			return false
		}

		return num1 > num2
	})

	sort.Slice(indexFile, func(i, j int) bool {

		str1 := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(indexFile[i]), "indexFile_"), ".db")
		str2 := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(indexFile[j]), "indexFile_"), ".db")

		substrings1 := strings.Split(str1, "_")
		level1, _ := strconv.Atoi(substrings1[0])
		num1, _ := strconv.Atoi(substrings1[1])

		substrings2 := strings.Split(str2, "_")
		level2, _ := strconv.Atoi(substrings2[0])
		num2, _ := strconv.Atoi(substrings2[1])

		if level1 < level2 {
			return true
		} else if level1 > level2 {
			return false
		}

		return num1 > num2
	})

	sort.Slice(summaryFile, func(i, j int) bool {

		str1 := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(summaryFile[i]), "summaryFile_"), ".db")
		str2 := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(summaryFile[j]), "summaryFile_"), ".db")

		substrings1 := strings.Split(str1, "_")
		level1, _ := strconv.Atoi(substrings1[0])
		num1, _ := strconv.Atoi(substrings1[1])

		substrings2 := strings.Split(str2, "_")
		level2, _ := strconv.Atoi(substrings2[0])
		num2, _ := strconv.Atoi(substrings2[1])

		if level1 < level2 {
			return true
		} else if level1 > level2 {
			return false
		}

		return num1 > num2
	})

	sort.Slice(oneFile, func(i, j int) bool {

		str1 := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(oneFile[i]), "oneFile_"), ".db")
		str2 := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(oneFile[j]), "oneFile_"), ".db")

		substrings1 := strings.Split(str1, "_")
		level1, _ := strconv.Atoi(substrings1[0])
		num1, _ := strconv.Atoi(substrings1[1])

		substrings2 := strings.Split(str2, "_")
		level2, _ := strconv.Atoi(substrings2[0])
		num2, _ := strconv.Atoi(substrings2[1])

		if level1 < level2 {
			return true
		} else if level1 > level2 {
			return false
		}

		return num1 > num2
	})

	return &LSMTree{
		Levels:                l,
		CountOfLevels:         Config.LevelCount,
		MaxSSTables:           Config.LevelNumber,
		DataFilesNames:        dataFile,
		IndexFilesNames:       indexFile,
		SummaryFilesNames:     summaryFile,
		BloomFilterFilesNames: filterFile,
		MerkleTreeFilesNames:  merkleFile,
		OneFilesNames:         oneFile,
		T:                     Config.T,
	}
}

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

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

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type Entry struct { //red u walu
	Crc         uint32
	Timestamp   uint64
	Tombstone   bool
	Transaction Transaction //transakcije
}

type Transaction struct { //jedna transakcija
	Key   string
	Value []byte
}

func NewEntry(Tombstone bool, Transaction Transaction) *Entry {
	return &Entry{
		Crc:         CRC32(Transaction.Value),
		Timestamp:   uint64(time.Now().Unix()),
		Tombstone:   Tombstone,
		Transaction: Transaction,
	}
}
func NewTransaction(key string, value []byte) *Transaction {
	return &Transaction{
		Key:   key,
		Value: value,
	}
}

func ToEntry(data []byte) Entry {

	entry := Entry{}

	entry.Crc = binary.LittleEndian.Uint32(data[:CRC_SIZE]) //ucitavaju se prva 4 bajta
	data = data[CRC_SIZE:]                                  //pomeramo se za 4 bajta

	entry.Timestamp = binary.LittleEndian.Uint64(data[:TIMESTAMP_SIZE])
	data = data[TIMESTAMP_SIZE:]

	entry.Tombstone = data[0] != 0 //true ako je 1, false ako je 0
	data = data[TOMBSTONE_SIZE:]

	keySize := binary.LittleEndian.Uint64(data[:KEY_SIZE_SIZE])
	data = data[KEY_SIZE_SIZE+VALUE_SIZE_SIZE:] //pomeramo se za 16 zbog key size i value size

	entry.Transaction.Key = string(data[:keySize])
	data = data[keySize:]

	entry.Transaction.Value = data

	return entry
}

func (entry *Entry) ToByte() []byte { //pretvara iz vrednosti u bajtove
	var data []byte

	crcb := make([]byte, CRC_SIZE)
	binary.LittleEndian.PutUint32(crcb, CRC32(entry.Transaction.Value))
	data = append(data, crcb...) //dodaje se CRC

	sec := entry.Timestamp
	secb := make([]byte, TIMESTAMP_SIZE)
	binary.LittleEndian.PutUint64(secb, uint64(sec))
	data = append(data, secb...) //dodaje se Timestamp

	//1 - deleted; 0 - not deleted
	//dodaje se Tombstone
	if entry.Tombstone {
		var delb byte = 1
		data = append(data, delb)
	} else {
		var delb byte = 0
		data = append(data, delb)
	}

	keyb := []byte(entry.Transaction.Key)
	keybs := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(keybs, uint64(len(keyb)))

	valuebs := make([]byte, VALUE_SIZE_SIZE)
	binary.LittleEndian.PutUint64(valuebs, uint64(len(entry.Transaction.Value)))

	//dodaju se Key Size i Value Size
	data = append(data, keybs...)
	data = append(data, valuebs...)
	//dodaju se Key i Value
	data = append(data, keyb...)
	data = append(data, entry.Transaction.Value...)

	return data
}

func ReadDictionary(dict *map[int]string) error {
	fileContent, err := ioutil.ReadFile("dictionary.json")
	if err != nil {
		fmt.Println("Error reading the file:", err)
		return err
	}

	err = json.Unmarshal(fileContent, dict)
	if err != nil {
		fmt.Println("Error unmarshalling JSON:", err)
		return err
	}
	return nil
}

func SaveDictionary(dict *map[int]string) error {
	jsonString, err := json.Marshal(*dict)

	if err != nil {
		fmt.Println("Error marshalling JSON:", err)
		return err
	}

	err = ioutil.WriteFile("dictionary.json", jsonString, 0644)

	if err != nil {
		fmt.Println("Error writing to file:", err)
		return err
	}
	return nil
}

func ReadDictionary2(dict *map[string]int) error {
	fileContent, err := ioutil.ReadFile("dictionary2.json")
	if err != nil {
		fmt.Println("Error reading the file:", err)
		return err
	}

	err = json.Unmarshal(fileContent, dict)
	if err != nil {
		fmt.Println("Error unmarshalling JSON:", err)
		return err
	}
	return nil
}

func SaveDictionary2(dict *map[string]int) error {
	jsonString, err := json.Marshal(*dict)

	if err != nil {
		fmt.Println("Error marshalling JSON:", err)
		return err
	}

	err = ioutil.WriteFile("dictionary2.json", jsonString, 0644)

	if err != nil {
		fmt.Println("Error writing to file:", err)
		return err
	}
	return nil
}
