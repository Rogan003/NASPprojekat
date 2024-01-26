package Config

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"log"
	"os"
	"time"
)

type Config struct {
	WalSize           uint64 `json:"wal_size"`
	MemtableSize      uint64 `json:"memtable_size"`
	MemtableStructure string `json:"memtable_structure"`
	LevelCount        int    `json:"level_count"` // broj nivoa
	LevelNumber       int    `json:"level_num"`   // maksimum sstabela po nivou
	T  				  int	 `json:"t"`			//kolikoo se povecava svaki level
}

type LSMTree struct {
	Levels                []int
	CountOfLevels         int
	MaxSSTables           int
	DataFilesNames        []string
	IndexFilesNames       []string
	SummaryFilesNames     []string
	BloomFilterFilesNames []string
	MerkleTreeFilesNames  []string
	T 					  int
}

// cita parametre programa iz json fajla i pravi intsancu Configa
func config() (Config, error) {
	var config Config
	configData, err := os.ReadFile("config.json")
	if err != nil {
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
	file := make([]string, Config.LevelNumber*Config.LevelNumber)
	return &LSMTree{
		Levels:                l,
		CountOfLevels:         Config.LevelCount,
		MaxSSTables:           Config.LevelNumber,
		DataFilesNames:        file,
		IndexFilesNames:       file,
		SummaryFilesNames:     file,
		BloomFilterFilesNames: file,
		MerkleTreeFilesNames:  file,
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

	entry.Crc = binary.LittleEndian.Uint32(data[:4]) //ucitavaju se prva 4 bajta
	data = data[4:]                                  //pomeramo se za 4 bajta

	entry.Timestamp = binary.LittleEndian.Uint64(data[:8])
	data = data[8:]

	entry.Tombstone = data[0] != 0 //true ako je 1, false ako je 0
	data = data[1:]

	keySize := binary.LittleEndian.Uint32(data[:4])
	data = data[8:] //pomeramo se za 8 zbog key size i value size

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

	sec := time.Now().Unix()
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
