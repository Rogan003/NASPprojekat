package CountMinSketch

import (
	"encoding/binary"
	"math"
	"os"
)

func CalculateM(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

func CalculateK(delta float64) uint {
	return uint(math.Ceil(math.Log(math.E / delta)))
}

type CMS struct {
	HashArray []HashWithSeed
	Data      [][]int
	M         int
	K         int
}

func (cms *CMS) NewCMS(e float64, d float64) {
	NumHash := CalculateK(e)
	cols := CalculateM(d)
	data := make([][]int, NumHash)
	for i := range data {
		data[i] = make([]int, cols)
		for j := 0; j < len(data[i]); j++ {
			data[i][j] = 0
		}
	}
	hashArray := CreateHashFunctions(NumHash)
	cms.HashArray = hashArray
	cms.Data = data
	cms.M = int(cols)
	cms.K = int(NumHash)
}

func (cms *CMS) AddToCMS(newData string) {
	//data je novi podatak kao niz bajtova
	data := []byte(newData)
	//i-brojac u nizu hash funkcija, h- je trenutni HashSeed
	for i, h := range cms.HashArray {
		// value je index elementa u i-tom redu koji treba da povecamo
		value := h.Hash(data) % uint64(cms.M)
		cms.Data[i][value] += 1
	}
}
func (cms *CMS) SearchCSM(searchData string) int {
	min := 9223372036854775807
	data := []byte(searchData)
	//i-brojac u nizu hash funkcija, h- je trenutni HashSeed
	for i, h := range cms.HashArray {
		// value je index elementa u i-tom redu koji treba da povecamo
		value := h.Hash(data) % uint64(cms.M)
		//trazimo najmanji broj pojave zbog kolizija
		if cms.Data[i][value] < min {
			min = cms.Data[i][value]
		}
	}
	return min
}

// serializacija count min sketch
func (cms *CMS) Serialize(path string) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	bytess, _ := cms.ToBytes()
	_, err = file.Write(bytess)
	if err != nil {
		panic(err)
	}
}

// deserializacija count min sketch
func (cms *CMS) Deserialize(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		//panic(err)
		return err
	}
	defer file.Close()

	file.Seek(0, 0)

	fi, err2 := file.Stat()
	if err2 != nil {
		return err2
	}

	data := make([]byte, fi.Size())
	_, err = file.Read(data)
	if err != nil {
		return err
	}

	cms.FromBytes(data)

	return nil
}

func (cms *CMS) ToBytes() ([]byte, error) {
	data := make([]byte, 0)

	kBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(kBytes, uint64(cms.K))
	data = append(data, kBytes...)

	mBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(mBytes, uint64(cms.M))
	data = append(data, mBytes...)

	for _, hash := range cms.HashArray {
		hashSizeBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(hashSizeBytes, uint64(len(hash.Seed)))
		data = append(data, hashSizeBytes...)

		data = append(data, hash.Seed...)
	}

	for _, hash := range cms.Data {
		byteSlice := make([]byte, len(hash)*4) // Assuming int is 4 bytes
		for i, num := range hash {
			binary.LittleEndian.PutUint32(byteSlice[i*4:], uint32(num))
		}
		data = append(data, byteSlice...)
	}
	return data, nil
}

func (cms *CMS) FromBytes(bytess []byte) error {
	cms.K = int(binary.LittleEndian.Uint64(bytess[:8]))
	bytess = bytess[8:]

	cms.M = int(binary.LittleEndian.Uint64(bytess[:8]))
	bytess = bytess[8:]

	for i := 0; i < int(cms.K); i++ {
		length := binary.LittleEndian.Uint64(bytess[:8])
		bytess = bytess[8:]

		cms.HashArray[i] = HashWithSeed{Seed: bytess[:length]}
		bytess = bytess[length:]
	}

	for i := 0; i < int(cms.K); i++ {
		intSlice := make([]int, cms.M)
		for j := 0; j < len(intSlice); j++ {
			intSlice[j] = int(binary.LittleEndian.Uint32(bytess[:4]))
			bytess = bytess[4:]
		}
		cms.Data[i] = intSlice
	}

	return nil
}

/*
func main() {
	cms := NewCMS(0.05, 0.05)

	dataToUpdate := []string{"apple", "banana", "apple", "cherry", "banana"}
	for _, item := range dataToUpdate {
		cms.addToCMS(item)
	}

	itemsToSearch := []string{"apple", "banana", "cherry", "orange"}
	for _, item := range itemsToSearch {
		frequency := cms.searchCSM(item)
		fmt.Printf("Estimated frequency of %s: %d\n", item, frequency)
	}
}*/
