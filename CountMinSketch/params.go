package CountMinSketch

import (
	"encoding/gob"
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

func NewCMS(e float64, d float64) CMS {
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
	return CMS{
		HashArray: hashArray,
		Data:      data,
		M:         int(cols),
		K:         int(NumHash),
	}
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

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(cms)

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

	decoder := gob.NewDecoder(file)
	file.Seek(0, 0)
	for {
		err = decoder.Decode(cms)
		if err != nil {
			return err
		}
	}
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
