package BloomFilter

import (
	"encoding/gob"
	"os"
)

/*
STRUKTURA BLOOM FILTER:
M - niz 0 i 1
K - broj hash funkcija
Seeds - seeds za hash funkcije
*/

type BloomFilter struct {
	M     []byte
	K     uint
	Seeds []HashWithSeed
}

// konstruktor za bloom filter, na osnovu broja ocekivanih elemenata kreira pocetni bloom filter
func (bf *BloomFilter) Init(expectedElements int, falsePositiveRate float64) {
	m := CalculateM(expectedElements, falsePositiveRate)
	bf.M = make([]byte, m)
	k := CalculateK(expectedElements, m)
	bf.K = k
	bf.Seeds = CreateHashFunctions(k)
}

// proverava da li je dati niz bajtova (element) u bloom filteru
// check_elem treba da se vrsi samo po kljucu a ne po celom elementu jer nemamo jos element tokom trazenja?
// PROMENITI
func (bf BloomFilter) Check_elem(elem []byte) bool {
	isThere := true

	for _, seed := range bf.Seeds {
		index := (seed.Hash(elem) % uint64(len(bf.M)))

		if bf.M[index] == 0 {
			isThere = false
			break
		}
	}

	return isThere
}

// dodaje niz bajtova (element) u bloom filter
func (bf *BloomFilter) Add(elem []byte) {
	for _, seed := range bf.Seeds {
		index := (seed.Hash(elem) % uint64(len(bf.M)))

		if bf.M[index] == 0 {
			bf.M[index] = 1
		}
	}
}

// serializacija bloom filtera
func (bf BloomFilter) Serialize(path string) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(bf)

	if err != nil {
		panic(err)
	}
}

// deserializacija bloom filtera
func (bf *BloomFilter) Deserialize(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		//panic(err)
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	file.Seek(0, 0)
	for {
		err = decoder.Decode(bf)
		if err != nil {
			return err
		}
	}
	//zasto je unreachable???
	return nil
}
