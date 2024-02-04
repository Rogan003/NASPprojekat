package BloomFilter

import (
	"encoding/binary"
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

	bytess, _ := bf.ToBytes()
	_, err = file.Write(bytess)
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

	bf.FromBytes(data)

	return nil
}

func (bf *BloomFilter) ToBytes() ([]byte, error) {
	data := make([]byte, 0)

	kBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(kBytes, uint64(bf.K))
	data = append(data, kBytes...)

	mSizeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(mSizeBytes, uint64(len(bf.M)))
	data = append(data, mSizeBytes...)
	
	data = append(data, bf.M...)

	for _, hash := range bf.Seeds {
		hashSizeBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(hashSizeBytes, uint64(len(hash.Seed)))
		data = append(data, hashSizeBytes...)
		
		data = append(data, hash.Seed...)
	}

	return data, nil
}

func (bf *BloomFilter) FromBytes(bytess []byte) error {
	bf.K = uint(binary.LittleEndian.Uint64(bytess[:8]))
	bytess = bytess[8:]

	bf.Seeds = make([]HashWithSeed, bf.K)

	mSize := binary.LittleEndian.Uint64(bytess[:8])
	bytess = bytess[8:]

	bf.M = bytess[:mSize]
	bytess = bytess[mSize:]

	for i := 0;i < int(bf.K);i++ {
		length := binary.LittleEndian.Uint64(bytess[:8])
		bytess = bytess[8:]

		bf.Seeds[i] = HashWithSeed{Seed : bytess[:length]}
		bytess = bytess[length:]
	}

	return nil
}