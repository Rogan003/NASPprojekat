package SimHash

import (
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"strings"
	"unicode"
)

func Tokenization(input string) []string {
	var text strings.Builder
	input = strings.ToLower(input)
	for _, char := range input {
		//ako nije znak interpunkcije dodaje char u text
		if !unicode.IsPunct(char) {
			text.WriteRune(char)
		}
	}
	words := strings.Fields(text.String())
	var result []string
	for _, word := range words {
		if len(word) >= 3 { //izbacuje sve reci koje su krace od 3 slova ('if', 'on', 'of',...)
			result = append(result, word)
		}
	}

	return result
}

func Hashing(token string) [16]byte {
	hasher := md5.New()
	hasher.Write([]byte(token))
	hashBytes := hasher.Sum(nil)
	var simHash [16]byte
	copy(simHash[:], hashBytes[:16])

	return simHash
}

func SimHash(input string) [16]byte {
	tokens := Tokenization(input)
	vector := make([]int, 128)
	for _, token := range tokens {
		hashValue := Hashing(token)
		for i := 0; i < 16; i++ {
			B := hashValue[i]         //uzima redom bajtove iz hashValue
			for j := 7; j >= 0; j-- { //ide od 7 do 0 da bi uzeli bitove sa leva na desno
				bit := (B >> uint(j)) & 1 //shiftuje u desno uzeti bajt i vresi AND sa 0000001 da bi dobili zeljeni bit
				if bit == 1 {
					vector[i*8+7-j]++ // ako je vrednost 1 na bitu od 0 do 128 dodaje se u sumu + 1
				} else {
					vector[i*8+7-j]-- // ako je vrednost 0 na bitu od 0 do 128 dodaje se na sumu - 1
				}
			}
		}

	}

	var simHash [16]byte
	for i := 0; i < 16; i++ {
		simB := 0       //temporary bajt
		simMask := 0x80 //0b10000000
		for j := 0; j < 8; j++ {
			if vector[i*8+j] > 0 { //gledamo da li je zbir na 8*i+j-tom bitu > 0
				simB |= (simMask >> uint(j)) //upisujemo 1 na j-to mesto u simB
			} //preskace se else jer na njemu ostaje 0
		}
		simHash[i] = byte(simB)
	}

	return simHash
}

func HammingDistance(hash1, hash2 [16]byte) int {
	distance := 0
	for i := 0; i < 16; i++ {
		hash1B := hash1[i]
		hash2B := hash2[i]
		for j := 7; j >= 0; j-- { //ide od 7 do 0 da bi uzeli bitove sa leva na desno
			bit1 := (hash1B >> uint(j)) & 1 //shiftuje u desno uzeti bajt i vresi AND sa 0000001 da bi dobili zeljeni bit
			bit2 := (hash2B >> uint(j)) & 1 //shiftuje u desno uzeti bajt i vresi AND sa 0000001 da bi dobili zeljeni bit
			if bit1 != bit2 {
				distance++
			}
		}
	}
	return distance
}

func GetHashAsString(data []byte) string {
	hash := md5.Sum(data)
	res := ""
	for _, b := range hash {
		res = fmt.Sprintf("%s%b", res, b)
	}
	return res
}

func ReadFromFile(fileName string) (string, error) {
	fileContent, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return "", err
	}
	contentString := string(fileContent)
	return contentString, nil
}

/*
func main() {
	content1, _ := ReadFromFile("tekst1.txt")
	hash1 := SimHash(content1)
	content2, _ := ReadFromFile("tekst2.txt")
	hash2 := SimHash(content2)
	fmt.Print("Ispis Hamming distance za tekst1 i tekst2: ")
	result := HammingDistance(hash1, hash2)
	fmt.Println(result)
	fmt.Print("Ispis Hamming distance za tekst1 i tekst1: ")
	result = HammingDistance(hash1, hash1)
	fmt.Println(result)

	content3, _ := ReadFromFile("tekst3.txt")
	hash3 := SimHash(content3)
	content4, _ := ReadFromFile("tekst4.txt")
	hash4 := SimHash(content4)
	fmt.Print("Ispis Hamming distance za tekst3 i tekst4 (razliciti su): ")
	result = HammingDistance(hash3, hash4)
	fmt.Println(result)
	fmt.Print("Ispis Hamming distance za tekst4 i tekst4: ")
	result = HammingDistance(hash4, hash4)
	fmt.Println(result)
}
*/
