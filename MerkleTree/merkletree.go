package MerkleTree

import (
	"encoding/gob"
	"os"
	"fmt"
	"crypto/sha1"
	"encoding/hex"
)

 
/* pomocna struktura koju cemo cuvati u merkletree.gob 
Hashes = svi hesevi nasih cvorova u MerkleTree
L = broj cvorova u prvom/pocetnom redu, oznacava koliko smo podataka poslali
    u MerkleTree (ubacena radi lakseg Deserialize) */
type Tree struct {
	Hashes [][]byte
	L int
}

// MerkleTree sadrzi samo root hash 
type MerkleTree struct {
	Root *Node
}

/* Cvor koji ubacujemo u MerkleTree
sadrzi u sebi niz bajtova, kao i pokazivace na desni i lijevi cvor */
type Node struct {
	data []byte
	left *Node
	right *Node
}


// pomocne globalne promjenljive radi meni lakse serijalizacije */
var allHashes [][]byte
var lTree int 


// MerkleTree Konstruktor
func (mt *MerkleTree) Init(data [][]byte) {
	arr := []Node{}
	
	// krecemo od dole, od niza proslijedjenih podataka i pravimo pocetne donje cvorove
	for _, v := range data {
		if (len(v) == 0) {
			break
		}
		h := Hash(v)
		hash := h[:]	
		curNode := Node{hash, nil, nil}
		allHashes = append(allHashes, hash)
		arr = append(arr, curNode)
	}
	// ako je duzina cvorova neparna, dodamo prazan cvor
	if (len(arr) % 2 == 1) {
		h := Hash([]byte{})
		hash := h[:]
		lastNode := Node{hash, nil, nil}
		allHashes = append(allHashes, hash)
		arr = append(arr, lastNode)
	}	
	lTree = len(arr)  // pomocna globalna promjenljiva

   // nastavljamo da gradimo MerkleTree prema gore
	for (true) {
		arr2 := []Node{}  // arr2 je pomocni niz u koje stavljam sve cvorove iz jednog nivoa
		for i := 0; i < len(arr); i += 2 {
			nodeLeft := arr[i]
			nodeRight := arr[i + 1]
			h1 := Hash(nodeLeft.data)
			h2 := Hash(nodeRight.data)
			hash1 := h1[:]	
			hash2 := h2[:]
			hash3 := append(hash1, hash2...)
			h := Hash(hash3)
			hash := h[:]
			curNode := Node{hash, &nodeLeft, &nodeRight}
			allHashes = append(allHashes, hash)
			arr2 = append(arr2, curNode)
		}
		if (len(arr2) == 1) {
			// arr2 sadrzi jedan jedini cvor, sto je u stvari root, break while(true) petlju
			mt.Root = &arr2[0]
			break
		}
		// ako je duzina cvorova u jednom nivou neparna, dodamo opet prazan na kraj
		if (len(arr2) % 2 == 1) {
			h := Hash([]byte{})
			hash := h[:]
			lastNode := Node{hash, nil, nil}
			allHashes = append(allHashes, hash)
			arr2 = append(arr2, lastNode)
		}
		arr = arr2
	}
}

// Serijalizujemo u stvari pomocnu strukturu Tree (pogledaj gore sta sadrzi)
func (mt *MerkleTree) Serialize() {
	// *****VAZNO*****
	/* ako testiramo ovde, iz custom maina, onda dodati: "../files%" ("../" ispred files) */
	filePath := fmt.Sprintf("files%cmerkletree.gob", os.PathSeparator)
	file, err := os.OpenFile(filePath, os.O_RDWR | os.O_CREATE, 0666)
	if(err != nil) {
		panic(err)
	}
	defer file.Close()

	t := Tree{allHashes, lTree}
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(t)

	if(err != nil) {
		panic(err)
	}
}

/* Deserialize pomocnu strukturu Tree, iz koje izvlacimo sve
prethodne hasheve i pravimo ispocetka MerkleTree */
func (mt *MerkleTree) Deserialize() {
	// *****VAZNO*****
	/* ako testiramo ovde, iz custom maina, onda dodati: "../files%" ("../" ispred files) */
	filePath := fmt.Sprintf("files%cmerkletree.gob", os.PathSeparator)
	file, err := os.OpenFile(filePath, os.O_RDWR | os.O_CREATE, 0666)
	if(err != nil) {
		panic(err)
	}

	defer file.Close()
	
	t := Tree{}
	decoder := gob.NewDecoder(file)
	file.Seek(0, 0)
	for {
		err = decoder.Decode(&t)
		if err != nil {
			break
		}
	}

	arr := []Node{}
	for i := 0; i < t.L; i++ {
		curNode := Node{t.Hashes[i], nil, nil}
		arr = append(arr, curNode)
	}

	cur := t.L
	for (true) {
		arr2 := []Node{}
		for i := 0; i < len(arr); i += 2 {
			nodeLeft := arr[i]
			nodeRight := arr[i + 1]
			curNode := Node{t.Hashes[cur], &nodeLeft, &nodeRight}
			arr2 = append(arr2, curNode)
			cur++ 
		}
		if (len(arr2) == 1) {
			// onda je root...
			mt.Root = &arr2[0]
			break
		}
		if (len(arr2) % 2 == 1) {
			lastNode := Node{t.Hashes[cur], nil, nil}
			arr2 = append(arr2, lastNode)
			cur++
		}
		arr = arr2
	}
}


func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}


/*
func main() {
	arr := make([][]byte, 10, 100)
	var mt = MerkleTree{}
	
   var elem1 = []byte("1")
	var elem2 = []byte("2")
   var elem3 = []byte("3")
   var elem4 = []byte("4")
   var elem5 = []byte("5")

	arr[0] = elem1
	arr[1] = elem2
	arr[2] = elem3
	arr[3] = elem4
	arr[4] = elem5

	mt.Init(arr)
	mt.Serialize()

	var mt2 = MerkleTree{}
	mt2.Deserialize()

	fmt.Println()
	fmt.Println(mt.Root)
	fmt.Println(mt2.Root)
	fmt.Println()	
}
*/