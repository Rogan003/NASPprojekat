package MerkleTree

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"

	//"fmt"
	"os"
	"reflect"
)

/*
	pomocna struktura koju cemo cuvati u merkletree.gob

Hashes = svi hesevi nasih cvorova u MerkleTree
L = broj cvorova u prvom/pocetnom redu, oznacava koliko smo podataka poslali

	u MerkleTree (ubacena radi lakseg Deserialize)
*/
type Tree struct {
	Hashes [][]byte
	Length int
}

// MerkleTree sadrzi samo root hash
type MerkleTree struct {
	Root *Node
}

/*
	Cvor koji ubacujemo u MerkleTree

sadrzi u sebi niz bajtova, kao i pokazivace na desni i lijevi cvor
*/
type Node struct {
	Data  []byte
	Left  *Node
	Right *Node
	Level int
	Pos   int
}

/*
	Struktura koja vraca gdje je razlika tokom poredjenja 2 merkle stabla

Level = broj nivoa na kom se nalazi razlika (od dole gledano)
Pos = broj cvor u kome je razlika (gledano sa lijeva na desno)
Node1, Node2 = razlike u Node-ovima
*/
type DiffPoint struct {
	Level int
	Pos   int
	Node1 *Node
	Node2 *Node
}

// pomocne globalne promjenljive radi meni lakse serijalizacije */
var allHashes [][]byte
var lTree int

// MerkleTree Konstruktor
func (mt *MerkleTree) Init(data [][]byte) {
	arr := []Node{}
	allHashes = nil
	lTree = 0
	// krecemo od dole, od niza proslijedjenih podataka i pravimo pocetne donje cvorove
	for i, v := range data {
		if len(v) == 0 {
			break
		}
		h := Hash(v)
		hash := h[:]
		curNode := Node{hash, nil, nil, 0, i}
		allHashes = append(allHashes, hash)
		arr = append(arr, curNode)
	}
	// ako je duzina cvorova neparna, dodamo prazan cvor
	if len(arr)%2 == 1 {
		h := Hash([]byte{})
		hash := h[:]
		lastNode := Node{hash, nil, nil, 0, len(arr)}
		allHashes = append(allHashes, hash)
		arr = append(arr, lastNode)
	}
	lTree = len(arr) // pomocna globalna promjenljiva

	lvl := 1
	// nastavljamo da gradimo MerkleTree prema gore
	for true {
		cur := 0
		arr2 := []Node{} // arr2 je pomocni niz u koje stavljam sve cvorove iz jednog nivoa
		for i := 0; i < len(arr); i += 2 {
			nodeLeft := arr[i]
			nodeRight := arr[i+1]
			h1 := Hash(nodeLeft.Data)
			h2 := Hash(nodeRight.Data)
			hash1 := h1[:]
			hash2 := h2[:]
			hash3 := append(hash1, hash2...)
			h := Hash(hash3)
			hash := h[:]
			curNode := Node{hash, &nodeLeft, &nodeRight, lvl, cur}
			cur++
			allHashes = append(allHashes, hash)
			arr2 = append(arr2, curNode)
		}
		if len(arr2) == 1 {
			// arr2 sadrzi jedan jedini cvor, sto je u stvari root, break while(true) petlju
			mt.Root = &arr2[0]
			break
		}
		// ako je duzina cvorova u jednom nivou neparna, dodamo opet prazan na kraj
		if len(arr2)%2 == 1 {
			h := Hash([]byte{})
			hash := h[:]
			lastNode := Node{hash, nil, nil, lvl, len(arr2)}
			allHashes = append(allHashes, hash)
			arr2 = append(arr2, lastNode)
		}
		lvl++
		arr = arr2
	}

	//fmt.Println("INIT: ", allHashes, "\n\n")
}

// vraca strukturu DiffPoint koja sadrzi nivo promjene, cvor koji je izmijenjen i razlicite heseve
func (mt1 *MerkleTree) Compare(mt2 *MerkleTree) []DiffPoint {
	root1 := mt1.Root
	root2 := mt2.Root

	// ako su root isti -> strukture su iste, vraca nil
	if reflect.DeepEqual(root1.Data, root2.Data) {
		//fmt.Println("Strukture su potpuno iste!")
		return nil
	}

	// root nije isti, idemo dalje
	s := 0 // br razlicitih nodova
	stack1 := []Node{}
	stack2 := []Node{}
	differences := []DiffPoint{}
	d := DiffPoint{root1.Level, root1.Pos, root1, root2}
	differences = append(differences, d)
	s++
	h := Hash([]byte{})
	hash := h[:]
	if root1.Left != nil && !reflect.DeepEqual(root1.Left.Data, hash) {
		if !reflect.DeepEqual(root1.Left.Data, root2.Left.Data) {
			differences = differences[:0]
			stack1 = append(stack1, *root1.Left)
			stack2 = append(stack2, *root2.Left)
		}
	}
	if root1.Right != nil && !reflect.DeepEqual(root1.Right.Data, hash) {
		if !reflect.DeepEqual(root1.Right.Data, root2.Right.Data) {
			differences = differences[:0]
			stack1 = append(stack1, *root1.Right)
			stack2 = append(stack2, *root2.Right)
		}
	}

	for len(stack1) > 0 {
		node1 := stack1[len(stack1)-1]
		stack1 = stack1[:len(stack1)-1]
		node2 := stack2[len(stack2)-1]
		stack2 = stack2[:len(stack2)-1]

		d := DiffPoint{node1.Level, node1.Pos, &node1, &node2}
		if node1.Left == nil && node1.Right == nil && node2.Left == nil && node2.Right == nil {
			differences = append(differences, d)
		}
		s++
		h := Hash([]byte{})
		hash := h[:]
		if node1.Left != nil && !reflect.DeepEqual(node1.Left.Data, hash) {
			if !reflect.DeepEqual(node1.Left.Data, node2.Left.Data) {
				stack1 = append(stack1, *node1.Left)
				stack2 = append(stack2, *node2.Left)
			}
		}
		if node1.Right != nil && !reflect.DeepEqual(node1.Right.Data, hash) {
			if !reflect.DeepEqual(node1.Right.Data, node2.Right.Data) {
				stack1 = append(stack1, *node1.Right)
				stack2 = append(stack2, *node2.Right)
			}
		}
	}

	// reversing the array
	for i, j := 0, len(differences)-1; i < j; i, j = i+1, j-1 {
		differences[i], differences[j] = differences[j], differences[i]
	}

	return differences
}

func (n *Node) String() string {
	return hex.EncodeToString(n.Data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

/*
// Serijalizujemo u stvari pomocnu strukturu Tree (pogledaj gore sta sadrzi)
func (mt *MerkleTree) Serialize(fileName string) {
	// *****VAZNO*****
	//var f = ""
	//f += "files%c"
	//f += fileName
	// ako testiramo ovde, iz custom maina, onda dodati: "../files%" ("../" ispred files)
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	t := Tree{allHashes, lTree}

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(t)

	if err != nil {
		panic(err)
	}
}*/

/*

	//Deserialize pomocnu strukturu Tree, iz koje izvlacimo sve

//prethodne hasheve i pravimo ispocetka MerkleTree

func (mt *MerkleTree) Deserialize(fileName string) {
	// *****VAZNO*****
	//var f = ""
	//f += "files%c"
	// += fileName         // merkletree.gob
	// ako testiramo ovde, iz custom maina, onda dodati: "../files%" ("../" ispred files)
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
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
	for i := 0; i < t.Length; i++ {
		curNode := Node{t.Hashes[i], nil, nil, 0, i}
		arr = append(arr, curNode)
	}

	cur := t.Length
	lvl := 1
	for true {
		br := 0
		arr2 := []Node{}
		for i := 0; i < len(arr); i += 2 {
			nodeLeft := arr[i]
			nodeRight := arr[i+1]
			curNode := Node{t.Hashes[cur], &nodeLeft, &nodeRight, lvl, br}
			arr2 = append(arr2, curNode)
			br++
			cur++
		}
		if len(arr2) == 1 {
			// onda je root...
			mt.Root = &arr2[0]
			break
		}
		if len(arr2)%2 == 1 {
			lastNode := Node{t.Hashes[cur], nil, nil, lvl, len(arr2)}
			arr2 = append(arr2, lastNode)
			cur++
		}
		lvl++
		arr = arr2
	}
}
*/

/*
func (mt *MerkleTree) ToBytes() ([]byte, error) {

	t := Tree{allHashes, lTree}

	var network bytes.Buffer
	enc := gob.NewEncoder(&network)

	err := enc.Encode(t)
	if err != nil {
		return nil, err
	}

	return network.Bytes(), nil
}*/

/*
func (mt *MerkleTree) FromBytes(bytess []byte) error {

	t := Tree{}

	network := bytes.NewBuffer(bytess)
	dec := gob.NewDecoder(network)

	err := dec.Decode(&t)

	if err != nil {

		arr := []Node{}
		for i := 0; i < t.Length; i++ {
			curNode := Node{t.Hashes[i], nil, nil, 0, i}
			arr = append(arr, curNode)
		}

		cur := t.Length
		lvl := 1
		for true {
			br := 0
			arr2 := []Node{}
			for i := 0; i < len(arr); i += 2 {
				nodeLeft := arr[i]
				nodeRight := arr[i+1]
				curNode := Node{t.Hashes[cur], &nodeLeft, &nodeRight, lvl, br}
				arr2 = append(arr2, curNode)
				br++
				cur++
			}
			if len(arr2) == 1 {
				// onda je root...
				mt.Root = &arr2[0]
				break
			}
			if len(arr2)%2 == 1 {
				lastNode := Node{t.Hashes[cur], nil, nil, lvl, len(arr2)}
				arr2 = append(arr2, lastNode)
				cur++
			}
			lvl++
			arr = arr2
		}

		return err
	}

	return nil
}*/

// Serijalizujemo u stvari pomocnu strukturu Tree (pogledaj gore sta sadrzi)
func (mt *MerkleTree) Serialize(path string) {

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	bytess, _ := mt.ToBytes()
	_, err = file.Write(bytess)
	if err != nil {
		panic(err)
	}
}

func (mt *MerkleTree) ToBytes() ([]byte, error) {

	t := Tree{allHashes, lTree}
	data := make([]byte, 0)

	// Serialize Length
	lengthBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(lengthBytes, uint64(t.Length))
	data = append(data, lengthBytes...)

	// Serialize Hashes
	for _, hash := range t.Hashes {
		hashSizeBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(hashSizeBytes, uint64(len(hash)))
		data = append(data, hashSizeBytes...)

		data = append(data, hash...)
	}

	return data, nil
}

// Deserialize pomocnu strukturu Tree, iz koje izvlacimo sve
// prethodne hasheve i pravimo ispocetka MerkleTree
func (mt *MerkleTree) Deserialize(path string) error {

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
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

	mt.FromBytes(data)
	return nil
}

func (mt *MerkleTree) FromBytes(bytess []byte) error {
	t := Tree{}

	t.Length = int(binary.LittleEndian.Uint64(bytess[:8]))
	bytess = bytess[8:]

	allHashes = make([][]byte, t.Length*t.Length)
	s := 0

	curLen := t.Length
	for true {
		if curLen == 0 {
			break
		}
		s += curLen
		curLen = curLen / 2
		if curLen%2 == 1 && curLen != 1 {
			curLen++
		}
	}

	for i := 0; i < s; i++ {
		hashSize := binary.LittleEndian.Uint64(bytess[:8])
		bytess = bytess[8:]
		allHashes[i] = bytess[:hashSize]
		bytess = bytess[hashSize:]
	}

	t.Hashes = make([][]byte, s)

	for i := 0; i < s; i++ {
		t.Hashes[i] = allHashes[i]
	}

	arr := []Node{}
	for i := 0; i < t.Length; i++ {
		curNode := Node{t.Hashes[i], nil, nil, 0, i}
		arr = append(arr, curNode)
	}

	cur := t.Length
	lvl := 1
	for true {
		br := 0
		arr2 := []Node{}
		for i := 0; i < len(arr); i += 2 {
			nodeLeft := arr[i]
			nodeRight := arr[i+1]
			curNode := Node{t.Hashes[cur], &nodeLeft, &nodeRight, lvl, br}
			arr2 = append(arr2, curNode)
			br++
			cur++
		}
		if len(arr2) == 1 {
			// onda je root...
			mt.Root = &arr2[0]
			break
		}
		if len(arr2)%2 == 1 {
			lastNode := Node{t.Hashes[cur], nil, nil, lvl, len(arr2)}
			arr2 = append(arr2, lastNode)
			cur++
		}
		lvl++
		arr = arr2
	}

	return nil
}

/*
func main() {
	arr1 := make([][]byte, 10, 100)
	var mt1 = MerkleTree{}

	var elem1 = []byte("1")
	var elem2 = []byte("2")
	var elem3 = []byte("3")
	var elem4 = []byte("4")
	var elem5 = []byte("6")

	arr1[0] = elem1
	arr1[1] = elem2
	arr1[2] = elem3
	arr1[3] = elem4
	arr1[4] = elem5

	mt1.Init(arr1)
	mt1.Serialize("merkletree1.db")

	arr2 := make([][]byte, 10, 100)
	var mt2 = MerkleTree{}

	var elem6 = []byte("1")
	var elem7 = []byte("2")
	var elem8 = []byte("2")
	var elem9 = []byte("4")
	var elem10 = []byte("5")

	arr2[0] = elem6
	arr2[1] = elem7
	arr2[2] = elem8
	arr2[3] = elem9
	arr2[4] = elem10

	mt2.Init(arr2)
	mt2.Serialize("merkletree2.db")

	var mt11 = MerkleTree{}
	mt11.Deserialize("merkletree1.db")
	fmt.Println(mt11)


	var mt12 = MerkleTree{}
	mt12.Deserialize("merkletree2.db")

	fmt.Println()
	fmt.Println(mt11.Compare(&mt12))
	fmt.Println()
}
*/
