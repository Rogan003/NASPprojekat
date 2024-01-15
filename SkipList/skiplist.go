// package main -> za provjeriti ovde
// package SkipList -> za main.go
package SkipList

import (
	"fmt"
	"math/rand"
	"encoding/binary"
	"hash/crc32"
)


type Data struct {
	Timestamp   uint64
	Tombstone   bool
	Key   string
	Value []byte
}

func (data Data) ToBytes() []byte {
	var dataBytes []byte

	crcb := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcb, crc32.ChecksumIEEE(data.Value))
	dataBytes = append(dataBytes, crcb...) //dodaje se CRC

	secb := make([]byte, 8)
	binary.LittleEndian.PutUint64(secb, uint64(data.Timestamp))
	dataBytes = append(dataBytes, secb...) //dodaje se Timestamp

	//1 - deleted; 0 - not deleted
	//dodaje se Tombstone
	if data.Tombstone {
		var delb byte = 1
		dataBytes = append(dataBytes, delb)
	} else {
		var delb byte = 0
		dataBytes = append(dataBytes, delb)
	}

	keyb := []byte(data.Key)
	keybs := make([]byte, 8)
	binary.LittleEndian.PutUint64(keybs, uint64(len(keyb)))

	valuebs := make([]byte, 8)
	binary.LittleEndian.PutUint64(valuebs, uint64(len(data.Value)))

	//dodaju se Key Size i Value Size
	dataBytes = append(dataBytes, keybs...)
	dataBytes = append(dataBytes, valuebs...)
	//dodaju se Key i Value
	dataBytes = append(dataBytes, keyb...)
	dataBytes = append(dataBytes, data.Value...)

	return dataBytes
}


/* JEDAN CVOR, sadrzi:
   elem - vrijednost koju prosledjujemo u SkipListu
   left, right, down, up - pokazivaci na cvorove oko njega */
type SkipNode struct {
	Elem  *Data
	Left  *SkipNode
	Right *SkipNode
	Down  *SkipNode
	Up    *SkipNode
}

/* StartNode - pocetak, pocetni cvor (gore lijevo, vidi sliku na prezentaciji)
   maxHeight - maksimalna visina skip liste */
type SkipList struct {
	StartNode *SkipNode
	EndNode   *SkipNode
	maxHeight int
	CurHeight int
}

// KONSTRUKTOR ZA SKIP LISTU 
/* - prosledjujemo jedino maxHeight, koji je maksimalna visina strukture
   - na pocetku pravimo StartNode (desni pocetni Node) i EndNode (lijevi pocetni Node)
     i povezujemo ih, to je prvi nivo
   - curHeight = sluzi za pamcenje na kom smo trenutno nivou, na pocetku je to 1. nivo */
func (sl *SkipList) Init(maxHeight int) {
	sl.maxHeight = maxHeight

	data1 := Data{0, false, "", make([]byte, 0)}
	data2 := Data{0, false, "\x7F", make([]byte, 0)}
	var leftNode = SkipNode{&data1, nil, nil, nil, nil}
	var rightNode = SkipNode{&data2, nil, nil, nil, nil}
	leftNode.Right = &rightNode
	rightNode.Left = &leftNode

	sl.StartNode = &leftNode
	sl.EndNode = &rightNode

	sl.CurHeight = 1
}

func (sl SkipList) Find(key string) (*SkipNode, bool) {
	// pretragu pocinjemo od prvog desnog gornjeg cvora
	sn := sl.StartNode

	for sn != nil { /*
		- ako je element desnog cvora od trenutnog cvora == elementu koji trazimo,
		  stajemo sa pretragom, i silazimo vertikalno dole dok ne nadjemoc
		  poslednji cvor u poslednjem nivou koji je jednak elementu kojeg trazimo */
		if sn.Right.Elem.Key == key {
			sn = sn.Right
			for sn.Down != nil {
				sn = sn.Down
			}
			return sn, true
		} /*

		- ako je element desnog cvora od trenutnog cvora < elementa koji trazimo,
		  nastavljamo desno sa pretragom */
		if sn.Right.Elem.Key < key {
			sn = sn.Right
			continue
		} /*

		- ako je element desnog cvora od trenutnog cvora > elementa koji trazimo,
		  nastavljamo dole sa pretragom, ako je donji u tom trenutku nil,
		  vracamo taj trenutni koji nije nil  */
		if sn.Right.Elem.Key > key {
			if sn.Down == nil {
				return sn, false
			}
			sn = sn.Down
			continue
		}
	}
	return nil, false
}

func (sl *SkipList) Add(key string, value []byte, timestamp uint64) (bool) {
	sn, found := sl.Find(key)
	if found == true { // ako postoji vec, ne dodajemo ga
		if sn.Elem.Tombstone == true {
			sn.Elem.Tombstone = false
		}
		sn.Elem.Value = value
		sn.Elem.Timestamp = timestamp
		return false
	} else { /*
		- posto element ne postoji, sn pokazuje na prvi manji iza njega
		- kreiramo novi cvor, pravimo sledece veze:
		  sn <-- newSkipNode --> sn.Right
		- povezujemo u suprotnom pravcu:
		  random <-> sn <-> newSkipNode <-> sn.Right */
		data := Data{timestamp, false, key, value}
		newSkipNode := SkipNode{&data, sn, sn.Right, nil, nil}
		sn.Right.Left = &newSkipNode
		sn.Right = &newSkipNode /*
			- povezali smo sve kako treba, sada roll-ujemo da vidimo koliko
			  levela idemo gore
			- leftNode = lijevi cvor od novog dodanog cvora
			- rightNode = desni cvor od novog dodanog cvora
			  ( leftNode <-> nas novi dodani cvor <-> rightNode) */
		levels := sl.roll()
		leftNode := newSkipNode.Left
		rightNode := newSkipNode.Right
		for levels > 0 {
			prevNode := leftNode.Right /*
			- da bismo dodali na k-ti level nas novi cvor kako treba,
			  krecemo prvo od lijevog cvora (leftNode) i trazimo
			  prvi lijevi i desni cvor izmedju novog cvora na tom levelu,
			  kako bismo mogli izmedju njih da dodamo nas novi cvor na k-ti level. */
			for leftNode.Up == nil {
				if leftNode.Left == nil { /*
					- ovo je slucaj kada dodjemo do startNode, a iznad i lijevo od njega
					  nema cvorova
					- pravimo novi startNode i povezujemo ga sa prethodnim startNode-om
					- pravimo novi endNode i povezujemo ga sa prethodnim endNode-om
					- povezujemo novi startNode i endNode
					- povecavamo trenutnu visinu za += 1 */
					data1 := Data{0, false, "", make([]byte, 0)}
					data2 := Data{0, false, "\x7F", make([]byte, 0)}
					var newStartNode = SkipNode{&data1, nil, nil, sl.StartNode, nil}
					var newEndNode = SkipNode{&data2, nil, nil, sl.EndNode, nil}

					sl.StartNode.Up = &newStartNode
					sl.StartNode = &newStartNode
					sl.EndNode.Up = &newEndNode
					sl.EndNode = &newEndNode
					sl.StartNode.Right = sl.EndNode
					sl.EndNode.Left = sl.StartNode

					sl.CurHeight++
					break
				}
				leftNode = leftNode.Left
			}
			leftNode = leftNode.Up

			// nastavljamo sa desnom stranom
			for rightNode.Up == nil {
				rightNode = rightNode.Right
			}
			rightNode = rightNode.Up

			newestSkipNode := SkipNode{&data, leftNode, rightNode, prevNode, nil}
			prevNode.Up = &newestSkipNode
			leftNode.Right = &newestSkipNode
			rightNode.Left = &newestSkipNode

			levels--
		}
		return true
	}
}

func (sl *SkipList) Delete(key string, timestamp uint64) (bool) {
	sn, found := sl.Find(key)
	if found != true {
		// ako ne postoji, nema potrebe da ga brisemo
		return false
	} else {
		sn.Elem.Tombstone = true
		sn.Elem.Timestamp = timestamp
		return true
	}
}
func (sl *SkipList) DeletePhysically(key string) {
	sn, found := sl.Find(key)
	if found != true {
		// ako ne postoji, nema potrebe da ga brisemo
		return
	} else {
		leftNode := sn.Left
		rightNode := sn.Right
		leftNode.Right = rightNode
		rightNode.Left = leftNode
		/* moze i bez ovog ispod, jer niko iz skipliste vise ne
		   pokazuje na taj obrisani node */
		sn.Right = nil
		sn.Left = nil
		sn.Down = nil
		for sn.Up != nil {
			sn = sn.Up
			leftNode := sn.Left
			rightNode := sn.Right
			leftNode.Right = rightNode
			rightNode.Left = leftNode
			/* moze i bez ovog ispod, jer niko iz skipliste vise ne
			   pokazuje na taj obrisani node */
			sn.Right = nil
			sn.Left = nil
			sn.Down = nil
		}
	}
}

// funkcija koja vraca sve elemente sortirane
func (sl *SkipList) AllElem() ([]*Data) {
	elements := make([]*Data, 0)

	sn, _ := sl.Find("")
	for sn.Right != nil {
		if (sn.Right.Elem.Key == "\x7F") {
			break
		}
		sn = sn.Right
		elements = append(elements, sn.Elem)
	}

	return elements
}

/* - sluzi samo za prikaz malo cvorova (meni odgovara 12 levela/10 u sirinu),
     moze se staviti i na vise levela, ali ovako je preglednije.
	- samo da bi se provjerilo da li sve okej izgleda/radi.
	- vraca niz vrijednosti */
func (sl *SkipList) ToVisual() {
	sn := sl.StartNode
	for sn.Down != nil {
		sn = sn.Down
	}
	var n[12][10]string
	x := 10
	y := 0
	for sn != nil {
		n[x][y] = "-1000"
		y++
		r := sn.Right
		for r != nil {
			var toAdd string = "0"
			if r.Elem.Key == "" {
				toAdd = "-1000"
			} else if r.Elem.Key == "\x7F" {
				toAdd = "1000"
			} else {
				toAdd = r.Elem.Key
			}
			if r.Down != nil {
				if n[x+1][y] != toAdd {
					n[x][y] = "0"
					y++
					continue
				}
			}
			n[x][y] = toAdd
			r = r.Right
			y++
		}
		x--
		y = 0
		sn = sn.Up
	}
	
	for i := 0; i < 12; i++ {
		for j := 0; j < 10; j++ {
			if ((n[i][j] == "-1000") || (n[i][j] == "1000") || (n[i][j] == "0")) {
				fmt.Print(n[i][j], " ")
			} else {
				sn, found := sl.Find(n[i][j])
				if (!found) {
					fmt.Print("0 ")
					continue
				}
				fmt.Print("(", sn.Elem.Key, ", ", sn.Elem.Tombstone, ", ", sn.Elem.Timestamp, ", ", sn.Elem.Value, ") ")
			}
		} 
		fmt.Println()
	}
}


func (s *SkipList) roll() int {
	level := 0
	/* possible ret values from rand are 0 and 1
	   we stop when we get a 0 */
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxHeight {
			return level
		}
	}
	return level
}


/*
func main() {

	// var sl = SkipList.SkipList{}

	var sl = SkipList{}
	sl.Init(10)

	fmt.Println()
	fmt.Println("Dodajem 1. put 5: ")
	b1 := []byte{1}
	sl.Add("5", b1, 1)
	sl.ToVisual()

	fmt.Println()
	fmt.Println("Brisem 5: ")
	sl.Delete("5")
	sl.ToVisual()
	
	fmt.Println()
	fmt.Println("Dodajem 2. put 5: ")
	b2 := []byte{2}
	sl.Add("5", b2, 2)
	sl.ToVisual()
	
	fmt.Println()
	fmt.Println("Dodajem 1. put 6: ")
	b3 := []byte{2}
	sl.Add("6", b3, 3)
	sl.ToVisual()

	fmt.Println()
	fmt.Println("Dodajem 2. put 6: ")
	sl.Add("6", b3, 3)
	sl.ToVisual()

	fmt.Println()
	fmt.Println("Dodajem 3. put 6 (izmijenjen): ")
	b4 := []byte{5}
	sl.Add("6", b4, 3)
	sl.ToVisual()

	fmt.Println()
	fmt.Println("Brisem 5: ")
	sl.Delete("5")
	sl.ToVisual()

	fmt.Println()
	fmt.Println("Dodajem 1. put 3: ")
	b5 := []byte{3}
	sl.Add("3", b5, 4)
	sl.ToVisual()

	fmt.Println()
	fmt.Println("Dodajem 1. put 4: ")
	b6 := []byte{4}
	sl.Add("4", b6, 5)
	sl.ToVisual()

	fmt.Println()
	fmt.Println("Brisem 7: ")
	sl.Delete("7")
	sl.ToVisual()

	fmt.Println()
	var sn1, found = sl.Find("5")
	fmt.Println("Find(5) = ", found, "  sn1 = (time:", sn1.Elem.Timestamp, ", tombstone:", sn1.Elem.Tombstone, ", key:", sn1.Elem.Key, ", value:", sn1.Elem.Value)
	
	fmt.Println()
	var sn2, found2 = sl.Find("3")
	fmt.Println("Find(3) = ", found2, "  sn2 = (time:", sn2.Elem.Timestamp, ", tombstone:", sn2.Elem.Tombstone, ", key:", sn2.Elem.Key, ", value:", sn2.Elem.Value)

	fmt.Println()
	var sn3, found3 = sl.Find("7")
	fmt.Println("Find(7) = ", found3, "  sn3 = (time:", sn3.Elem.Timestamp, ", tombstone:", sn3.Elem.Tombstone, ", key:", sn3.Elem.Key, ", value:", sn3.Elem.Value)

	fmt.Println()
	fmt.Println("Dodajem 1. put 7: ")
	b7 := []byte{7}
	sl.Add("7", b7, 6)
	sl.ToVisual()

	fmt.Println()
	var sn4, found4 = sl.Find("7")
	fmt.Println("Find(7) = ", found4, "  sn4 = (time:", sn4.Elem.Timestamp, ", tombstone:", sn4.Elem.Tombstone, ", key:", sn4.Elem.Key, ", value:", sn4.Elem.Value)
	fmt.Println()

	elems := sl.AllElem()
	for _, data := range elems {
		fmt.Println("Data:", *data)
   }

	fmt.Println()
	fmt.Println()
	fmt.Println("Odradio sve!!")
	fmt.Println()
}
*/