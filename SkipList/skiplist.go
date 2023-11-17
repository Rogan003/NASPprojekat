// package main -> za provjeriti ovde
package SkipList

import (
	//"fmt"
	"math"
	"math/rand"
)


/* JEDAN CVOR, sadrzi:
   elem - vrijednost koju prosledjujemo u SkipListu
   left, right, down, up - pokazivaci na cvorove oko njega */
type SkipNode struct {
	Elem  float64
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

	var leftNode = SkipNode{math.Inf(-1), nil, nil, nil, nil}
	var rightNode = SkipNode{math.Inf(1), nil, nil, nil, nil}
	leftNode.Right = &rightNode
	rightNode.Left = &leftNode

	sl.StartNode = &leftNode
	sl.EndNode = &rightNode

	sl.CurHeight = 1
}

func (sl SkipList) Find(elem float64) (*SkipNode, bool) {
	// pretragu pocinjemo od prvog desnog gornjeg cvora
	sn := sl.StartNode

	for sn != nil { /*
		- ako je element desnog cvora od trenutnog cvora == elementu koji trazimo,
		  stajemo sa pretragom, i silazimo vertikalno dole dok ne nadjemoc
		  poslednji cvor u poslednjem nivou koji je jednak elementu kojeg trazimo */
		if sn.Right.Elem == elem {
			sn = sn.Right
			for sn.Down != nil {
				sn = sn.Down
			}
			return sn, true
		} /*

		- ako je element desnog cvora od trenutnog cvora < elementa koji trazimo,
		  nastavljamo desno sa pretragom */
		if sn.Right.Elem < elem {
			sn = sn.Right
			continue
		} /*

		- ako je element desnog cvora od trenutnog cvora > elementa koji trazimo,
		  nastavljamo dole sa pretragom, ako je donji u tom trenutku nil,
		  vracamo taj trenutni koji nije nil  */
		if sn.Right.Elem > elem {
			if sn.Down == nil {
				return sn, false
			}
			sn = sn.Down
			continue
		}
	}
	return nil, false
}

func (sl *SkipList) Add(elem float64) {
	sn, found := sl.Find(elem)
	if found == true { // ako postoji vec, ne dodajemo ga
		return
	} else { /*
		- posto element ne postoji, sn pokazuje na prvi manji iza njega
		- kreiramo novi cvor, oravimo sledece veze:
		  sn <-- newSkipNode --> sn.Right
		- povezujemo u suprotnom pravcu:
		  random <-> sn <-> newSkipNode <-> sn.Right */
		newSkipNode := SkipNode{elem, sn, sn.Right, nil, nil}
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
					var newStartNode = SkipNode{math.Inf(-1), nil, nil, sl.StartNode, nil}
					var newEndNode = SkipNode{math.Inf(+1), nil, nil, sl.EndNode, nil}

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

			newestSkipNode := SkipNode{elem, leftNode, rightNode, prevNode, nil}
			prevNode.Up = &newestSkipNode
			leftNode.Right = &newestSkipNode
			rightNode.Left = &newestSkipNode

			levels--
		}
	}
}

func (sl *SkipList) Delete(elem float64) {
	sn, found := sl.Find(elem)
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


/* - sluzi samo za prikaz malo cvorova (meni odgovara 12 levela/10 u sirinu),
     moze se staviti i na vise levela, ali ovako je preglednije.
	- samo da bi se provjerilo da li sve okej izgleda/radi.
	- vraca niz vrijednosti */
func (sl *SkipList) ToVisual() [12][10]float64 {
	sn := sl.StartNode
	for sn.Down != nil {
		sn = sn.Down
	}
	var n [12][10]float64
	x := 10
	y := 0
	for sn != nil {
		n[x][y] = -1000
		y++
		r := sn.Right
		for r != nil {
			var toAdd float64 = 0
			if r.Elem == math.Inf(-1) {
				toAdd = -1000
			} else if r.Elem == math.Inf(1) {
				toAdd = 1000
			} else {
				toAdd = r.Elem
			}
			if r.Down != nil {
				if n[x+1][y] != toAdd {
					n[x][y] = 0
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
	return n
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
 
	var sl = SkipList.SkipList{}
	sl.Init(10)

	fmt.Println()
	fmt.Println("Dodajem 1. put 5: ")
	sl.Add(5)
	var n = sl.ToVisual()
	for i := 0; i < 12; i++ {
		fmt.Println(n[i])
	}

	fmt.Println()
	fmt.Println("Brisem 5: ")
	sl.Delete(5)
	n = sl.ToVisual()
	for i := 0; i < 12; i++ {
		fmt.Println(n[i])
	}

	fmt.Println()
	fmt.Println("Dodajem 2. put 5: ")
	sl.Add(5)
	n = sl.ToVisual()
	for i := 0; i < 12; i++ {
		fmt.Println(n[i])
	}
	
	fmt.Println()
	fmt.Println("Dodajem 1. put 6: ")
	sl.Add(6)
	n = sl.ToVisual()
	for i := 0; i < 12; i++ {
		fmt.Println(n[i])
	}

	fmt.Println()
	fmt.Println("Dodajem 2. put 6: ")
	sl.Add(6)
	n = sl.ToVisual()
	for i := 0; i < 12; i++ {
		fmt.Println(n[i])
	}

	fmt.Println()
	fmt.Println("Brisem 5: ")
	sl.Delete(5)
	n = sl.ToVisual()
	for i := 0; i < 12; i++ {
		fmt.Println(n[i])
	}

	fmt.Println()
	fmt.Println("Dodajem 1. put 3: ")
	sl.Add(3)
	n = sl.ToVisual()
	for i := 0; i < 12; i++ {
		fmt.Println(n[i])
	}

	fmt.Println()
	fmt.Println("Dodajem 1. put 4: ")
	sl.Add(4)
	n = sl.ToVisual()
	for i := 0; i < 12; i++ {
		fmt.Println(n[i])
	}

	fmt.Println()
	fmt.Println("Brisem 7: ")
	sl.Delete(7)
	n = sl.ToVisual()
	for i := 0; i < 12; i++ {
		fmt.Println(n[i])
	}

	fmt.Println()
	var _, found = sl.Find(5)
	fmt.Println("Find(5) = ", found)
	
	fmt.Println()
	var _, found2 = sl.Find(3)
	fmt.Println("Find(3) = ", found2)

	fmt.Println()
	var _, found3 = sl.Find(7)
	fmt.Println("Find(7) = ", found3)

	fmt.Println()
	fmt.Println("Dodajem 1. put 7: ")
	sl.Add(7)
	n = sl.ToVisual()
	for i := 0; i < 12; i++ {
		fmt.Println(n[i])
	}

	fmt.Println()
	var _, found4 = sl.Find(7)
	fmt.Println("Find(7) = ", found4)

	fmt.Println()
	fmt.Println()
	fmt.Println("Odradio sve!!")
	fmt.Println()
}
*/