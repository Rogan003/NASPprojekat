package SkipList

import (
	"math/rand"
	"math"
)


// JEDAN CVOR, sadrzi:
// elem - vrijednost koju prosledjujemo u SkipListu
// left, right, down, up - pokazivaci na cvorove oko njega
type SkipNode struct { 
	Elem	float64
	Left 	*SkipNode
	Right *SkipNode
	Down 	*SkipNode
	Up 	*SkipNode
}
// StartNode - pocetak, pocetni cvor (gore lijevo, vidi sliku na prezentaciji)
// maxHeight - maksimalna visina skip liste
type SkipList struct {
	StartNode *SkipNode
	EndNode *SkipNode
	maxHeight int
	CurHeight int
}

// KONSTRUKTOR ZA SKIP LISTU
/*
prosledjujemo jedino maxHeight, koji je maksimalna visina strukture
- na pocetku pravimo StartNode (desni pocetni Node) i EndNode (lijevi pocetni Node)
  i povezujemo ih, to je prvi nivo
- curHeight = sluzi za pamcenje na kom smo trenutno nivou, na pocetku je to 1. nivo
*/
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


func (sl SkipList) find(elem float64) (*SkipNode, bool) {
	// pretragu pocinjemo od prvog desnog gornjeg cvora
	sn := sl.StartNode
	for (sn != nil) {
		/*
		- ako je element desnog cvora od trenutnog cvora == elementu koji trazimo,
		  stajemo sa pretragom, i silazimo vertikalno dole dok ne nadjemoc
		  poslednji cvor u poslednjem nivou koji je jednak elementu kojeg trazimo
		*/
		if ((*sn.Right).Elem == elem) {
			sn = sn.Right
			for (sn.Down != nil) {
				sn = sn.Down
			}
			// Dodati petlju da nadje prvi donji element
			return sn, true
		} 
		/*
		- ako je element desnog cvora od trenutnog cvora < elementa koji trazimo,
		  nastavljamo desno sa pretragom
		*/
		if ((*sn.Right).Elem < elem) {
			sn = sn.Right
			continue
		} 
		/*
		- ako je element desnog cvora od trenutnog cvora > elementa koji trazimo,
		  nastavljamo dole sa pretragom, ako je donji u tom trenutku nil,
		  vracamo taj trenutni koji nije nil
		*/
		if ((*sn.Right).Elem > elem) {
			if (sn.Down == nil) { 
				return sn, false
			}
			sn = sn.Down;
			continue;
		}
	}
	return nil, false
}

func (sl *SkipList) add(elem float64) {
	sn, found := sl.find(elem)
	if (found == true) {
		return
	} else {
		newSkipNode := SkipNode{elem, sn, sn.Right, nil, nil}
		sn.Right.Left = &newSkipNode
		sn.Right = &newSkipNode

		levels := sl.roll()
		leftNode := newSkipNode.Left
		rightNode := newSkipNode.Right
		for (levels > 0) {
			// krecemo od lijeve strane
			for (leftNode.Up == nil) {
				if (leftNode.Left == nil) {    // dosli smo do -inf startNode, dodati novi -inf gore lijevo
					var newStartNode = SkipNode{math.Inf(-1), nil, nil, sl.StartNode, nil}
					var newEndNode = SkipNode{math.Inf(+1), nil, nil, sl.EndNode, nil}
					
					sl.StartNode.Up = &newStartNode
					sl.StartNode = &newStartNode
					sl.EndNode.Up = &newEndNode
					sl.EndNode = &newEndNode
					sl.StartNode.Right = &newEndNode
					sl.EndNode.Left = &newStartNode

					sl.CurHeight++
					break
				}
				leftNode = leftNode.Left
			}
			leftNode = leftNode.Up;

			// nastavljamo sa desnom stranom
			for (rightNode.Up == nil) {
				rightNode = rightNode.Right;
			}
			rightNode = rightNode.Up;

			newestSkipNode := SkipNode{elem, leftNode, rightNode, &newSkipNode, nil}
			leftNode.Right = &newestSkipNode
			rightNode.Left = &newestSkipNode
			newSkipNode = newestSkipNode
			
			levels--
		}
	}
}

func (sl *SkipList) delete(elem float64) {
	sn, found := sl.find(elem)
	if (found != true) {
		return
	} else {
		leftNode := sn.Left
		rightNode := sn.Right
		leftNode.Right = rightNode
		rightNode.Left = leftNode
		// moze i bez ovog ispod, jer niko iz skipliste vise ne 
		// pokazuje na taj obrisani node
		sn.Right = nil
		sn.Left = nil
		sn.Down = nil
		sn = nil
		for (sn.Up != nil) {
			sn = sn.Up
			leftNode := sn.Left
			rightNode := sn.Right
			leftNode.Right = rightNode
			rightNode.Left = leftNode
			// moze i bez ovog ispod, jer niko iz skipliste vise ne 
			// pokazuje na taj obrisani node
			sn.Right = nil
			sn.Left = nil
			sn.Down = nil
			sn = nil
		}
	}
}


func (s *SkipList) roll() int {
	level := 0
	// possible ret values from rand are 0 and 1
	// we stop when we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxHeight {
			return level
		}
	}
	return level
}

/*
func main() {
	s := SkipList{maxHeight: 3}
	for i := 0; i < 10; i++ {
		fmt.Println(s.roll())
	}
}
*/



// nemoj gledati - ne treba

/* // NIKADA SE NECE DESITI!
if (rightNode.Right == nil) {    // dosli smo do +inf endNode, dodati novi +inf gore desno
	newEndNode := SkipNode{math.Inf(1), nil, nil, sl.EndNode, nil}
	sl.EndNode.Up = &newEndNode
	sl.EndNode = &newEndNode
	break
}
*/