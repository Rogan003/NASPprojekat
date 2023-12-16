package BTree

import (
	//"fmt"
	"math"
)

// struktura podatka, mahom preuzeta sa WAL-a ono sto nam treba za memtable
type Data struct {
	Timestamp   uint64
	Tombstone   bool
	Key   string
	Value []byte
}

/*	
	STRUKTURA B STABLO

	Pokrivamo dve strukture, cvor B stabla i samo stablo
	Cvor B stabla sadrzi niz kljuceva i niz pokazivaca na decu (ostale cvorove)
	B stablo kao strutkura sadrzi informaciju o maksimalnom broju dece i o korenskom cvoru stabla
*/

type BTreeNode struct {
	keys	[]*Data
	children	[]*BTreeNode
	parent	*BTreeNode
}

type BTree struct {
	Root	*BTreeNode
	maxKids	int
	minKids int
}

// Konstruktor za B stablo, kreira B stablo sa zadatim maximalnim brojem dece i praznim korenskim cvorem
func (btree *BTree) Init(max int) {
	btree.maxKids = max
	btree.minKids = int(math.Ceil(float64(btree.maxKids / 2)))
	node := BTreeNode{make([]*Data, 0, max - 1), nil, nil}
	btree.Root = &node
}

// Funkcija pretrage, za zadati element prolazi kroz stablo i trazi gde on treba da bude
// vraca cvor i ako je zadati element tu vraca true, a ako nije vraca cvor pre kog treba dodati i false
func (btree *BTree) Find(elem string) (*BTreeNode, int, bool, *Data) {
	iterNode := btree.Root

	for true {
		indexFound := false

		for index, key := range iterNode.keys {
			if key.Key == elem {
				return iterNode, index, true, key
			} else if key.Key > elem {
				if iterNode.children != nil {
					iterNode = iterNode.children[index]
					indexFound = true
					break
				} else {
					return iterNode, index, false, nil
				}
			}
		}

		if !indexFound {
			if iterNode.children != nil {
				iterNode = iterNode.children[len(iterNode.children) - 1]
			} else {
				return iterNode, len(iterNode.keys), false, nil
			}
		}
	}

	return iterNode, 0, false, nil
}

// Pomocna funkcija za razdvajanje cvora
func (btree *BTree) splitNode(node *BTreeNode) { 
	index := int(btree.maxKids / 2)
	parentIndex := 0
	
	// prebaci u parent node element na indexu
	if(btree.Root == node) {
		// moramo novi root da imamo, ako je pun trenutni root
		newRoot := BTreeNode{make([]*Data, 1, btree.maxKids - 1), make([]*BTreeNode, 1, btree.maxKids), nil}
		newRoot.keys[0] = node.keys[index]
		btree.Root = &newRoot
		node.parent = &newRoot
	} else {
		place := 0

		for _, value := range node.parent.keys {
			if value.Key > node.keys[index].Key {
				break
			}

			place++
		}

		parentIndex = place
		if len(node.parent.keys) == place {
			node.parent.keys = append(node.parent.keys, node.keys[index])
		} else {
			node.parent.keys = append(node.parent.keys[:place+1], node.parent.keys[place:]...)
			node.parent.keys[place] = node.keys[index]
		}
	}
	
	// rastavi keys i napravi dva niza koja ces dodati na mesta gde treba kao decu gore
	var nodeOne, nodeTwo BTreeNode
	one := make([]*Data, len(node.keys[:index]), btree.maxKids - 1)
	two := make([]*Data, len(node.keys[index + 1:]), btree.maxKids - 1)
	copy(one, node.keys[:index])
	copy(two, node.keys[index + 1:])
	if node.children == nil {
		nodeOne = BTreeNode{one, nil, node.parent}
		nodeTwo = BTreeNode{two, nil, node.parent}
	} else {
		oneChild := make([]*BTreeNode, len(node.children[:index + 1]), btree.maxKids)
		twoChild := make([]*BTreeNode, len(node.children[index + 1:]), btree.maxKids)
		copy(oneChild, node.children[:index + 1])
		copy(twoChild, node.children[index + 1:])
		nodeOne = BTreeNode{one, oneChild, node.parent}
		nodeTwo = BTreeNode{two, twoChild, node.parent}

		for _, value := range oneChild {
			value.parent = &nodeOne
		}

		for _, value := range twoChild {
			value.parent = &nodeTwo
		}
	}
	
	node.parent.children[parentIndex] = &nodeOne
	parentIndex++
	if len(node.parent.children) == parentIndex {
		node.parent.children = append(node.parent.children, &nodeTwo)
	} else {
		node.parent.children = append(node.parent.children[:parentIndex+1], node.parent.children[parentIndex:]...)
		node.parent.children[parentIndex] = &nodeTwo
	}

	if len(node.parent.keys) == btree.maxKids {
		btree.splitNode(node.parent)
	}
}

// Funkcija za dodavanje elementa u B stablo
func (btree *BTree) Add(elem string, val []byte, ts uint64) {
	node, indexVal, isThere, dat := btree.Find(elem)

	if isThere {
		if val == nil {
			if !dat.Tombstone {
				dat.Tombstone = true
			} else {
				// greska, vec obrisan element
			}
		} else {
			dat.Value = val
			dat.Timestamp = ts
		}
	} else {
		data := Data{ts, false, elem, val}
		// dodavanje elementa u kljuceva cvora u listu na mesto gde treba (ovo je u sustini insert)
		if len(node.keys) == indexVal {
			node.keys = append(node.keys, &data)
		} else {
			node.keys = append(node.keys[:indexVal+1], node.keys[indexVal:]...)
			node.keys[indexVal] = &data
		}

		// ako je broj kljuceva sada veci od dozvoljenog(tj veci jednak maksimalnom dozvoljenom broju dece), radimo rotacije ili split
		if len(node.keys) >= btree.maxKids {
			done := false

			first := -1
			last := -1
			nodeIndex := -1

			// izvlacimo najblizu bracu sa brojem elemenata manjima od maximuma
			if node.parent != nil {
				for index, value := range node.parent.children {
					if value == node {
						nodeIndex = index
					} else if len(value.keys) < (btree.maxKids - 1) {
						done = true

						if nodeIndex != -1 {
							last = index
							break
						} else {
							first = index
						}
					}
				}
			}

			if done {
				// odredjivanje koji je blizi, tj gde je lakse odgurati element
				if first != -1 && last != -1 {
					if (nodeIndex - first) < (last - nodeIndex) {
						last = -1
					} else {
						first = -1
					}
				}

				if first == -1 {
					// poslednji elem insert u node.parent.keys
					if len(node.parent.keys) == nodeIndex {
						node.parent.keys = append(node.parent.keys, node.parent.children[nodeIndex].keys[len(node.parent.children[nodeIndex].keys) - 1])
					} else {
						node.parent.keys = append(node.parent.keys[:nodeIndex+1], node.parent.keys[nodeIndex:]...)
						node.parent.keys[nodeIndex] = node.parent.children[nodeIndex].keys[len(node.parent.children[nodeIndex].keys) - 1]
					}		

					// remove poslednji elem
					node.parent.children[nodeIndex].keys = node.parent.children[nodeIndex].keys[:len(node.parent.children[nodeIndex].keys) - 1]
					
					nodeIndex++

					for nodeIndex <= last {
						// elem na trazenom indexu u node.parent.keys postaje prvi u value.keys
						node.parent.children[nodeIndex].keys = append([]*Data{node.parent.keys[nodeIndex]}, node.parent.children[nodeIndex].keys...)	
		
						// remove taj elem
						node.parent.keys = append(node.parent.keys[:nodeIndex], node.parent.keys[nodeIndex+1:]...)
						
						if nodeIndex != last {
							// poslednji elem insert u node.parent.keys
							if len(node.parent.keys) == nodeIndex {
								node.parent.keys = append(node.parent.keys, node.parent.children[nodeIndex].keys[len(node.parent.children[nodeIndex].keys) - 1])
							} else {
								node.parent.keys = append(node.parent.keys[:nodeIndex+1], node.parent.keys[nodeIndex:]...)
								node.parent.keys[nodeIndex] = node.parent.children[nodeIndex].keys[len(node.parent.children[nodeIndex].keys) - 1]
							}
														
							// remove poslednji elem
							node.parent.children[nodeIndex].keys = node.parent.children[nodeIndex].keys[:len(node.parent.children[nodeIndex].keys) - 1]
						}

						nodeIndex++
					}
				} else {
					// prvi elem insert u node.parent.keys
					if len(node.parent.keys) == nodeIndex {
						node.parent.keys = append(node.parent.keys, node.parent.children[nodeIndex].keys[0])
					} else {
						node.parent.keys = append(node.parent.keys[:nodeIndex+1], node.parent.keys[nodeIndex:]...)
						node.parent.keys[nodeIndex] = node.parent.children[nodeIndex].keys[0]
					}
					
					// remove prvi elem
					node.parent.children[nodeIndex].keys = node.parent.children[nodeIndex].keys[1:]

					nodeIndex--

					for nodeIndex >= first {
						// elem na trazenom indexu u node.parent.keys postaje poslednji u value.keys
						node.parent.children[nodeIndex].keys = append(node.parent.children[nodeIndex].keys, node.parent.keys[nodeIndex])	
						
						// remove taj elem
						node.parent.keys = append(node.parent.keys[:nodeIndex], node.parent.keys[nodeIndex+1:]...)

						if nodeIndex != first {
							// prvi elem insert u node.parent.keys
							if len(node.parent.keys) == nodeIndex {
								node.parent.keys = append(node.parent.keys, node.parent.children[nodeIndex].keys[0])
							} else {
								node.parent.keys = append(node.parent.keys[:nodeIndex+1], node.parent.keys[nodeIndex:]...)
								node.parent.keys[nodeIndex] = node.parent.children[nodeIndex].keys[0]
							}
							
							// remove prvi elem
							node.parent.children[nodeIndex].keys = node.parent.children[nodeIndex].keys[1:]
						}

						nodeIndex--
					}
				}
			} else {
				btree.splitNode(node)
			}
		}
	}
}

/*
// Pomocna funkcija za spajanje cvora 
func (btree *BTree) adjustNodeDeletion(node *BTreeNode) {
	left := true
	done := true

	leftIndex := -1
	rightIndex := -1
	indexVal := 0

	// ako je moguce, pronaci cemo levog ili desnog brata od koga mozemo pozajmiti jedan element
	for index, value := range node.parent.children {
		if len(value.keys) > btree.minKids {
			if left {
				leftIndex = index
			} else {
				rightIndex = index
				break
			}
		}

		if value == node {
			indexVal = index
			left = false
		}
	}

	// u zavisnosti od toga da li smo pronasli i koji je blizi nasem cvoru, pravimo rotacije i postavljamo flegove
	if leftIndex == -1 && rightIndex == -1 {
		done = false
	} else if leftIndex == -1 {
		// desno
		tempKey := node.parent.keys[rightIndex - 1]
		node.parent.keys[rightIndex - 1] = node.parent.children[rightIndex].keys[0]
		node.parent.children[rightIndex].keys = node.parent.children[rightIndex].keys[1:]

		var newChild *BTreeNode = nil
		if node.parent.children[rightIndex].children != nil {
			newChild = node.parent.children[rightIndex].children[0]
			node.parent.children[rightIndex].children = node.parent.children[rightIndex].children[1:]
		}

		rightIndex--

		for true {
			node.parent.children[rightIndex].keys = append(node.parent.children[rightIndex].keys, tempKey)
			if newChild != nil {
				node.parent.children[rightIndex].children = append(node.parent.children[rightIndex].children, newChild)
			}

			if node.parent.children[rightIndex] == node {
				break
			} else {
				tempKey = node.parent.keys[rightIndex - 1]
				node.parent.keys[rightIndex - 1] = node.parent.children[rightIndex].keys[0]
				node.parent.children[rightIndex].keys = node.parent.children[rightIndex].keys[1:]

				if newChild != nil {
					newChild = node.parent.children[rightIndex].children[0]
					node.parent.children[rightIndex].children = node.parent.children[rightIndex].children[1:]
				}

				rightIndex--
			}
		}
	} else if rightIndex == -1 {
		// levo
		tempKey := node.parent.keys[leftIndex]
		node.parent.keys[leftIndex] = node.parent.children[leftIndex].keys[len(node.parent.children[leftIndex].keys) - 1]
		node.parent.children[leftIndex].keys = node.parent.children[leftIndex].keys[:len(node.parent.children[leftIndex].keys) - 1]
		
		var newChild *BTreeNode = nil
		if node.parent.children[leftIndex].children != nil {
			newChild = node.parent.children[leftIndex].children[len(node.parent.children[leftIndex].children) - 1]
			node.parent.children[leftIndex].children = node.parent.children[leftIndex].children[:len(node.parent.children[leftIndex].children) - 1]
		}

		leftIndex++

		for true {
			node.parent.children[leftIndex].keys = append([]int{tempKey}, node.parent.children[leftIndex].keys...)
			if newChild != nil {
				node.parent.children[leftIndex].children = append([]*BTreeNode{newChild}, node.parent.children[leftIndex].children...)
			}

			if node.parent.children[leftIndex] == node {
				break
			} else {
				tempKey = node.parent.keys[leftIndex]
				node.parent.keys[leftIndex] = node.parent.children[leftIndex].keys[len(node.parent.children[leftIndex].keys) - 1]
				node.parent.children[leftIndex].keys = node.parent.children[leftIndex].keys[:len(node.parent.children[leftIndex].keys) - 1]
				
				if newChild != nil {
					newChild = node.parent.children[leftIndex].children[len(node.parent.children[leftIndex].children) - 1]
					node.parent.children[leftIndex].children = node.parent.children[leftIndex].children[:len(node.parent.children[leftIndex].children) - 1]
				
				}
				
				leftIndex++
			}
		}
	} else if (rightIndex - indexVal) >= (indexVal - leftIndex) {
		// levo
		tempKey := node.parent.keys[leftIndex]
		node.parent.keys[leftIndex] = node.parent.children[leftIndex].keys[len(node.parent.children[leftIndex].keys) - 1]
		node.parent.children[leftIndex].keys = node.parent.children[leftIndex].keys[:len(node.parent.children[leftIndex].keys) - 1]
		
		var newChild *BTreeNode = nil
		if node.parent.children[leftIndex].children != nil {
			newChild = node.parent.children[leftIndex].children[len(node.parent.children[leftIndex].children) - 1]
			node.parent.children[leftIndex].children = node.parent.children[leftIndex].children[:len(node.parent.children[leftIndex].children) - 1]
		}

		leftIndex++

		for true {
			node.parent.children[leftIndex].keys = append([]int{tempKey}, node.parent.children[leftIndex].keys...)
			if newChild != nil {
				node.parent.children[leftIndex].children = append([]*BTreeNode{newChild}, node.parent.children[leftIndex].children...)
			}

			if node.parent.children[leftIndex] == node {
				break
			} else {
				tempKey = node.parent.keys[leftIndex]
				node.parent.keys[leftIndex] = node.parent.children[leftIndex].keys[len(node.parent.children[leftIndex].keys) - 1]
				node.parent.children[leftIndex].keys = node.parent.children[leftIndex].keys[:len(node.parent.children[leftIndex].keys) - 1]
				
				if newChild != nil {
					newChild = node.parent.children[leftIndex].children[len(node.parent.children[leftIndex].children) - 1]
					node.parent.children[leftIndex].children = node.parent.children[leftIndex].children[:len(node.parent.children[leftIndex].children) - 1]
				
				}

				leftIndex++
			}
		}
	} else {
		// desno
		tempKey := node.parent.keys[rightIndex - 1]
		node.parent.keys[rightIndex - 1] = node.parent.children[rightIndex].keys[0]
		node.parent.children[rightIndex].keys = node.parent.children[rightIndex].keys[1:]

		var newChild *BTreeNode = nil
		if node.parent.children[rightIndex].children != nil {
			newChild = node.parent.children[rightIndex].children[0]
			node.parent.children[rightIndex].children = node.parent.children[rightIndex].children[1:]
		}

		rightIndex--

		for true {
			node.parent.children[rightIndex].keys = append(node.parent.children[rightIndex].keys, tempKey)
			if newChild != nil {
				node.parent.children[rightIndex].children = append(node.parent.children[rightIndex].children, newChild)
			}

			if node.parent.children[rightIndex] == node {
				break
			} else {
				tempKey = node.parent.keys[rightIndex - 1]
				node.parent.keys[rightIndex - 1] = node.parent.children[rightIndex].keys[0]
				node.parent.children[rightIndex].keys = node.parent.children[rightIndex].keys[1:]

				if newChild != nil {
					newChild = node.parent.children[rightIndex].children[0]
					node.parent.children[rightIndex].children = node.parent.children[rightIndex].children[1:]
				}

				rightIndex--
			}
		}
	}
	
	// ako nije, spajamo cvorove
	if !done {
		if indexVal != 0 {
			// spustimo onaj na index - 1 node.parents.keys i append na kraj index - 1 node.parent.children, pa append sve kljuceve iz index
			node.parent.children[indexVal - 1].keys = append(node.parent.children[indexVal - 1].keys, node.parent.keys[indexVal - 1])
			node.parent.children[indexVal - 1].keys = append(node.parent.children[indexVal - 1].keys, node.parent.children[indexVal].keys...)
			// spajanje dece iz cvorova koje spajamo
			if node.parent.children[indexVal - 1].children != nil {
				node.parent.children[indexVal - 1].children = append(node.parent.children[indexVal - 1].children, node.parent.children[indexVal].children...)
			}
			// izbaciti index - 1 iz node.parents.keys, kao i nepotrebno dete jedno iz children (node.parent)
			node.parent.keys = append(node.parent.keys[:indexVal - 1], node.parent.keys[indexVal:]...)
			node.parent.children = append(node.parent.children[:indexVal], node.parent.children[indexVal + 1:]...)
		} else {
			// spustimo onaj na index - 1 node.parents.keys i append na kraj index - 1 node.parent.children, pa append sve kljuceve iz index
			node.parent.children[indexVal].keys = append(node.parent.children[indexVal].keys, node.parent.keys[indexVal])
			node.parent.children[indexVal].keys = append(node.parent.children[indexVal].keys, node.parent.children[indexVal + 1].keys...)
			// spajanje dece iz cvorova koje spajamo
			if node.parent.children[indexVal].children != nil {
				node.parent.children[indexVal].children = append(node.parent.children[indexVal].children, node.parent.children[indexVal + 1].children...)
			}
			// izbaciti index - 1 iz node.parents.keys, kao i nepotrebno dete jedno iz children (node.parent)
			node.parent.keys = append(node.parent.keys[:indexVal], node.parent.keys[indexVal + 1:]...)
			node.parent.children = append(node.parent.children[:indexVal + 1], node.parent.children[indexVal + 2:]...)
		}
	
		if node.parent == btree.Root && len(node.parent.keys) == 0{
			btree.Root = node
		}
		
		if node != btree.Root && node.parent != btree.Root && len(node.parent.children) < btree.minKids {
			btree.adjustNodeDeletion(node.parent)
		}
	}
}

// Funkcija za brisanje elementa iz B stabla
func (btree *BTree) Delete(elem int) {
	node, indexVal, isThere := btree.Find(elem)

	if isThere {
		// ako nije list ono sto brisemo, obrisi taj element, i na njegovo mesto dovedi njegovog prethodnika(koji je sigurno u listu)
		if node.children != nil {
			tempNode := node

			tempNode = tempNode.children[indexVal]
			for tempNode.children != nil {
				tempNode = tempNode.children[len(tempNode.children) - 1]
			}

			node.keys[indexVal] = tempNode.keys[len(tempNode.keys) - 1]

			node = tempNode

			node.keys = node.keys[:len(node.keys) - 1]
		} else {
			node.keys = append(node.keys[:indexVal], node.keys[indexVal+1:]...)
		}

		// ako je broj kljuceva u tom listu sada manji od minimalnog dozvoljenog, radimo odradjene operacije da to sredimo
		if len(node.keys) < btree.minKids {
			btree.adjustNodeDeletion(node)
		}
	} else {
		return
	}
}
*/

// Pomocna funkcija za funkciju koja vraca listu svih elemenata u sortiranom redosledu
// Vraca listu svih elemenata u sortiranom redosledu za odredjeni cvor
func allElemNode(node *BTreeNode) ([]*Data) {
	if node.children == nil {
		return node.keys
	} else {
		elems := make([]*Data, 0, len(node.children))
		for index, key := range node.keys {
			elems = append(elems, allElemNode(node.children[index])...)
			elems = append(elems, key)
		}

		elems = append(elems, allElemNode(node.children[len(node.children) - 1])...)

		return elems
	}
}

// Funkcija koja vraca listu svih elem u sortiranom redosledu
func (btree *BTree) AllElem() ([]*Data) {
	return allElemNode(btree.Root)
}

/* 	KORISNO ZA DEBUG, ISPISIVANJE KLJUCEVA IZ KORENA, ISPISIVANJE KLJUCEVA DECE KORENA I ISPISIVANJE KLJUCEVA UNUKA KORENA
func (btree *BTree) RootElem() {
	for _, value := range btree.Root.keys {
		fmt.Printf("%d ", value)
	}
	fmt.Printf("\n")
}

func (btree *BTree) RootChildElem() {
	for _, node2 := range btree.Root.children {
		for _, value := range node2.keys {
			fmt.Printf("%d ", value)
		}
		fmt.Printf("\n")
	}
	fmt.Printf("\n")
}

func (btree *BTree) RootGrandChildElem() {
	for _, node3 := range btree.Root.children {
		for _,node2 := range node3.children {
			for _, value := range node2.keys {
				fmt.Printf("%d ", value)
			}
			fmt.Printf("\n")
		}
	}
	fmt.Printf("\n")
}
*/

// NAPOMENA: SLICING MOZE NAPRAVITI VELIKI PROBLEM SA REFERENCAMA