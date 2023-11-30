package BTree

import (
	"math"
)

/*	
	STRUKTURA B STABLO

	Pokrivamo dve strukture, cvor B stabla i samo stablo
	Cvor B stabla sadrzi niz kljuceva i niz pokazivaca na decu (ostale cvorove)
	B stablo kao strutkura sadrzi informaciju o maksimalnom broju dece i o korenskom cvoru stabla
*/

type BTreeNode struct {
	keys	[]int
	children	[]*BTreeNode
	parent	*BTreeNode
}

type BTree struct {
	Root	*BTreeNode
	maxKids	int
}

// Konstruktor za B stablo, kreira B stablo sa zadatim maximalnim brojem dece i praznim korenskim cvorem
func (btree *BTree) Init(max int) {
	btree.maxKids = max
	node := BTreeNode{make([]int, 0, max - 1), nil, nil}
	btree.Root = &node
}

// Funkcija pretrage, za zadati element prolazi kroz stablo i trazi gde on treba da bude
// vraca cvor i ako je zadati element tu vraca true, a ako nije vraca cvor pre kog treba dodati i false
func (btree *BTree) Find(elem int) (*BTreeNode, int, bool) {
	iterNode := btree.Root

	for true {
		indexFound := false

		for index, key := range iterNode.keys {
			if key == elem {
				return iterNode, index, true
			} else if key > elem {
				if iterNode.children != nil {
					iterNode = iterNode.children[index]
					indexFound = true
					break
				} else {
					return iterNode, index, false
				}
			}
		}

		if !indexFound {
			if iterNode.children != nil {
				iterNode = iterNode.children[len(iterNode.children) - 1]
			} else {
				return iterNode, len(iterNode.keys), false
			}
		}
	}

	return iterNode, 0, false
}

func (btree *BTree) splitNode(node *BTreeNode) {
	index := int(btree.maxKids / 2)

	// prebaci u parent node element na indexu
	if(btree.Root == node) {
		// moramo novi root da imamo, ako je pun trenutni root
		newRoot := BTreeNode{make([]int, 0, btree.maxKids - 1), nil, nil}
		newRoot.keys[0] = node.keys[index]
		btree.Root = &newRoot
		node.parent = &newRoot
	} else {
		place := 0

		for _, value := range node.parent.keys {
			if value > node.keys[index] {
				break
			}

			place++
		}

		if len(node.parent.keys) == place {
			node.parent.keys = append(node.parent.keys, node.keys[index])
		}
		node.parent.keys = append(node.parent.keys[:place+1], node.parent.keys[place:]...)
		node.parent.keys[place] = node.keys[index]
	}

	// rastavi keys i napravi dva niza koja ces dodati na mesta gde treba kao decu gore
	nodeOne := BTreeNode{node.keys[:index], node.children[:index], node.parent}
	nodeTwo := BTreeNode{node.keys[index + 1:], node.children[index + 1:], node.parent}

	node.parent.children[index] = &nodeOne
	if len(node.parent.children) == index {
		node.parent.children = append(node.parent.children, node.children[index])
	}
	node.parent.children = append(node.parent.children[:index+1], node.parent.children[index:]...)
	node.parent.children[index] = &nodeTwo

	if len(node.parent.keys) == btree.maxKids {
		btree.splitNode(node.parent)
	}
}

// Funkcija za dodavanje elementa u B stablo
func (btree *BTree) Add(elem int) {
	node, indexVal, isThere := btree.Find(elem)

	if isThere {
		return
	} else {
		if len(node.keys) == indexVal {
			node.keys = append(node.keys, elem)
		}
		node.keys = append(node.keys[:indexVal+1], node.keys[indexVal:]...)
		node.keys[indexVal] = elem

		if len(node.keys) == btree.maxKids {
			done := false

			first := false
			last := false

			for _, value := range node.parent.children {
				if node == value {
					first = true
				} else if first && len(value.keys) < (btree.maxKids - 1) {
					done = true
					break
				}

			}

			first = false

			if done {
				for index, value := range node.parent.children {
					if last {
						break
					}
	
					if first {
						// elem na trazenom indexu u node.parent.keys postaje prvi u value.keys
						value.keys = append([]int{node.parent.keys[index]}, value.keys...)	
	
						// remove taj elem
						node.parent.keys = append(node.keys[:index], node.keys[index+1:]...)
	
						if len(value.keys) < (btree.maxKids - 1) {
							last = true
						} else {
							// poslednji elem insert u node.parent.keys
							if len(node.parent.keys) == index {
								node.parent.keys = append(node.parent.keys, value.keys[len(value.keys) - 1])
							}
							node.parent.keys = append(node.parent.keys[:index+1], node.parent.keys[index:]...)
							node.parent.keys[index] = value.keys[len(value.keys) - 1]
							
							// remove poslednji elem
							value.keys = value.keys[:len(value.keys) - 1]
						}
					}
	
					if value == node {
						// poslednji elem insert u node.parent.keys
						if len(node.parent.keys) == index {
							node.parent.keys = append(node.parent.keys, value.keys[len(value.keys) - 1])
						}
						node.parent.keys = append(node.parent.keys[:index+1], node.parent.keys[index:]...)
						node.parent.keys[index] = value.keys[len(value.keys) - 1]
						
						// remove poslednji elem
						value.keys = value.keys[:len(value.keys) - 1]
	
						first = true
					}
				}
			} else {
				btree.splitNode(node)
			}
		}
	}
}

// Funkcija za brisanje elementa iz B stabla
func (btree *BTree) Delete(elem int) {
	node, indexVal, isThere := btree.Find(elem)

	if isThere {
		// minimum dece
		min := int(math.Ceil(float64(btree.maxKids / 2)))

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
		if len(node.keys) < min {
			left := true
			done := true

			leftIndex := -1
			rightIndex := -1

			// ako je moguce, pronaci cemo levog ili desnog brata od koga mozemo pozajmiti jedan element
			for index, value := range node.parent.children {
				if len(value.keys) > min {
					if left {
						leftIndex = index
					} else {
						rightIndex = index
						break
					}
				}

				if value == node {
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
				rightIndex--

				for true {
					node.parent.children[rightIndex].keys = append(node.parent.children[rightIndex].keys, tempKey)

					if node.parent.children[rightIndex] == node {
						break
					} else {
						tempKey = node.parent.keys[rightIndex - 1]
						node.parent.keys[rightIndex - 1] = node.parent.children[rightIndex].keys[0]
						node.parent.children[rightIndex].keys = node.parent.children[rightIndex].keys[1:]
						rightIndex--
					}
				}
			} else if rightIndex == -1 {
				// levo
				tempKey := node.parent.keys[leftIndex]
				node.parent.keys[leftIndex] = node.parent.children[leftIndex].keys[len(node.parent.children[leftIndex].keys) - 1]
				node.parent.children[leftIndex].keys = node.parent.children[leftIndex].keys[:len(node.parent.children[leftIndex].keys) - 1]
				leftIndex++

				for true {
					node.parent.children[leftIndex].keys = append([]int{tempKey}, node.parent.children[leftIndex].keys...)

					if node.parent.children[leftIndex] == node {
						break
					} else {
						tempKey = node.parent.keys[leftIndex]
						node.parent.keys[leftIndex] = node.parent.children[leftIndex].keys[len(node.parent.children[leftIndex].keys) - 1]
						node.parent.children[leftIndex].keys = node.parent.children[leftIndex].keys[:len(node.parent.children[leftIndex].keys) - 1]
						leftIndex++
					}
				}
			} else if (rightIndex - indexVal) >= (indexVal - leftIndex) {
				// levo
				tempKey := node.parent.keys[leftIndex]
				node.parent.keys[leftIndex] = node.parent.children[leftIndex].keys[len(node.parent.children[leftIndex].keys) - 1]
				node.parent.children[leftIndex].keys = node.parent.children[leftIndex].keys[:len(node.parent.children[leftIndex].keys) - 1]
				leftIndex++

				for true {
					node.parent.children[leftIndex].keys = append([]int{tempKey}, node.parent.children[leftIndex].keys...)

					if node.parent.children[leftIndex] == node {
						break
					} else {
						tempKey = node.parent.keys[leftIndex]
						node.parent.keys[leftIndex] = node.parent.children[leftIndex].keys[len(node.parent.children[leftIndex].keys) - 1]
						node.parent.children[leftIndex].keys = node.parent.children[leftIndex].keys[:len(node.parent.children[leftIndex].keys) - 1]
						leftIndex++
					}
				}
			} else {
				// desno
				tempKey := node.parent.keys[rightIndex - 1]
				node.parent.keys[rightIndex - 1] = node.parent.children[rightIndex].keys[0]
				node.parent.children[rightIndex].keys = node.parent.children[rightIndex].keys[1:]
				rightIndex--

				for true {
					node.parent.children[rightIndex].keys = append(node.parent.children[rightIndex].keys, tempKey)

					if node.parent.children[rightIndex] == node {
						break
					} else {
						tempKey = node.parent.keys[rightIndex - 1]
						node.parent.keys[rightIndex - 1] = node.parent.children[rightIndex].keys[0]
						node.parent.children[rightIndex].keys = node.parent.children[rightIndex].keys[1:]
						rightIndex--
					}
				}
			}
			
			// ako nije, kombinujemo cvorove
			if !done {

			}
		}
	} else {
		return
	}
}

// Pomocna funkcija za funkciju koja vraca listu svih elemenata u sortiranom redosledu
// Vraca listu svih elemenata u sortiranom redosledu za odredjeni cvor
func allElemNode(node *BTreeNode) ([]int) {
	if node.children == nil {
		return node.keys
	} else {
		elems := make([]int, 0, len(node.children))
		for index, key := range node.keys {
			elems = append(elems, allElemNode(node.children[index])...)
			elems = append(elems, key)
		}

		elems = append(elems, allElemNode(node.children[len(node.children) - 1])...)

		return elems
	}
}

// Funkcija koja vraca listu svih elem u sortiranom redosledu
func (btree *BTree) AllElem() ([]int) {
	return allElemNode(btree.Root)
}