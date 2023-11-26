package BTree

/*	
	STRUKTURA B STABLO

	Pokrivamo dve strukture, cvor B stabla i samo stablo
	Cvor B stabla sadrzi niz kljuceva i niz pokazivaca na decu (ostale cvorove)
	B stablo kao strutkura sadrzi informaciju o maksimalnom broju dece i o korenskom cvoru stabla
*/

type BTreeNode struct {
	keys	[]int
	children	[]*BTreeNode
}

type BTree struct {
	Root	*BTreeNode
	maxKids	int
}

// Konstruktor za B stablo, kreira B stablo sa zadatim maximalnim brojem dece i praznim korenskim cvorem
func (btree *BTree) Init(max int) {
	btree.maxKids = max
	node := BTreeNode{make([]int, 0, max - 2), nil}
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

// Funkcija za dodavanje elementa u B stablo
func (btree *BTree) Add(elem int) {
	node, index, isThere := btree.Find(elem)

	if isThere {
		return
	} else {
		if len(node.keys) == index {
			node.keys = append(node.keys, elem)
		}
		node.keys = append(node.keys[:index+1], node.keys[index:]...)
		node.keys[index] = elem
	}
}

// Funkcija za brisanje elementa iz B stabla
func  (btree *BTree) Delete(elem int) {
	node, index, isThere := btree.Find(elem)

	if isThere {
		node.keys = append(node.keys[:index], node.keys[index+1:]...)
	} else {
		return
	}
}