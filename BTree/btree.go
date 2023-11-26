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
	node := BTreeNode{nil, nil}
	btree.Root = &node
}

// Funkcija pretrage, za zadati element prolazi kroz stablo i trazi gde on treba da bude
// vraca cvor (koji, kako) i ako je zadati element tu vraca true, a ako nije vraca false
func (btree *BTree) Find(elem int) (*BTreeNode, bool) {
	iterNode := btree.Root

	for iterNode.keys != nil {
		indexFound := false

		for index, key := range iterNode.keys {
			if key == elem {
				return iterNode, true
			} else if key > elem {
				iterNode = iterNode.children[index]
				indexFound = true
				break
			}
		}

		if !indexFound {
			iterNode = iterNode.children[len(iterNode.children) - 1]
		}
	}

	return iterNode, false
}

// Funkcija za dodavanje elementa u B stablo
func (btree *BTree) Add(elem int) {

}