package Memtable

import (
	"NASPprojekat/BTree"
	"NASPprojekat/Config"
	"NASPprojekat/SSTable"
	"NASPprojekat/SkipList"
	"fmt"
	"os"
	"strconv"
)

/*
	STRUKTURA MEMTABLE

Sadrzi SkipList i BTree kao strukture preko kojih je moguce da je implementiran
Sadrzi preko koje je strukture implementiran
Sadrzi maks kapacitet i trenutni broj operacija
*/
type Memtable struct {
	skiplist SkipList.SkipList
	btree    BTree.BTree
	version  string
	maxCap   int
	curCap   int
	empty    bool
}

// konstruktor za memtable, na osnovu verzije (skip lista ili b stablo) i maksimalnog kapaciteta kreira inicijalno stanje strukture
func (mt *Memtable) Init(vers string, mCap int) {
	if vers == "skiplist" {
		mt.skiplist = SkipList.SkipList{}
		mt.skiplist.Init(20)
	} else {
		mt.btree = BTree.BTree{}
		mt.btree.Init(10) // koje vrednosti treba da idu u konstruktore? za sada nek bude ovako pa cemo videti, isto i u flush
	}

	mt.version = vers
	mt.maxCap = mCap
	mt.curCap = 0
	mt.empty = true
}

/*
	STRUKTURA NMemtables:

	   N - broj koji se proslijedi vjerovatno preko configa
	 Arr - niz koji sadrzi N memtabli, samo 1. memtabla je "aktivna" i
	       'read-write' je, a ostale iza nje su sve "neaktivne" i 'read-only'
	   l - pokazivac na poslednju read-only memtablu
	   r - pokazivac na prvu write-read memtablu (aktivnu u koju upisujemo)

Kada neka od N memtabli treba da se flushuje, provjerava se da li je svih N popunjeno vec,
ako nije, pomijeramo samo 'r' pokazivac za jedno mjesto unaprijed i popunjavamo sledecu
slobodnu, a ako jesu ipak sve popunjenje, flushujemo poslednju read-only tabelu i pomijeramo
oba pokazivaca l' i 'r' za jedno mjesto ispred.
*/
type NMemtables struct {
	N   int         // broj memtabli
	Arr []*Memtable // niz memtabli
	l   int         // left index
	r   int         // right index
}

// konstruktor za vise memtabli, sve isto, dodan num = broj memtabli
func (nmt *NMemtables) Init(vers string, mCap int, num int) {
	var curArr []*Memtable

	for i := 0; i < num; i++ {
		memtable := Memtable{}
		memtable.Init(vers, mCap)
		curArr = append(curArr, &memtable)
	}

	nmt.N = num
	nmt.Arr = curArr
	nmt.l = 0
	nmt.r = 0
}

// funkcija i za dodavanje i za izmenu elementa sa kljucem
// u zavisnosti od verzije i prisutnosti elementa dodaje elem ili ga menja u odredjenoj strukturi
// poziva se iz WAL-a, ako je uspesno odradjeno dodavanje/izmena
func (nmt *NMemtables) Add(key string, value []byte) {

	arr := nmt.Arr         // arr memtabli
	memtable := arr[nmt.r] // prva "aktivna" memtabla

	var ok bool = false
	if memtable.version == "skiplist" {
		ok = memtable.skiplist.Add(key, value)
	} else {
		ok = memtable.btree.Add(key, value)
	}
	if ok {
		memtable.curCap++
		memtable.empty = false
	}

	if memtable.curCap == memtable.maxCap {
		if (nmt.r-nmt.l == nmt.N-1) || (nmt.r < nmt.l) {
			memtableLast := arr[nmt.l] // izbrisala sam proveru da li je memtable empty
			memtableLast.flush()       // valjda nece trebati (testiracu)
			nmt.l = (nmt.l + 1) % nmt.N
		}
		nmt.r = (nmt.r + 1) % nmt.N
	}
}

// funkcija za brisanje elementa sa kljucem iz memtable
// brisanje je logicko
// poziva se iz WAL-a ako je uspesno sve zapisano
func (nmt *NMemtables) Delete(key string) bool {

	arr := nmt.Arr
	memtable := arr[nmt.r]

	if memtable.version == "skiplist" {
		// logicko brisanje iz skip liste
		// funkcija za brisanje -> vraca true ako je obrisan, false ako smo obrisali element koji ne postoji
		return memtable.skiplist.Delete(key)
	} else {
		return memtable.btree.Add(key, nil)
	}
}

// funkcija za dobavljanje i prikaz elementa sa kljucem iz memtable
/* Get gleda samo prvu aktivnu memtablu, ostale se ne gledaju ni kod Get, ni Delete, ni Add */
func (nmt *NMemtables) Get(key string) {

	arr := nmt.Arr
	memtable := arr[nmt.r]

	if memtable.version == "skiplist" {
		// pronalazak u skip listi
		skipNode, found := memtable.skiplist.Find(key)
		if skipNode.Elem.Transaction.Value != nil && skipNode.Elem.Tombstone && found {
			fmt.Printf("%s %d\n", skipNode.Elem.Transaction.Key, skipNode.Elem.Transaction.Value)
		} else {
			fmt.Printf("Element sa kljucem %s ne postoji!\n", key)
		}

	} else {
		_, _, _, elem := memtable.btree.Find(key)
		if elem != nil && !elem.Tombstone {
			fmt.Printf("%s %d\n", elem.Transaction.Key, elem.Transaction.Value)
		} else {
			fmt.Printf("Element sa kljucem %s ne postoji!\n", key)
		}
	}
}

// funkcija prima kljuc i po njemu trazi podatak u memtable,
// vraca string vrednosti podatka i bool koji oznacava da li je nadjen element
func (nmt *NMemtables) GetElement(key string) ([]byte, bool) {

	arr := nmt.Arr
	memtable := arr[nmt.r]

	if memtable.version == "skiplist" {
		// pronalazak u skip listi
		skipNode, found := memtable.skiplist.Find(key)
		if skipNode.Elem.Transaction.Value != nil && !skipNode.Elem.Tombstone && found {
			return skipNode.Elem.Transaction.Value, true
		} else {
			return []byte{}, false
		}

	} else {
		_, _, _, elem := memtable.btree.Find(key)
		if elem != nil && !elem.Tombstone {
			return elem.Transaction.Value, true
		} else {
			return []byte{}, false
		}
	}
}

// funkcija koja radi flush na disk (sstable)
func (mt *Memtable) flush() {
	for _, value := range mt.GetSortedElems() {
		fmt.Printf("%s %d %t %s\n", value.Transaction.Key, value.Transaction.Value, value.Tombstone, value.ToByte())
	}
	fmt.Printf("\n")

	// m.flushToDisk() ovde treba ovaj flushToDisk da se pozove

	if mt.version == "skiplist" {
		mt.skiplist = SkipList.SkipList{}
		mt.skiplist.Init(20)

	} else {
		mt.btree = BTree.BTree{}
		mt.btree.Init(10)
	}

	mt.curCap = 0
}

func (mt *Memtable) GetSortedElems() ([]*Config.Entry) {

	// arr := nmt.Arr
	// memtable := arr[nmt.r]

	if mt.version == "skiplist" {
		return mt.skiplist.AllElem()
	}

	return mt.btree.AllElem()
}

func (m *Memtable) flushToDisk(lsm Config.LSMTree) {
	fmt.Println("Zapisano na disk.")
	//citamo podatke prvog nivoa jer u njega flushujemo, osmi sstable na prvom nivou je u fajlu npr SSTable/files/dataFile_1_8
	var DataFileName = "SSTable/files/dataFile_1"
	var IndexFileName = "SSTable/files/indexFile_1"
	var SummaryFileName = "SSTable/files/summaryFile_1"
	var BloomFilterFileName = "SSTable/files/bloomFilterFile_1"
	var MerkleTreeFileName = "SSTable/files/merkleTreeFile_1"

	//MEMTABLE TREBA DA SE SORTIRA

	// level[] cuva koliko sstableova se nalazi na svakom od nivoa, dodajemo jos jedan sstable
	var i = lsm.Levels[0] + 1
	//pravimo fajlove za novi sstable
	DataFileName += "_" + strconv.Itoa(i) + ".txt"
	IndexFileName += "_" + strconv.Itoa(i) + ".txt"
	SummaryFileName += "_" + strconv.Itoa(i) + ".txt"
	BloomFilterFileName += "_" + strconv.Itoa(i) + ".txt"
	MerkleTreeFileName += "_" + strconv.Itoa(i) + ".txt"

	//pravljenje fajlova za novi sstable
	dataFile, _ := os.Create(DataFileName)
	err := dataFile.Close()
	if err != nil {
		return
	}

	indexFile, _ := os.Create(IndexFileName)
	err = indexFile.Close()
	if err != nil {
		return
	}

	summaryFile, _ := os.Create(SummaryFileName)
	err = summaryFile.Close()
	if err != nil {
		return
	}

	bloomFilterFile, _ := os.Create(BloomFilterFileName)
	err = bloomFilterFile.Close()
	if err != nil {
		return
	}

	merkleTreeFile, _ := os.Create(MerkleTreeFileName)
	err = merkleTreeFile.Close()
	if err != nil {
		return
	}

	//novi fajlovi se dodaju u liste sa imenima svih fajlova koji cine lsm tree
	lsm.DataFilesNames = append(lsm.DataFilesNames, DataFileName)
	lsm.IndexFilesNames = append(lsm.IndexFilesNames, IndexFileName)
	lsm.SummaryFilesNames = append(lsm.SummaryFilesNames, SummaryFileName)
	lsm.BloomFilterFilesNames = append(lsm.BloomFilterFilesNames, BloomFilterFileName)
	lsm.MerkleTreeFilesNames = append(lsm.MerkleTreeFilesNames, MerkleTreeFileName)

	//pravimo sstable, mora da se pre prosledjivanja SORTIRA MEMTABLE
	//MORA DA SE PROSLEDI LISTA SORTIRANIH ENTYJA A NE MEMTABLE
	SSTable.MakeData(m.GetSortedElems(), DataFileName, IndexFileName, SummaryFileName, BloomFilterFileName)
}
