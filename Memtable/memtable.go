package Memtable

import (
	"fmt"
	"NASPprojekat/SkipList"
	"NASPprojekat/BTree"
)

/*
	STRUKTURA MEMTABLE

	Sadrzi SkipList i BTree kao strukture preko kojih je moguce da je implementiran
	Sadrzi preko koje je strukture implementiran
	Sadrzi maks kapacitet i trenutni broj operacija
*/

type Memtable struct {
	skiplist	SkipList.SkipList
	btree	BTree.BTree
	version	string
	maxCap	int
	curCap	int
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
}

// funkcija i za dodavanje i za izmenu elementa sa kljucem
// u zavisnosti od verzije i prisutnosti elementa dodaje elem ili ga menja u odredjenoj strukturi
// poziva se iz WAL-a, ako je uspesno odradjeno dodavanje/izmena
func (mt *Memtable) Add(key string, value []byte, timestamp uint64) {
	if mt.version == "skiplist" {
		// mt.skiplist.Add(elem)
	} else {
		mt.btree.Add(key, value, timestamp)
	}

	mt.curCap++

	if mt.curCap == mt.maxCap {
		mt.flush()
	}
}

// funkcija za brisanje elementa sa kljucem iz memtable
// brisanje je logicko
// poziva se iz WAL-a ako je uspesno sve zapisano
func (mt *Memtable) Delete(key string, timestamp uint64) {
	if mt.version == "skiplist" {
		// logicko brisanje iz skip liste
	} else {
		mt.btree.Add(key, nil, timestamp)
	}

	mt.curCap++

	if mt.curCap == mt.maxCap {
		mt.flush()
	}
}

// funkcija za dobavljanje i prikaz elementa sa kljucem iz memtable
func (mt *Memtable) Get(key string) {
	if mt.version == "skiplist" {
		// pronalazak u skip listi
	} else {
		_, _, _, elem := mt.btree.Find(key, nil, timestamp)
		return elem
	}

	mt.curCap++

	if mt.curCap == mt.maxCap {
		mt.flush()
	}
}

// funkcija koja radi flush na disk (sstable)
func (mt *Memtable) flush() {
	if mt.version == "skiplist" {
		// isto ovo sto i u else, samo za skiplistu dodati funkciju koja vraca sve elemente sortirane
	} else {
		elems := mt.btree.AllElem()

		for _, value := range elems {
			fmt.Printf("%s %s %t", value.Key, value.Value, value.Tombstone)
		}
		fmt.Printf("\n")

		// sortirana lista svih elem, nad njom pozvati sstable kreaciju, za sada samo ispisujemo elem

		mt.btree = BTree.BTree{}
		mt.btree.Init(10)
	}

	mt.curCap = 0
}