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
func (mt *Memtable) Add(elem float64) {
	if mt.version == "skiplist" {
		// dodavanje u WAL u zavisnosti do toga sta je, i ako je sve okej dodajemo i ovde
		mt.skiplist.Add(elem)
	} else {
		// dodavanje u WAL u zavisnosti do toga sta je, i ako je sve okej dodajemo i ovde
		mt.btree.Add(int(elem))
	}

	// ako je sve okej

	mt.curCap++

	if mt.curCap == mt.maxCap {
		mt.flush()
	}
}

// funkcija za brisanje elementa sa kljucem iz memtable
// brisanje je logicko
func (mt *Memtable) Delete(elem float64) {
	// dodavanje zapisa za brisanje u wal i ako je sve okej obrisati ga logicki

	if mt.version == "skiplist" {
		// logicko brisanje iz skip liste
	} else {
		// logicko brisanje iz b stabla
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
			fmt.Printf("%d ", value)
		}
		fmt.Printf("\n")

		// sortirana lista svih elem, nad njom pozvati sstable kreaciju, za sada samo ispisujemo elem

		mt.btree = BTree.BTree{}
		mt.btree.Init(10)
	}

	mt.curCap = 0
}