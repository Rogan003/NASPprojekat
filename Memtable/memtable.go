package Memtable

import (
	"NASPprojekat/BTree"
	"NASPprojekat/BloomFilter"
	"NASPprojekat/SSTable"
	"NASPprojekat/SkipList"
	"time"

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
func (mt *Memtable) Add(key string, value []byte) {
	timestamp := uint64(time.Now().Unix())

	if mt.version == "skiplist" {
		// mt.skiplist.Add(elem)
		ok := mt.skiplist.Add(key, value, timestamp)
		if ok {
			mt.curCap++
		}
	} else {
		ok := mt.btree.Add(key, value, timestamp)
		if ok {
			mt.curCap++
		}
	}

	if mt.curCap == mt.maxCap {
		mt.flush()
	}
}

// funkcija za brisanje elementa sa kljucem iz memtable
// brisanje je logicko
// poziva se iz WAL-a ako je uspesno sve zapisano
func (mt *Memtable) Delete(key string) bool {
	timestamp := uint64(time.Now().Unix())

	if mt.version == "skiplist" {
		// logicko brisanje iz skip liste
		// funkcija za brisanje -> vraca true ako je obrisan, false ako smo obrisali element koji ne postoji
		return mt.skiplist.Delete(key, timestamp)
	} else {
		return mt.btree.Add(key, nil, timestamp)
	}
}

// funkcija za dobavljanje i prikaz elementa sa kljucem iz memtable
func (mt *Memtable) Get(key string) {
	if mt.version == "skiplist" {
		// pronalazak u skip listi
		skipNode, found := mt.skiplist.Find(key)
		if skipNode.Elem.Value != nil && skipNode.Elem.Tombstone && found {
			fmt.Printf("%s %d\n", skipNode.Elem.Key, skipNode.Elem.Value)
		} else {
			fmt.Printf("Element sa kljucem %s ne postoji!\n", key)
		}

	} else {
		_, _, _, elem := mt.btree.Find(key)
		if elem != nil && !elem.Tombstone {
			fmt.Printf("%s %d\n", elem.Key, elem.Value)
		} else {
			fmt.Printf("Element sa kljucem %s ne postoji!\n", key)
		}
	}
}

// funkcija prima kljuc i po njemu trazi podatak u memtable, vraca string vrednosti podatka i bool koji oznacava da li je nadjen element
func (mt *Memtable) GetElement(key string) ([]byte, bool) {
	if mt.version == "skiplist" {
		// pronalazak u skip listi
		skipNode, found := mt.skiplist.Find(key)
		if skipNode.Elem.Value != nil && !skipNode.Elem.Tombstone && found {
			return skipNode.Elem.Value, true
		} else {
			return []byte{}, false
		}

	} else {
		_, _, _, elem := mt.btree.Find(key)
		if elem != nil && !elem.Tombstone {
			return elem.Value, true
		} else {
			return []byte{}, false
		}
	}
}

// funkcija koja radi flush na disk (sstable)
func (mt *Memtable) flush() {
	if mt.version == "skiplist" {
		// isto ovo sto i u else, samo za skiplistu dodati funkciju koja vraca sve elemente sortirane
		elems := mt.skiplist.AllElem()

		fmt.Println("\nFLUSHED:")
		for _, value := range elems {
			fmt.Printf("%s %d %t %s\n", value.Key, value.Value, value.Tombstone, value.ToBytes())
		}
		fmt.Printf("\n")

		mt.skiplist = SkipList.SkipList{}
		mt.skiplist.Init(20)

	} else {
		elems := mt.btree.AllElem()

		for _, value := range elems {
			fmt.Printf("%s %d %t %s\n", value.Key, value.Value, value.Tombstone, value.ToBytes())
		}
		fmt.Printf("\n")

		// sortirana lista svih elem, nad njom pozvati sstable kreaciju, za sada samo ispisujemo elem

		mt.btree = BTree.BTree{}
		mt.btree.Init(10)
	}

	mt.curCap = 0
}

func (m *Memtable) flushToDisk() {
	fmt.Println("Zapisano na disk.")
	//citamo podatke prvog nivoa jer u njega flushujemo, osmi sstable na prvom nivou je u fajlu npr SSTable/files/dataFile_1_8
	var DataFileName = "SSTable/files/dataFile_1"
	var IndexFileName = "SSTable/files/indexFile_1"
	var SummaryFileName = "SSTable/files/summaryFile_1"
	var BloomFilterFileName = "SSTable/files/bloomFilterFile_1"
	var MerkleTreeFileName = "SSTable/files/merkleTreeFile_1"

	//pravimo bloomfilter za sstable od memtable broja elemenata
	bf := BloomFilter.BloomFilter{}
	// da li je bitno da li prosledjujemo m.curCap ili m.maxCap jer se svakako flush zove akd su oni jendkai tj memtable pun??
	bf.Init(m.maxCap, 0.01)

	//MEMTABLE TREBA DA SE SORTIRA

	// level[] cuva koliko sstableova se nalazi na svakom od nivoa, dodajemo jos jedan sstable
	var i = level[0] + 1
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
	dataFilesNames = append(dataFilesNames, DataFileName)
	indexFilesNames = append(indexFilesNames, IndexFileName)
	summaryFilesNames = append(summaryFilesNames, SummaryFileName)
	bloomFilterFilesNames = append(bloomFilterFilesNames, BloomFilterFileName)
	merkleTreeFilesNames = append(merkleTreeFilesNames, MerkleTreeFileName)

	//pravimo sstable, mora da se pre prosledjivanja SORTIRA MEMTABLE
	SSTable.MakeData(m, bf, DataFileName, IndexFileName, SummaryFileName, BloomFilterFileName)

}
