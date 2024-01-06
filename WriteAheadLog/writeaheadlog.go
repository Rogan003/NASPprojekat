package WriteAheadLog

import (
	"NASPprojekat/Memtable"
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (entry *Entry) ToByte() []byte { //pretvara iz vrednosti u bajtove
	var data []byte

	crcb := make([]byte, CRC_SIZE)
	binary.LittleEndian.PutUint32(crcb, CRC32(entry.Transaction.Value))
	data = append(data, crcb...) //dodaje se CRC

	sec := time.Now().Unix()
	secb := make([]byte, TIMESTAMP_SIZE)
	binary.LittleEndian.PutUint64(secb, uint64(sec))
	data = append(data, secb...) //dodaje se Timestamp

	//1 - deleted; 0 - not deleted
	//dodaje se Tombstone
	if entry.Tombstone {
		var delb byte = 1
		data = append(data, delb)
	} else {
		var delb byte = 0
		data = append(data, delb)
	}

	keyb := []byte(entry.Transaction.Key)
	keybs := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(keybs, uint64(len(keyb)))

	valuebs := make([]byte, VALUE_SIZE_SIZE)
	binary.LittleEndian.PutUint64(valuebs, uint64(len(entry.Transaction.Value)))

	//dodaju se Key Size i Value Size
	data = append(data, keybs...)
	data = append(data, valuebs...)
	//dodaju se Key i Value
	data = append(data, keyb...)
	data = append(data, entry.Transaction.Value...)

	return data
}

func toEntry(data []byte) Entry {

	entry := Entry{}

	entry.Crc = binary.LittleEndian.Uint32(data[:4]) //ucitavaju se prva 4 bajta
	data = data[4:]                                  //pomeramo se za 4 bajta

	entry.Timestamp = binary.LittleEndian.Uint64(data[:8])
	data = data[8:]

	entry.Tombstone = data[0] != 0 //true ako je 1, false ako je 0
	data = data[1:]

	keySize := binary.LittleEndian.Uint32(data[:4])
	data = data[8:] //pomeramo se za 8 zbog key size i value size

	entry.Transaction.Key = string(data[:keySize])
	data = data[keySize:]

	entry.Transaction.Value = data

	return entry
}

//prilikom pokretanja wal-a, treba da se skenira folder sa segmentima i unutar wala da se kreira struktura sa offsetima segmenata
//i putanjama do njih. Poslednji segment treba da se ucita u memorijsku strukturu i moze da se markira sa _END. Da li treba obezbediti trajnost
//podataka u takvoj strukturi?

func ScanWALFolder() ([]string, error) {
	path := "NASPprojekat/files/WAL" //putanja do foldera sa segmentima

	files, err := ioutil.ReadDir(path) //ucitavanje svega sto je u folderu,  vraca listu fajlova ili gresku
	if err != nil {                    //ako do nje dodje za slucaj da je doslo do greske
		return nil, err
	}

	var segments []string //vracamo ovu promenljivu koja je niz stringova sa putanjama do segmenata

	for _, file := range files { //_ jer nam ne treba indeks
		if file.IsDir() { //ako je direktorijum ignorisemo
			continue
		}

		if strings.HasSuffix(file.Name(), ".log") && strings.HasPrefix(file.Name(), "segment") {
			segmentPath := filepath.Join(path, file.Name()) //Konstruišemo putanju do trenutnog segmenta koristeći kombinujući path i ime trenutnog fajla.
			segments = append(segments, segmentPath)        //dodajemo putanju
		}
	}

	sort.Slice(segments, func(i, j int) bool {

		numI, _ := strconv.Atoi(strings.TrimPrefix(filepath.Base(segments[i]), "segment"))
		numJ, _ := strconv.Atoi(strings.TrimPrefix(filepath.Base(segments[j]), "segment"))

		return numI < numJ
	})

	return segments, nil
}

//treba putanja do segmenata, mozda putanja do tog foldera koji kupi sve segmente i onda se odredjuje najskoriji
//provaliti kako da znam u koji segment da upisem
//funkcija samo radi cisto upisivanje jednog entry-ja

func WriteInFile(entry []byte, path string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644) //0644 - vlasnik moze da cita i pise, ostali mogu samo da citaju
	if err != nil {
		return err
	}
	defer file.Close() //zatvaranje u slucaju greske

	_, err = file.Write(entry)
	if err != nil {
		return err
	}

	return nil
}

//treba mi putanja do poslednjeg segmenta
//provaliti kako dobiti poslednji segment

func ReadEntriesFromFile(path string) ([]*Entry, error) {

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []*Entry //kolekcija entrija koja se vraca

	scanner := bufio.NewScanner(file) //za kretanje red po red

	for scanner.Scan() {
		line := scanner.Text()

		entry := toEntry([]byte(line))    //konvertovanje u entry
		entries = append(entries, &entry) //dodavanje u kolekciju
	}

	return entries, nil

}

func DeleteSegments() error {
	//brise fajlove ispod lowWaterMarka

	path := "NASPprojekat/files/WAL"
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	for _, file := range files {

		filePath := filepath.Join(path, file.Name())
		err = os.Remove(filePath)
		if err != nil {
			fmt.Printf("Greška prilikom brisanja fajla %s: %s\n", filePath, err)
		} else {
			fmt.Printf("Fajl %s uspešno obrisan.\n", filePath)
		}
	}
	return nil
}

func DeleteWAL() {
	//ovde treba da se poziva prethodna funkcija periodicno (vremenski uslov)
}

// fja koja iz svih fajlova segmenata saznaje koji je poslednji, po indeksu u imenu fajla, setuje lastIndex u tom wal i postavlja mu lastSegment
// slicno kao sto si radila scanWAL prodji kroz fajlove i nadji lastindex
func (wal *WAL) OpenWAL() error {

	path := "NASPprojekat/files/WAL"

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	var segments []string

	for _, file := range files { //_ jer nam ne treba indeks
		if file.IsDir() { //ako je direktorijum ignorisemo
			continue
		}

		if strings.HasSuffix(file.Name(), ".log") && strings.HasPrefix(file.Name(), "segment") {
			segmentPath := filepath.Join(path, file.Name())
			segments = append(segments, segmentPath)
		}
	}

	if len(segments) == 0 {
		return errors.New("Nema segmenta u WAL direktorijumu")
	}

	sort.Slice(segments, func(i, j int) bool {

		numI, _ := strconv.Atoi(strings.TrimPrefix(filepath.Base(segments[i]), "segment"))
		numJ, _ := strconv.Atoi(strings.TrimPrefix(filepath.Base(segments[j]), "segment"))

		return numI < numJ
	})

	last := segments[len(segments)-1]
	entries, err := ReadEntriesFromFile(last)
	if err != nil {
		return err
	}
	lastSegment := NewSegment(last, int64(len(segments)-1), int64(len(entries)), entries)
	wal.lastSegment = *lastSegment
	wal.lastIndex = int64(len(segments) - 1)

	return nil
}

// dodaje novi entri u aktivni segment, ako je pun segment, pravi novi i cuva stari
func (wal *WAL) AddEntry(entry *Entry) error {
	//dodaje entri u poslednji segment
	//ako je pun aktivni segment
	if wal.lastSegment.size >= wal.segmentSize {
		//pise entirje iz segmenta u njegov fajl
		for _, e := range wal.lastSegment.entries {
			entryBytes := e.ToByte()
			err := WriteInFile(entryBytes, wal.path+wal.lastSegment.fileName)
			if err != nil {
				return err
			}
		}
		//pravi novi aktivni segment i na njega dodaje entry
		wal.lastIndex++
		newPath := "segment" + strconv.FormatInt(wal.lastIndex, 10) + ".log"
		wal.lastSegment = *NewSegment(newPath, wal.lastIndex, 0, []*Entry{})
		wal.lastSegment.AppendEntry(entry)
	} else {
		wal.lastSegment.AppendEntry(entry)
	}
	return nil

}
func (wal *WAL) AddTransaction(Tombstone bool, transaction Transaction) (error, uint64) { // pravi entry od transakcije i cuva ga
	entry := NewEntry(Tombstone, transaction)
	err := wal.AddEntry(entry)
	if err != nil {
		return err, 0
	}
	return nil, entry.Timestamp
}
func Put(wal *WAL, mem *Memtable.Memtable, key string, value []byte) bool { //dodaje transakciju dodavanja u wal pa dodaje u memtable
	transaction := NewTransaction(key, value)
	err, ts := wal.AddTransaction(false, *transaction)
	if err != nil {
		return false
	}
	mem.Add(key, value, ts)
	return true
}
func Delete(wal *WAL, mem *Memtable.Memtable, key string) { //dodaje transakciju brisanja u wal pa brise iz memtable
	transaction := NewTransaction(key, []byte{})
	err, ts := wal.AddTransaction(true, *transaction)
	if err != nil {
		return
	}
	mem.Delete(key, ts)
}
