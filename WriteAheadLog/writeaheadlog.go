package WriteAheadLog

import (
	"NASPprojekat/Config"
	"NASPprojekat/Memtable"
	"bufio"
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

// treba jos resiti kad segment sadrzi pola jedne a pola druge memtabele
func (wal *WAL) RemakeWAL(mem *Memtable.NMemtables) error {
	//ucitavamo imena svih fajlova sa segmentima
	segementsFiles, err := ScanWALFolder()
	if err != nil {
		return err
	}

	//redom ucitavamo fajlove sa segmentima
	for _, fileName := range segementsFiles {
		//procitamo entirje iz segmenta
		entries, err := ReadEntriesFromFile("NASPprojekat/files/WAL/" + fileName)
		if err != nil { //ako do nje dodje za slucaj da je doslo do greske
			return err
		}

		//za svaki entry izvrsimo ponovo operaciju
		for _, entry := range entries {
			// ako je operacija brisanja
			if entry.Tombstone {
				mem.Delete(entry.Transaction.Key)
			} else {
				//ako je operacija dodavanja ili izmene
				mem.Add(entry.Transaction.Key, entry.Transaction.Value)
			}
		}

	}
	return nil
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

// WriteInFile treba da doda entry i ako dodje do prelaza u sledeci segment, njegov deo upise u sledeci segment i vraca err,trye/false da ki je presao na sledeci entry
func WriteInFile(entry []byte, path string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644) //0644 - vlasnik moze da cita i pise, ostali mogu samo da citaju
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

func getNextSegmentPath(path string) (string, error) {
	
	_, fileName := filepath.Split(path)

	dirPath := strings.TrimSuffix(path, fileName)

	segmentNumberStr := strings.TrimSuffix(strings.TrimPrefix(fileName, "segment"), ".log")

	segmentNumber, err := strconv.Atoi(segmentNumberStr)
	if err != nil {
		return "", err
	}

	segmentNumber++

	newFileName := fmt.Sprintf("segment%d.log", segmentNumber)
	newPath := filepath.Join(dirPath, newFileName)

	return newPath, nil
}

func isInSegments(targetPathpath string, segmnets []string) bool {
	for _, path := range segments {
		if path == targetPath {
			return true
		}
	}
	return false
}
//treba mi putanja do poslednjeg segmenta
//provaliti kako dobiti poslednji segment

// vraca entry, offset, true/false da li je presao
// prima putanju do fajal ili fajl i offset

func ReadEntry(path string, offset int) (Config.Entry, int, bool) {

	segementsFiles, err := ScanWALFolder()
	if err!= nil{
		log.Fatal(err)
		return Entry{}, 0, false 
	}
	file, err :=os.OpenFile(path, os.O_RDONLY, 0644)

	//da li treba vratiti gresku?
	if err != nil{
		log.Fatal(err)
		return Entry{}, 0, false 
	}

	defer file.Close()

	mmapFile, mmapErr := mmap.Open(file.Fd(), 0, 0, mmap.READ)
	if mmapErr != nil {
		fmt.Println("Greška prilikom mmap:", mmapErr)
		return Entry{}, 0, false
	}
	defer mmapFile.Close()
	//mozda treba defer.mmapFile.Unmap() ????

	buffer := mmapFile[offset:]							//u buffer ide sve sto je ostalo od trenutnog fajla
	var entry Config.Entry{}
	var shifted bool
	var newoffset int
	var buffer2 []byte									//baffer2 je za bajtove drugog fajl - inicijalizovace se ako postoji sledeci fajl				
	nextPath,err = getNextSegmentPath(path)				//trazimo sledeci fajl i ako postoji otvaramo ga

	if isInSegments(nextPath, segementsFiles){
		if err!=nil{
			log.Fatal(err)
			return Entry{}, 0, false 
		}
	
		file2, err :=os.OpenFile(nextPath, os.O_RDONLY, 0644)	
	
		if err != nil{
			log.Fatal(err)
			return Entry{}, 0, false 
		}
	
		defer file2.Close()
	
		mmapFile2, mmapErr := mmap.Open(file2.Fd(), 0, 0, mmap.READ)
		if mmapErr != nil {
			fmt.Println("Greška prilikom mmap:", mmapErr)
			return Entry{}, 0, false
		}
		defer mmapFile2.Close()
	
		buffer2 := mmapFile2[0:]
	}

	
	if len(buffer) < 4{
		crc := buffer 
		bytesLeft := buffer2[:(4-len(buffer))]

		entry.Crc = binary.LittleEndian.Uint32(append(crc,bytesLeft...))
		buffer2 = buffer2[(4-len(buffer)):]

		entry.Timestamp = binary.LittleEndian.Uint64(buffer2[:8])
		buffer2 = buffer2[8:]

		entry.Tombstone = (buffer2[0] != 0)
		buffer2 = buffer2[1:]
		
		keySize := binary.LittleEndian.Uint64(buffer2[:8])
		buffer2 = buffer2[8:]

		valueSize := binary.LittleEndian.Uint64(buffer2[:8])
		buffer2 = buffer2[8:]

		entry.Transaction.Key = string(buffer2[:keySize])
		buffer2 = buffer2[keySize:]

		entry.Transaction.Value = buffer2[:valueSize]
		shifted = true
		newoffset = (4-len(buffer)) + 8 + 1 + 8 + 8 + keySize + valueSize

	}else{
		entry.Crc = binary.LittleEndian.Uint32(buffer[:4])
		buffer = buffer[4:]

		if len(buffer) < 8{
			timestamp := buffer
			bytesLeft := buffer2[:(8-len(buffer))]

			entry.Timestamp = binary.LittleEndian.Uint64(append(timestamp,bytesLeft...))
			buffer2 = buffer2[(8-len(buffer)):]
			
			entry.Tombstone = (buffer2[0] != 0)
			buffer2 = buffer2[1:]
			
			keySize := binary.LittleEndian.Uint64(buffer2[:8])
			buffer2 = buffer2[8:]

			valueSize := binary.LittleEndian.Uint64(buffer2[:8])
			buffer2 = buffer2[8:]

			entry.Transaction.Key = string(buffer2[:keySize])
			buffer2 = buffer2[keySize:]

			entry.Transaction.Value = buffer2[:valueSize]
			shifted = true
			newoffset = (8-len(buffer)) + 1 + 8 + 8 + keySize + valueSize

		}else{
			entry.Timestamp = binary.LittleEndian.Uint64(buffer[:8])
			buffer = buffer[8:]

			if len(buffer) == 0{

				entry.Tombstone = (buffer2[0] != 0)
				buffer2 = buffer2[1:]
				
				keySize := binary.LittleEndian.Uint64(buffer2[:8])
				buffer2 = buffer2[8:]

				valueSize := binary.LittleEndian.Uint64(buffer2[:8])
				buffer2 = buffer2[8:]

				entry.Transaction.Key = string(buffer2[:keySize])
				buffer2 = buffer2[keySize:]

				entry.Transaction.Value = buffer2[:valueSize]
				shifted = true
				newoffset = 1 + 8 + 8 + keySize + valueSize
			}else{

				entry.Tombstone = (buffer[0] != 0)
				buffer = buffer[1:]

				if len(buffer) < 8 {
					help := buffer
					bytesLeft := buffer2[:(8-len(buffer))]

					keySize := binary.LittleEndian.Uint64(append(help,bytesLeft...))
					buffer2 = buffer2[(8-len(buffer)):]

					valueSize := binary.LittleEndian.Uint64(buffer2[:8])
					buffer2 = buffer2[8:]

					entry.Transaction.Key = string(buffer2[:keySize])
					buffer2 = buffer2[keySize:]

					entry.Transaction.Value = buffer2[:valueSize]
					shifted = true
					newoffset = (8-len(buffer)) + 8 + keySize + valueSize
				}
			}else{
				keySize := binary.LittleEndian.Uint64(buffer[:8])
				buffer = buffer[8:]

				if len(buffer) < 8 {
					help := buffer
					bytesLeft := buffer2[:(8-len(buffer))]

					valueSize := binary.LittleEndian.Uint64(append(help,bytesLeft...))
					buffer2 = buffer2[(8-len(buffer)):]

					entry.Transaction.Key = string(buffer2[:keySize])
					buffer2 = buffer2[keySize:]

					entry.Transaction.Value = buffer2[:valueSize]
					shifted = true
					newoffset = (8-len(buffer)) + keySize + valueSize
				}else{
					valueSize := binary.LittleEndian.Uint64(buffer2[:8])
					buffer2 = buffer2[8:]
					
					if len(buffer) < keySize{
						key := buffer
						bytesLeft := buffer2[:(keySize - len(buffer))]

						keyBytes := append(value, bytesLeft...)
						entry.Transaction.Key = string(keyBytes)
						buffer2 = buffer2[(keySize - len(buffer)):]

						entry.Transaction.Value = buffer2[:valueSize]
						newoffset = valueSize + (keySize - len(buffer))
						shifted = true

					}else{

						entry.Transaction.Key = string(buffer[:keySize])
						buffer = buffer[keySize:]

						if len(buffer) < valueSize{
							value := buffer			//ovde se sada nalazi deo value-a, sada treba ostatak value-a da uzmemo iz drugog fajla
							bytesLeft := buffer2[:(valueSize - len(buffer))] 

							valueBytes := append(value, bytesLeft...)
							entry.Transaction.Value = valueBytes
							newoffset = valueSize - len(buffer)
							shifted = true
						}else{
							shifted = false
							newoffset = offset + 4 + 8 + 1 + 8 + 8 + valueSize + keySize
						}
					}
				}
			}
		}
	
	}

	return entry, newoffset, shifted

}

func (wal *WAL) DeleteSegments() error {
	//brise fajlove ispod lowWaterMarka

	path := "NASPprojekat/files/WAL"
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	for _, file := range files {

		filePath := filepath.Join(path, file.Name())
		num, _ := strconv.Atoi(strings.TrimPrefix(filepath.Base(file.Name()), "segment"))

		if num < wal.lowWaterMark {

			err = os.Remove(filePath)
			if err != nil {
				fmt.Printf("Greška prilikom brisanja fajla %s: %s\n", filePath, err)
			} else {
				fmt.Printf("Fajl %s uspešno obrisan.\n", filePath)
			}
		}
	}

	return nil
}

func (wal *WAL) DeleteWAL() {
	//ovde treba da se poziva prethodna funkcija periodicno (vremenski uslov)
	interval := wal.duration * time.Second
	ticker := time.NewTicker(interval)

	for {
		select {
		case <-ticker.C:
			wal.DeleteSegments()
		}
	}

}

// fja koja iz svih fajlova segmenata saznaje koji je poslednji, po indeksu u imenu fajla, setuje lastIndex u tom wal i postavlja mu lastSegment
// slicno kao sto si radila scanWAL prodji kroz fajlove i nadji lastindex

// promeniti za lastindex da bude samo ime segmenta ili otovreni fajl lastsegmenta
func (wal *WAL) OpenWAL() error {

	path := "files_WAL/"

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

	//otvaramo fajl poslednjeg segmenta
	lastSegmentPath := segments[len(segments)-1]
	lastSegmentFile, err := os.OpenFile(lastSegmentPath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644) //0644 - vlasnik moze da cita i pise, ostali mogu samo da citaju
	if err != nil {
		return err
	}
	//defer lastSegmentFile.Close()
	wal.lastSegment = lastSegmentFile

	fileInfo, err := os.Stat(lastSegmentPath)
	if err != nil {
		fmt.Println("Error getting file information:", err)
		return err
	}
	//pamtimo koliko je zauzet trneutno
	wal.CurrentSize = fileInfo.Size()

	return nil
}

// dodaje novi entri u aktivni segment, ako je pun segment, pravi novi i cuva stari
func (wal *WAL) AddEntry(entry *Config.Entry) error {
	//dodaje entri u poslednji segment
	err, next := WriteInFile(entry)
	if err != nil {
		return err
	}
	//ako je presao na sledeci segment ucitaj ga kao poslednji
	if next {
		idxStr := strings.TrimSuffix(strings.TrimPrefix(wal.lastSegment.Name(), "segment"), ".log")
		currentIndex, _ := strconv.Atoi(idxStr)
		currentIndex += 1
		lastSegmentPath := "files_WAL/segment" + strconv.FormatInt(int64(currentIndex), 10) + ".log"
		lastSegmentFile, err := os.OpenFile(lastSegmentPath, os.O_RDWR|os.O_APPEND, 0644) //0644 - vlasnik moze da cita i pise, ostali mogu samo da citaju
		if err != nil {
			return err
		}
		//defer lastSegmentFile.Close()
		//otvaranje novog last segmenta
		wal.lastSegment = lastSegmentFile
	}

	return nil

}
func (wal *WAL) AddTransaction(Tombstone bool, transaction Config.Transaction) (error, uint64) { // pravi entry od transakcije i cuva ga
	entry := Config.NewEntry(Tombstone, transaction)
	err := wal.AddEntry(entry)
	if err != nil {
		return err, 0
	}
	return nil, entry.Timestamp
}
func Put(wal *WAL, mem *Memtable.NMemtables, key string, value []byte) bool { //dodaje transakciju dodavanja u wal pa dodaje u memtable
	transaction := Config.NewTransaction(key, value)
	err, _ := wal.AddTransaction(false, *transaction)
	if err != nil {
		return false
	}
	mem.Add(key, value)
	return true
}
func Delete(wal *WAL, mem *Memtable.NMemtables, key string) { //dodaje transakciju brisanja u wal pa brise iz memtable
	transaction := Config.NewTransaction(key, []byte{})
	err, _ := wal.AddTransaction(true, *transaction)
	if err != nil {
		return
	}
	mem.Delete(key)
}
