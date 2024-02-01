package WriteAheadLog

import (
	"NASPprojekat/Config"
	"NASPprojekat/Memtable"
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/edsrzf/mmap-go"
)

// treba jos resiti kad segment sadrzi pola jedne a pola druge memtabele
func (wal *WAL) RemakeWAL(mem *Memtable.NMemtables) error {
	//ucitavamo imena svih fajlova sa segmentima
	segementsFiles, err := wal.ScanWALFolder()
	if err != nil {
		return err
	}
	memIdx := 0 //PROMENITI ZA NAJSTARIJI MEM, pretpostavljam da treba 0? posto je memtabla inicijalno prazna?
	// za sada nema info koja je najstarija, mozemo cuvati nekako ili da menjamo memseg prilikom remake, mislim da je bolje menjati memseg
	// citamo od kog offseta treba krenuti citanje segmenta sa najmanjim indeksom
	file, err := os.Open("files_WAL/memseg.txt")
	if err != nil {
		fmt.Println("Error opening memseg file:", err)
		return err
	}
	//defer file.Close()

	scanner := bufio.NewScanner(file)

	// citanje linija iz memseg redom
	offset := 0
	lineCount := 0
	for scanner.Scan() {
		lineCount++
		if lineCount == memIdx {
			// linija sa nasim pocetnim segmentom za rekreiranje
			currentLine := scanner.Text()
			elements := strings.Split(currentLine, ",")
			firstSegData := strings.Split(elements[0], " ")
			offsetStart, err := strconv.Atoi(firstSegData[1])
			offset = offsetStart
			if err != nil {
				fmt.Println("Error:", err)
				return err
			}
			break
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return err
	}

	//redom ucitavamo fajlove sa segmentima
	for _, fileName := range segementsFiles {
		for {
			// fmt.Println(fileName)
			entry, next, jump := wal.readEntry(fileName, offset)
			// fmt.Println(entry)
			//ako smo zavrsili sa citanjem svih entrya izadji iz fje
			if reflect.DeepEqual(entry, Config.Entry{}) { // mislim da ovo ne valja skroz? mozda i valja
				return nil
			}
			offset = next
			if entry.Crc != Config.CRC32(entry.Transaction.Value) {
				return errors.New("crc is different")
			}
			
			if entry.Tombstone {
				//ako je operacija brisanja
				_, index, lwm := mem.Delete(entry.Transaction.Key) // dodao za lwm i index kod brisanja? jel treba? ja mislim da je okej
				if lwm != -1 {
					wal.lowWaterMark = lwm
				}
				wal.currentMemIndex = int64(index)

			} else {
				//ako je operacija dodavanja ili izmene
				index, lwm := mem.Add(entry.Transaction.Key, entry.Transaction.Value)
				if lwm != -1 {
					wal.lowWaterMark = lwm
				}
				wal.currentMemIndex = int64(index)
			}
			//ako su procitani svi entry iz ovog segmenta idi na sledeci
			if jump {
				break
			}
		}

	}
	return nil
}

// prilikom pokretanja wal-a, treba da se skenira folder sa segmentima i unutar wala da se kreira struktura sa offsetima segmenata
// i putanjama do njih. Poslednji segment treba da se ucita u memorijsku strukturu i moze da se markira sa _END. Da li treba obezbediti trajnost
// podataka u takvoj strukturi?
// skenira sva imena fajlova sa segmentima wala i pravi sortiran niz imena po indeksima segmenata
func (wal *WAL) ScanWALFolder() ([]string, error) {
	files, err := ioutil.ReadDir(wal.path) //ucitavanje svega sto je u folderu,  vraca listu fajlova ili gresku
	if err != nil {                        //ako do nje dodje za slucaj da je doslo do greske
		return nil, err
	}

	var segments []string //vracamo ovu promenljivu koja je niz stringova sa putanjama do segmenata

	for _, file := range files { //_ jer nam ne treba indeks
		if file.IsDir() { //ako je direktorijum ignorisemo
			continue
		}

		if strings.HasSuffix(file.Name(), ".log") && strings.HasPrefix(file.Name(), "segment") {
			segmentPath := filepath.Join(wal.path, file.Name()) //Konstruišemo putanju do trenutnog segmenta koristeći kombinujući path i ime trenutnog fajla.
			segments = append(segments, segmentPath)            //dodajemo putanju
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
func (wal *WAL) WriteInFile(entry *Config.Entry, path string) (error, bool) {

	var shifted bool

	// segementsFiles, err := wal.ScanWALFolder()
	// if err!= nil{
	// 	log.Fatal(err)
	// 	return err, false
	// }

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		log.Fatal(err)
		return err, false
	}

	fi, err2 := file.Stat()
	if err2 != nil {
		return err2, false
	}

	if fi.Size() == 0 {
		file.Truncate(1)
	}

	entryBytes := entry.ToByte()

	//remainingCapacity := Config.MaxSegmentSize - fileInfo.Size()
	remainingCapacity := wal.segmentSize - fi.Size()
	firstFileSize := fi.Size()

	defer file.Close()

	if len(entryBytes) <= int(remainingCapacity) {
		err = file.Truncate(fi.Size() + int64(len(entryBytes)))

		if err != nil {
			log.Fatal(err)
			return err, false
		}

		mmapFile, err := mmap.Map(file, mmap.RDWR, 0)
		defer mmapFile.Unmap()

		if err != nil {
			log.Fatal(err)
			return err, false
		}

		mmapFile = append(mmapFile[:firstFileSize], entryBytes...)
		shifted = false

	} else {

		firstPart := entryBytes[:remainingCapacity]
		secondPart := entryBytes[remainingCapacity:]
		nextPath, err := getNextSegmentPath(path)
		if err != nil {
			log.Fatal(err)
			return err, false
		}

		err = file.Truncate(fi.Size() + int64(len(firstPart)))

		if err != nil {
			log.Fatal(err)
			return err, false
		}

		mmapFile, err := mmap.Map(file, mmap.RDWR, 0)
		defer mmapFile.Unmap()

		if err != nil {
			log.Fatal(err)
			return err, false
		}

		file2, err := os.OpenFile(nextPath, os.O_CREATE|os.O_RDWR, 0644)

		if err != nil {
			log.Fatal(err)
			return err, false
		}

		fi, err2 := file2.Stat()
		if err2 != nil {
			return err2, false
		}

		if fi.Size() == 0 {
			file2.Truncate(1)
		}

		err = file2.Truncate(int64(len(secondPart)))

		if err != nil {
			log.Fatal(err)
			return err, false
		}

		defer file2.Close()

		mmapFile2, err := mmap.Map(file2, mmap.RDWR, 0)

		if err != nil {
			log.Fatal(err)
			return err, false
		}
		defer mmapFile2.Unmap()

		mmapFile = append(mmapFile[:firstFileSize], firstPart...)
		mmapFile2 = append(mmapFile2[:fi.Size()], secondPart...)
		shifted = true

	}

	return nil, shifted

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

func isInSegments(targetPath string, segments []string) bool {
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

func (wal *WAL) readEntry(path string, offset int) (Config.Entry, int, bool) {

	segementsFiles, err := wal.ScanWALFolder()
	if err != nil {
		log.Fatal(err)
		return Config.Entry{}, 0, false
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0644) // promenio sam os.O_RDONLY

	//da li treba vratiti gresku?
	if err != nil {
		log.Fatal(err)
		return Config.Entry{}, 0, false
	}

	fi, err2 := file.Stat()
	if err2 != nil {
		return Config.Entry{}, 0, false
	}

	if fi.Size() <= 1 {
		return Config.Entry{}, 0, false // ovo potencijalno treba? da li treba <= 1? da li je to greska?
	}

	defer file.Close()

	mmapFile, err := mmap.Map(file, mmap.RDWR, 0)
	defer mmapFile.Unmap()

	if err != nil {
		log.Fatal(err)
		return Config.Entry{}, 0, false
	}
	//mozda treba defer.mmapFile.Unmap() ????

	buffer := mmapFile[offset:] //u buffer ide sve sto je ostalo od trenutnog fajla
	//entry := Config.Entry{}
	var entry Config.Entry
	var shifted bool
	var newoffset int
	var buffer2 []byte                        //baffer2 je za bajtove drugog fajl - inicijalizovace se ako postoji sledeci fajl
	nextPath, err := getNextSegmentPath(path) //trazimo sledeci fajl i ako postoji otvaramo ga
	if err != nil {
		log.Fatal(err)
		return Config.Entry{}, 0, false
	}

	if isInSegments(nextPath, segementsFiles) {
		if err != nil {
			log.Fatal(err)
			return Config.Entry{}, 0, false
		}

		file2, err := os.OpenFile(nextPath, os.O_RDWR, 0644) // promenio sam os.O_RDONLY

		fi, err2 := file2.Stat()
		if err2 != nil {
			return Config.Entry{}, 0, false
		}

		if fi.Size() == 0 {
			file2.Truncate(1)
		}

		if err != nil {
			log.Fatal(err)
			return Config.Entry{}, 0, false
		}

		defer file2.Close()

		mmapFile2, err := mmap.Map(file2, mmap.RDWR, 0)
		if err != nil {
			log.Fatal(err)
			return Config.Entry{}, 0, false
		}
		defer mmapFile2.Unmap()

		buffer2 = mmapFile2[0:]
	}

	if len(buffer) < 4 {
		crc := buffer
		bytesLeft := buffer2[:(4 - len(buffer))]

		entry.Crc = binary.LittleEndian.Uint32(append(crc, bytesLeft...))
		buffer2 = buffer2[(4 - len(buffer)):]

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

		bytess := make([]byte, int(valueSize))
		copy(bytess, buffer2[:int(valueSize)])
		entry.Transaction.Value = bytess
		shifted = true
		newoffset = (4 - len(buffer)) + 8 + 1 + 8 + 8 + int(keySize) + int(valueSize)

	} else {
		entry.Crc = binary.LittleEndian.Uint32(buffer[:4])
		buffer = buffer[4:]
		
		if len(buffer) < 8 {
			timestamp := buffer
			bytesLeft := buffer2[:(8 - len(buffer))]

			entry.Timestamp = binary.LittleEndian.Uint64(append(timestamp, bytesLeft...))
			buffer2 = buffer2[(8 - len(buffer)):]

			entry.Tombstone = (buffer2[0] != 0)
			buffer2 = buffer2[1:]

			keySize := binary.LittleEndian.Uint64(buffer2[:8])
			buffer2 = buffer2[8:]

			valueSize := binary.LittleEndian.Uint64(buffer2[:8])
			buffer2 = buffer2[8:]

			entry.Transaction.Key = string(buffer2[:keySize])
			buffer2 = buffer2[keySize:]

			bytess := make([]byte, int(valueSize))
			copy(bytess, buffer2[:int(valueSize)])
			entry.Transaction.Value = bytess
			shifted = true
			newoffset = (8 - len(buffer)) + 1 + 8 + 8 + int(keySize) + int(valueSize)

		} else {
			entry.Timestamp = binary.LittleEndian.Uint64(buffer[:8])
			buffer = buffer[8:]

			if len(buffer) == 0 {

				entry.Tombstone = (buffer2[0] != 0)
				buffer2 = buffer2[1:]

				keySize := binary.LittleEndian.Uint64(buffer2[:8])
				buffer2 = buffer2[8:]

				valueSize := binary.LittleEndian.Uint64(buffer2[:8])
				buffer2 = buffer2[8:]

				entry.Transaction.Key = string(buffer2[:keySize])
				buffer2 = buffer2[keySize:]

				bytess := make([]byte, int(valueSize))
				copy(bytess, buffer2[:int(valueSize)])
				entry.Transaction.Value = bytess
				shifted = true
				newoffset = 1 + 8 + 8 + int(keySize) + int(valueSize)
			} else {

				entry.Tombstone = (buffer[0] != 0)
				buffer = buffer[1:]

				if len(buffer) < 8 {
					help := buffer
					bytesLeft := buffer2[:(8 - len(buffer))]

					keySize := binary.LittleEndian.Uint64(append(help, bytesLeft...))
					buffer2 = buffer2[(8 - len(buffer)):]

					valueSize := binary.LittleEndian.Uint64(buffer2[:8])
					buffer2 = buffer2[8:]

					entry.Transaction.Key = string(buffer2[:keySize])
					buffer2 = buffer2[keySize:]

					bytess := make([]byte, int(valueSize))
					copy(bytess, buffer2[:int(valueSize)])
					entry.Transaction.Value = bytess
					shifted = true
					newoffset = (8 - len(buffer)) + 8 + int(keySize) + int(valueSize)
				} else {
					keySize := binary.LittleEndian.Uint64(buffer[:8])
					buffer = buffer[8:]

					if len(buffer) < 8 {
						help := buffer
						bytesLeft := buffer2[:(8 - len(buffer))]

						valueSize := binary.LittleEndian.Uint64(append(help, bytesLeft...))
						buffer2 = buffer2[(8 - len(buffer)):]

						entry.Transaction.Key = string(buffer2[:keySize])
						buffer2 = buffer2[keySize:]

						bytess := make([]byte, int(valueSize))
						copy(bytess, buffer2[:int(valueSize)])
						entry.Transaction.Value = bytess
						shifted = true
						newoffset = (8 - len(buffer)) + int(keySize) + int(valueSize)
					} else {
						valueSize := binary.LittleEndian.Uint64(buffer[:8])
						buffer = buffer[8:]

						if len(buffer) < int(keySize) {
							key := buffer
							bytesLeft := buffer2[:(int(keySize) - len(buffer))]

							keyBytes := append(key, bytesLeft...)
							entry.Transaction.Key = string(keyBytes)
							buffer2 = buffer2[(int(keySize) - len(buffer)):]

							bytess := make([]byte, int(valueSize))
							copy(bytess, buffer2[:int(valueSize)])
							entry.Transaction.Value = bytess
							newoffset = int(valueSize) + (int(keySize) - len(buffer))
							shifted = true

						} else {
							entry.Transaction.Key = string(buffer[:int(keySize)])
							buffer = buffer[int(keySize):]

							if len(buffer) < int(valueSize) {
								value := buffer //ovde se sada nalazi deo value-a, sada treba ostatak value-a da uzmemo iz drugog fajla
								bytesLeft := buffer2[:(int(valueSize) - len(buffer))]

								valueBytes := append(value, bytesLeft...)
								bytess := make([]byte, int(valueSize))
								copy(bytess, valueBytes)
								entry.Transaction.Value = bytess
								entry.Transaction.Value = valueBytes
								newoffset = int(valueSize) - len(buffer)
								shifted = true
							} else if len(buffer) == int(valueSize) {
								bytess := make([]byte, int(valueSize))
								copy(bytess, buffer)
								entry.Transaction.Value = bytess
								shifted = true
								newoffset = 0
							} else {
								bytess := make([]byte, int(valueSize))
								copy(bytess, buffer[:int(valueSize)])
								entry.Transaction.Value = bytess
								shifted = false
								newoffset = offset + 4 + 8 + 1 + 8 + 8 + int(valueSize) + int(keySize)
							}
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
	files, err := ioutil.ReadDir(wal.path)
	if err != nil {
		return err
	}
	//brisanje fajla ako je index tog segmenta ispod lowWaterMarka
	for _, file := range files {

		idxStr := strings.TrimSuffix(strings.TrimPrefix(file.Name(), "files_WAL/segment"), ".log")
		idx, _ := strconv.Atoi(idxStr)

		if idx <= wal.lowWaterMark {

			err = os.Remove(wal.path + "/" + file.Name())
			if err != nil {
				fmt.Printf("Greška prilikom brisanja fajla %s: %s\n", file.Name(), err)
			} else {
				fmt.Printf("Fajl %s uspešno obrisan.\n", file.Name())
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

// otvara last segment i sprema wal za pisanje
func (wal *WAL) OpenWAL(mem *Memtable.NMemtables) error {
	//citamo sve segmente, sortirane za nas WAL
	segments, err := wal.ScanWALFolder()
	if err != nil {
		return err
	}
	//ako ne postoji nijedan segment napravi ga
	if len(segments) == 0 {
		//pravljenje novog segmenta
		file, err := os.Create("files_WAL/segment1.log")
		if err != nil {
			fmt.Println("Error:", err)
			return err
		}
		wal.lastSegment = file

		segments = append(segments, "files_WAL/segment1.log")
		//pravi novu tabelu
		fileMS, err := os.Create(filepath.Join(wal.path, "memseg.txt"))
		if err != nil {
			fmt.Println("Error:", err)
			return err
		}
		wal.segmentsTable = fileMS
		var lines []string
		var counter = 1
		lines = append(lines, "1 0")
		for {
			if counter == mem.N-1 {
				break
			}
			counter++
			lines = append(lines, "")
		}
		//jos jenda da bi cita lepo
		lines = append(lines, "")
		writer := bufio.NewWriter(wal.segmentsTable)
		for _, line := range lines {
			_, err := fmt.Fprintln(writer, line)
			if err != nil {
				panic(err)
			}
		}

		if err := writer.Flush(); err != nil {
			panic(err)
		}

		wal.CurrentSize = 0
		wal.currentMemIndex = 0
		wal.lowWaterMark = 0
	} else {
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
		idxStr := strings.TrimSuffix(strings.TrimPrefix(segments[0], "segment"), ".log")
		minIndex, _ := strconv.Atoi(idxStr)
		wal.lowWaterMark = minIndex
		wal.RemakeWAL(mem)
	}

	return nil
}

// dodaje novi entri u aktivni segment, ako je pun segment, pravi novi i cuva stari
func (wal *WAL) AddEntry(entry *Config.Entry) error {
	//dodaje entri u poslednji segment
	err, next := wal.WriteInFile(entry, wal.lastSegment.Name()) // mozda treba drugaciji parametar?
	if err != nil {
		return err
	}

	//ako je presao na sledeci segment ucitaj ga kao poslednji
	if next {
		currentIndex := getIndexFromSegment(wal.lastSegment.Name())
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
	print(next)
	println("lastSeg")
	println(wal.lastSegment.Name())
	fileInfo, err := os.Stat(wal.lastSegment.Name())
	if err != nil {
		fmt.Println("Error getting file information:", err)
		return err
	}
	//pamtimo koliko je zauzet trneutno
	wal.CurrentSize = fileInfo.Size()
	println("currentsize")
	println(wal.CurrentSize)
	println("lastSeg")
	println(wal.lastSegment.Name())
	return nil

}
func (wal *WAL) AddTransaction(Tombstone bool, transaction Config.Transaction) (error, *Config.Entry) { // pravi entry od transakcije i cuva ga
	entry := Config.NewEntry(Tombstone, transaction)
	err := wal.AddEntry(entry)
	if err != nil {
		return err, nil
	}
	return nil, entry
}

// vraca ime prethodnog aktivnog segmenta
func getIndexFromSegment(segment string) int {
	idxStr := strings.TrimSuffix(strings.TrimPrefix(segment, "files_WAL/segment"), ".log")
	currentIndex, _ := strconv.Atoi(idxStr)
	return currentIndex
}

// doslo je do smene memtableova - upisuje su
func (wal *WAL) updateMemSeg(entry *Config.Entry, memIndex int) {
	//trazim koliko je bajtova posledji unet entry dugacak
	newEntryBytes := entry.ToByte()
	numberOfBytes := len(newEntryBytes)
	var lines []string

	_, err := wal.segmentsTable.Seek(0, 0)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	scanner := bufio.NewScanner(wal.segmentsTable)
	var counter = 0
	for scanner.Scan() {
		line := scanner.Text()
		//treba da u memseg.txt da za stari memtable pise u kom segmentu mu je zavrsetak i do kog bajta
		if counter == int(wal.currentMemIndex) {
			if wal.CurrentSize == int64(numberOfBytes) {
				strNumber := fmt.Sprintf("%d", 0)
				idx := fmt.Sprintf("%d", getIndexFromSegment(wal.lastSegment.Name()))
				line += "," + idx + " " + strNumber
			} else if wal.CurrentSize < int64(numberOfBytes) {
				strNumber := fmt.Sprintf("%d", wal.segmentSize+wal.CurrentSize-int64(numberOfBytes))
				idx := fmt.Sprintf("%d", getIndexFromSegment(wal.lastSegment.Name())-1)
				line += "," + idx + " " + strNumber
			} else {
				strNumber := fmt.Sprintf("%d", wal.CurrentSize-int64(numberOfBytes))
				idx := fmt.Sprintf("%d", getIndexFromSegment(wal.lastSegment.Name()))
				line += "," + idx + " " + strNumber
			}
			//treba da u memseg.txt da za novi memtable pise u kom segmentu mu je pocetak i od kog bajta
		} else if counter == int(memIndex) {
			if wal.CurrentSize == int64(numberOfBytes) {
				idx := fmt.Sprintf("%d", getIndexFromSegment(wal.lastSegment.Name()))
				strNumber := fmt.Sprintf("%d", 0)
				line = idx + " " + strNumber
			} else if wal.CurrentSize > int64(numberOfBytes) {
				idx := fmt.Sprintf("%d", getIndexFromSegment(wal.lastSegment.Name()))
				strNumber := fmt.Sprintf("%d", wal.CurrentSize-int64(numberOfBytes))
				line = idx + " " + strNumber
			} else {
				strNumber := fmt.Sprintf("%d", wal.segmentSize+wal.CurrentSize-int64(numberOfBytes))
				idx := fmt.Sprintf("%d", getIndexFromSegment(wal.lastSegment.Name())-1)
				line = idx + " " + strNumber
			}
		}
		counter++
		lines = append(lines, line)
	}

	newFile, err := os.Create(wal.segmentsTable.Name())
	if err != nil {
		panic(err)
	}
	wal.segmentsTable = newFile

	writer := bufio.NewWriter(wal.segmentsTable)
	for _, line := range lines {
		_, err := fmt.Fprintln(writer, line)
		if err != nil {
			panic(err)
		}
	}

	if err := writer.Flush(); err != nil {
		panic(err)
	}

	wal.currentMemIndex = int64(memIndex)
}

func Delete(wal *WAL, mem *Memtable.NMemtables, key string) { //dodaje transakciju brisanja u wal pa brise iz memtable
	transaction := Config.NewTransaction(key, []byte{})
	err, entry := wal.AddTransaction(true, *transaction)
	if err != nil {
		return
	}
	_, memIndex, lwm := mem.Delete(key)
	if lwm != -1 {
		wal.lowWaterMark = lwm
		wal.DeleteSegments()
	}
	if wal.currentMemIndex != int64(memIndex) {
		wal.updateMemSeg(entry, memIndex)
	}
}

func Put(wal *WAL, mem *Memtable.NMemtables, key string, value []byte) bool { //dodaje transakciju dodavanja u wal pa dodaje u memtable
	transaction := Config.NewTransaction(key, value)
	err, entry := wal.AddTransaction(false, *transaction)
	if err != nil {
		return false
	}
	memIndex, lwm := mem.Add(key, value)
	if lwm != -1 {
		wal.lowWaterMark = lwm
		wal.DeleteSegments() // mozda je ovde okej?
	}
	if wal.currentMemIndex != int64(memIndex) {
		wal.updateMemSeg(entry, memIndex)
		//wal.DeleteSegments() treba da se desava samo nakon flush-a, a ne nakon svakog prelaska na novu memtabelu
	}
	print("low water mark ")
	println(wal.lowWaterMark)
	return true
}

// delete segments ne radi kako treba, obrise sve fajlove?
// sstable get