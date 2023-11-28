package WriteAheadLog

import (
	"time"
)

type WAL struct {
	path         string        //putanja do fajla sa walom
	lastSegment  Segment       //aktivni segment
	duration     time.Duration //na koji period ce se zvati brisanje
	lowWaterMark int           //do kog indeksa se brisu segmenti
	lastIndex    int64         //indeks poslednjeg segmenta u walu
	segmentSize  int64
}

type Segment struct { //segment
	fileName string //putanja do fajla segmenta
	index    int64  //pocetak segnemta
	size     int64  //broj entrija
	//ENRIJI ILI NIZ BAJTOVA
	entries []*Entry //entriji u segmentu
}

type Entry struct { //red u walu
	Crc         uint32
	Timestamp   uint64
	Tombstone   bool
	Transaction Transaction //transakcije
}

type Transaction struct { //jedna transakcija
	Key   string
	Value []byte
}

// Funkcije za segmente
// getteri
func (s *Segment) FileName() string {
	return s.fileName
}

func (s *Segment) Entries() []*Entry {
	return s.entries
}

func (s *Segment) Size() int64 {
	return s.size
}

func (s *Segment) Index() int64 {
	return s.index
}

// funkcionalnosti
func (s *Segment) AppendEntry(entry *Entry) { //upis novog podatka u segment
	s.entries = append(s.entries, entry)
	s.size = s.size + 1
}

// Funkcije za WAL
// getteri
func (wal *WAL) LastSegment() Segment {
	return wal.lastSegment
}

func (wal *WAL) Path() string {
	return wal.path
}

func (wal *WAL) Duration() time.Duration {
	return wal.duration
}

func (wal *WAL) LowWaterMark() int {
	return wal.lowWaterMark
}

func (wal *WAL) LastIndex() int64 {
	return wal.lastIndex
}
func (wal *WAL) SegmentSize() int64 {
	return wal.segmentSize
}
