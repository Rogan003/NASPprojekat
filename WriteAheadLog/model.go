package WriteAheadLog

import (
	"time"
)

type WAL struct {
	path         string        //putanja do fajla sa walom
	segments     []*Segment    //segmenti
	duration     time.Duration //na koji period ce se zvati brisanje
	lowWaterMark int           //do kog indeksa se brisu segmenti
	lastIndex    int64         //indeks poslednjeg segmenta u walu
}

type Segment struct { //segment
	path  string //putanja do fajla segmenta
	index int64  //pocetak segnemta
	size  int64
	data  []byte //entriji u segmentu u bajtovima
}

type Entry struct { //red u walu
	Crc       uint32
	Timestamp uint64
	Tombstone bool
	Key       string
	Value     []byte
}

type Transaction struct { //jedna transakcija
	Key     string
	Value   []byte
	Deleted bool
}

// Funkcije za segmente
// getteri
func (s *Segment) Path() string {
	return s.path
}

func (s *Segment) Data() []byte {
	return s.data
}

func (s *Segment) Size() int64 {
	return s.size
}

func (s *Segment) Index() int64 {
	return s.index
}

// funkcionalnosti
func (s *Segment) Append(data []byte, size int64) { //upis novog podatka u segment
	s.data = append(s.data, data...)
	s.size = s.size + size
}

// Funkcije za WAL
// getteri
func (wal *WAL) Segments() []*Segment {
	return wal.segments
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

// funkcionalnosti
func (wal *WAL) RemoveIndex(index int) { //izbacuje neki segment iz wala
	wal.segments = append(wal.segments[:index], wal.segments[index+1:]...)
}

func (wal *WAL) AppendSegment(segment *Segment) { //dodaje novi segment na listu segmenata
	wal.segments = append(wal.segments, segment)
}
