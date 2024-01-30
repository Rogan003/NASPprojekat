package WriteAheadLog

import (
	"os"
	"time"
)

type WAL struct {
	path            string        //putanja do fajla sa walom
	lastSegment     *os.File      //pokazivac na aktivni segment
	duration        time.Duration //na koji period ce se zvati brisanje
	lowWaterMark    int           //do kog indeksa se brisu segmenti
	lastIndex       int64         //indeks poslednjeg segmenta u walu
	segmentSize     int64         //maksimalni broj bajtova u segmentu
	CurrentSize     int64         //broj popunjenih bajtova u segmentu
	currentMemIndex int64         //pamti indeex trenutno aktivnog memtablea
	segmentsTable   *os.File      //pokazivac na tabelu segmenta
}

// Funkcije za WAL
// getteri
func (wal *WAL) LastSegment() *os.File {
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

// konstruktori
func NewWAL(path string, lastSeg *os.File, duration time.Duration, lowWaterMark int, segSize int64) (*WAL, error) {
	return &WAL{
		path:         path,
		lastSegment:  lastSeg,
		duration:     duration,
		lowWaterMark: lowWaterMark,
		segmentSize:  segSize,
		lastIndex:    0,
	}, nil
}
