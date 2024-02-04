package HyperLogLog

import (
	"math"
	"math/bits"
	"hash/fnv"
	"os"
	"encoding/binary"
	//"fmt"
	//"bytes"
	//"runtime"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

func defaultHashFunction(data []byte) uint64 {			//pogledati hes funkciju
	h := fnv.New64()
	h.Write(data)
	return h.Sum64()
}

func firstKbits(value, k uint64) uint64 {
	return value >> (64 - k)
}

func trailingZeroBits(value uint64) int {
	return bits.TrailingZeros64(value)
}

type HLL struct {
	M   uint64
	P   uint8
	Reg []uint8
}

func (hll *HLL)Init(precision uint8) *HLL{

	hll.M = uint64(1 << precision)			//racunanje maksimalnog broja registara, stepenovanje se vrsi shiftovanjem
	hll.Reg = make([]uint8, hll.M)			//niz registara velicine maxreg, ja sam izmenio da ne bude nepostojeci maxreg nego hll.M
	hll.P = precision

	return hll
}

func (hll *HLL) Add(elem []byte){
	hashValue:= defaultHashFunction(elem)

	value :=trailingZeroBits(hashValue) + 1

	index := firstKbits(hashValue, uint64(hll.P))

	if uint8(value) > hll.Reg[index]{
		hll.Reg[index]= uint8(value)
	}
	
}

//brisanje postojece instance
//u go jeziku nije potrebno ekspilictno obrisati instancu jer to sam go radi kada instanca postane nedostupna
//pokrene se garbage collector
func (hll *HLL) DeleteHLL(){
	hll = nil
	//runtime.GC()
}

//uklanja sadrzaj unutar hll-a
func (hll *HLL) Delete(){
				
	registri := make([]uint8, hll.M)			
	hll.Reg = registri
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.Reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.M))
	estimation := alpha * math.Pow(float64(hll.M), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.M) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.M) * math.Log(float64(hll.M)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.Reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func (hll *HLL) Serialize(path string) {
	file, err := os.OpenFile(path, os.O_RDWR | os.O_CREATE,0666)
	if(err!= nil){
		panic(err)
	}
	defer file.Close()

	bytess,_ := hll.ToBytes()
	_, err = file.Write(bytess)
	if err!=nil{
		panic(err)
	}
}


func (hll *HLL) Deserialize(path string) error{
	file,err:= os.OpenFile(path, os.O_RDWR | os.O_CREATE,0666)
	if(err != nil){
		panic(err)
	}
	defer file.Close()

	file.Seek(0,0)

	fi, err2 := file.Stat()
	if err2 != nil {
		return err2
	}

	data := make([]byte, fi.Size())
	_, err = file.Read(data)
	if err != nil {
		return err
	}
	
	hll.FromBytes(data)

	return nil
}

func (hll *HLL)ToBytes() ([]byte, error) {
	
	data := make([]byte, 0)
	mBytes := make([]byte , 8)
	binary.LittleEndian.PutUint64(mBytes, hll.M)
	data  = append(data, mBytes...)

	pBytes := make([]byte, 1)
	pBytes[0] = byte(hll.P)
	data = append(data, pBytes...)

	for _, r := range hll.Reg{
		
		regBytes := make([]byte, 1)
		regBytes[0] = byte(r)
		data = append(data, regBytes... )
	}
	
	return data, nil
}

func (hll *HLL)FromBytes(b []byte) error {
	
	hll.M = binary.LittleEndian.Uint64(b[:8])
	b = b[8:]

	hll.P = b[0]
	b = b[1:]

	var newReg []uint8
	for len(b) != 0{
		
		reg := uint8(b[0])
		b = b[1:]
		newReg = append(newReg, reg)
		
	}
	hll.Reg = newReg
	
	return nil
}