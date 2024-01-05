package HyperLogLog

import (
	"math"
	"math/bits"
	"hash/fnv"
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
	m   uint64
	p   uint8
	reg []uint8
}

func Init(precision uint8) *HLL{

	maxreg := uint64(1 << precision)			//racunanje maksimalnog broja registara, stepenovanje se vrsi shiftovanjem
	registri := make([]uint8, maxreg)			//niz registara velicine maxreg

	return &HLL{
		m:maxreg,
		p:precision,
		reg:registri,

	}
}

func (hll *HLL)Add(elem []byte){
	hashValue:= defaultHashFunction(elem)

	value :=trailingZeroBits(hashValue) + 1

	index := firstKbits(hashValue, uint64(hll.p))

	if uint8(value) > hll.reg[index]{
		hll.reg[index]= uint8(value)
	}
	//fmt.Println(value,";",index)
}


func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func (hll *HLL) Serialize(path string) error{
	file, err := os.OpenFile(path, os.O_RDWR | os.O_CREATE,0666)
	if(err!= nil){
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(hll)

	if err!=nil{
		return err
	}
	return nil
}


func (hll *HLL) Deserialize(path string){
	file,err:= os.OpenFile(path, os.O_RDWR | os.O_CREATE,0666)
	if(err != nil){
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	file.Seek(0,0)
	for{
		err = decoder.Decode(hll)
		if err!= nil{
			break
		}
	}
}

// func main(){
// 	hll :=Init(10)
// 	//fmt.Println(hll.p,",",hll.m)
// 	element1 := []byte("vanja")
// 	element2 := []byte("vanja")
// 	element3 := []byte("kostic")
// 	element4 := []byte("sv292022")
// 	element5 := []byte("asdfghjkl")
// 	hll.Add(element1)
// 	hll.Add(element3)
// 	hll.Add(element2)
// 	hll.Add(element4)
// 	hll.Add(element5)

// 	estimation := hll.Estimate()
// 	fmt.Printf("Procenjena kardinalnost: %f\n", estimation)
// }