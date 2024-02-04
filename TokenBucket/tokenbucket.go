package TokenBucket

import (
	"encoding/binary"
	"os"
	"time"
	//"fmt"
)

/*
	TokenBucket - struktura koja ogranicava korisnika u slanju vise

requesta zaredom, kako bismo se odbranili od "napada"
  - dok god imamo tokena, korisnik moze uraditi request,
    ako nema, javice gresku.
  - tokeni se refreshuju na neki odredjeni vremenski period
*/
type TokenBucket struct {
	MaxSize     int           // max broj cnt?
	Cnt         int           // broj dostupnih tokena, vrati se na broj podesen u configu vjerovatno? - tokenbucket_size?
	LastRefresh time.Time     // vrijeme poslednjeg refresha
	TimeLimit   time.Duration // timelimit nakon kojeg se smije refreshati
}

func (tb *TokenBucket) Init(cntSize int, timeLimit time.Duration) {
	tb.MaxSize = cntSize        // ** ne znam da li ovo treba ovako za sad, ali nega ga, ne smeta?
	tb.Cnt = cntSize            // ** valjda ce se proslijediti preko config?
	tb.LastRefresh = time.Now() // stavlja se na trenutno vrijeme
	tb.TimeLimit = timeLimit
}

func (tb *TokenBucket) refresh() {
	tb.Cnt = tb.MaxSize
	tb.LastRefresh = time.Now()
	//tb.Serialize()  // ** da li treba jos nekad Serialize?? nakon svakog Consume??
}

// vraca true ako je moguce uraditi request
// u suprotnome gleda da li se moze refresh,
// - ako moze: refreshuje i oduzima  cnt opet za request, vraca true
// - ako ne moze: samo vraca false (nije moguce uraditi request u tom trenutku)
func (tb *TokenBucket) ConsumeToken() bool {
	// provjera da li imamo slobodnih tokencica
	if tb.Cnt > 0 {
		tb.Cnt--
		return true // imamo, sve okej, moze uraditi request
	}

	// da li je vrijeme da se refresha?
	if time.Since(tb.LastRefresh) >= tb.TimeLimit {
		// ako je proslo dovoljno vremena, mozemo refresh i oduzeti taj jedan token jer ce se obaviti request ipak
		tb.refresh()
		tb.Cnt--

		return true
	}

	//  ne moze se obaviti request
	return false
}

/*
func (tb *TokenBucket) Serialize(fileName string) {
	filePath := fmt.Sprintf(fileName, os.PathSeparator)
	file, err := os.OpenFile(filePath, os.O_RDWR | os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(tb)  // enkoduj TokenBucket

	if err != nil {
		panic(err)
	}
}

func Deserialize(fileName string) *TokenBucket {
	filePath := fmt.Sprintf(fileName, os.PathSeparator)
	file, err := os.OpenFile(filePath, os.O_RDWR | os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var tb TokenBucket
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&tb)

	if err != nil {
		panic(err)
	}

	return &tb
}

func (tb *TokenBucket) ToBytes() ([]byte, error) {
	var network bytes.Buffer
    enc := gob.NewEncoder(&network)

    err := enc.Encode(*tb)
    if err != nil {
        return nil, err
    }

    return network.Bytes(), nil
}

func (tb *TokenBucket) FromBytes(bytess []byte) error {
	network := bytes.NewBuffer(bytess)
    dec := gob.NewDecoder(network)

    err := dec.Decode(&tb)

    if err != nil {
        return err
    }

    return nil
}
*/

func (tb *TokenBucket) Serialize(path string) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	bytess, _ := tb.ToBytes()
	_, err = file.Write(bytess)
	if err != nil {
		panic(err)
	}
}

func (tb *TokenBucket) ToBytes() ([]byte, error) {
	data := make([]byte, 0)

	maxSizeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(maxSizeBytes, uint64(tb.MaxSize))
	data = append(data, maxSizeBytes...)

	cntBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(cntBytes, uint64(tb.Cnt))
	data = append(data, cntBytes...)

	lastRefreshBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(lastRefreshBytes, uint64(tb.LastRefresh.UnixNano()))
	data = append(data, lastRefreshBytes...)

	timeLimitBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(timeLimitBytes, uint64(tb.TimeLimit))
	data = append(data, timeLimitBytes...)

	return data, nil
}



func (tb *TokenBucket) Deserialize(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	file.Seek(0, 0)

	fi, err2 := file.Stat()
	if err2 != nil {
		return err2
	}

	data := make([]byte, fi.Size())
	_, err = file.Read(data)
	if err != nil {
		return err
	}

	tb.FromBytes(data)
	return nil
}

func (tb *TokenBucket) FromBytes(bytess []byte) error {
	tb.MaxSize = int(binary.LittleEndian.Uint64(bytess[:8]))
	bytess = bytess[8:]

	tb.Cnt = int(binary.LittleEndian.Uint64(bytess[:8]))
	bytess = bytess[8:]

	lastRefreshNano := binary.LittleEndian.Uint64(bytess[:8])
	tb.LastRefresh = time.Unix(0, int64(lastRefreshNano))
	bytess = bytess[8:]

	tb.TimeLimit = time.Duration(binary.LittleEndian.Uint64(bytess[:8]))

	return nil
}




/*
func main() {

	var tb = TokenBucket{}
	tb.Init(10, time.Minute)

	// Serialize
	tb.Serialize("token_bucket.db")

	// Deserialize
	var tb2 = TokenBucket{}
	tb2.Deserialize("token_bucket.db")

	fmt.Print("\nLoaded TokenBucket: \n", tb2, "\n\n")


	tb2.ConsumeToken()

	fmt.Print("\nLoaded TokenBucket: \n", tb2, "\n\n")
	tb2.Serialize("token_bucket.db")

	tb2.Deserialize("token_bucket.db")

	fmt.Print("\nLoaded TokenBucket: \n", tb2, "\n\n")
	//tb.Serialize("token_bucket.gob")
}

*/