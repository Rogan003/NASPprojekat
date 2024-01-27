package TokenBucket


import (
	"fmt"
	"time"
	"encoding/gob"
	"os"
)


/* TokenBucket - struktura koja ogranicava korisnika u slanju vise 
requesta zaredom, kako bismo se odbranili od "napada"
- dok god imamo tokena, korisnik moze uraditi request,
  ako nema, javice gresku.
- tokeni se refreshuju na neki odredjeni vremenski period */
type TokenBucket struct { 
	MaxSize int                // max broj cnt?
	Cnt int   			         // broj dostupnih tokena, vrati se na broj podesen u configu vjerovatno? - tokenbucket_size?
	LastRefresh time.Time      // vrijeme poslednjeg refresha
	TimeLimit time.Duration    // timelimit nakon kojeg se smije refreshati
}


func (tb *TokenBucket) Init(cntSize int, timeLimit time.Duration) {
	tb.MaxSize = cntSize          // ** ne znam da li ovo treba ovako za sad, ali nega ga, ne smeta?
	tb.Cnt = cntSize				   // ** valjda ce se proslijediti preko config?
	tb.LastRefresh = time.Now()   // stavlja se na trenutno vrijeme
	tb.TimeLimit = timeLimit      
}


func (tb *TokenBucket) refresh() {
	tb.Cnt = tb.MaxSize
	tb.LastRefresh = time.Now()
}


// vraca true ako je moguce uraditi request
// u suprotnome gleda da li se moze refresh,
// - ako moze: refreshuje i oduzima  cnt opet za request, vraca true
// - ako ne moze: samo vraca false (nije moguce uraditi request u tom trenutku)
func (tb *TokenBucket) ConsumeToken() bool {
	// provjera da li imamo slobodnih tokencica
	if tb.Cnt > 0 {
		tb.Cnt--
		return true  // imamo, sve okej, moze uraditi request
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


func (tb *TokenBucket) Serialize(fileName string) {
	var f = ""
	f += "files%c"
	f += fileName
	/* ako testiramo ovde, iz custom maina, onda dodati: "../files%" ("../" ispred files) */

	filePath := fmt.Sprintf(f, os.PathSeparator)
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
	var f = ""     
	f += "files%c"
	f += fileName 
	/* ako testiramo ovde, iz custom maina, onda dodati: "../files%" ("../" ispred files) */

	filePath := fmt.Sprintf(f, os.PathSeparator)
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


/*
func main() {
	
	var tb = TokenBucket{}
	tb.Init(10, time.Minute)

	// Serialize
	//tb.Serialize("token_bucket.gob")

	// Deserialize
	tb2 := Deserialize("token_bucket.gob")

	fmt.Print("\nLoaded TokenBucket: \n", tb2, "\n\n")
	

	tb2.ConsumeToken()

	fmt.Print("\nLoaded TokenBucket: \n", tb2, "\n\n")

	//tb.Serialize("token_bucket.gob")
}
*/