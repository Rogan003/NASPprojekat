package Cache
import(
	"container/list"
)
//element kljuc-vrednost
type CacheEntry struct {
	key   string
	value interface{}			//interface{} znaci da value moze biti bilo koje vrednosti
}

type LRUCache struct{
	capacity int						//kapacitet kesa
	lrulist *list.List					//double linked lista koja cuva elemente kljuc-vrednost
	cache map[string]*list.Element		//hes mapa koja cuva kljuc-adresa. Sluzi zbog brzeg pristupa. Upucuje na tacnu adresu na kojoj se nalazi element u listi
}

func NewLRUCache(capacity int) *LRUCache{
	return &LRUCache{
		capacity: capacity,
		lrulist: list.New(),
		cache: make(map[string]*list.Element),
	}
}

func (c*LRUCache) Get(key string) interface{}{
	element,exist := c.cache[key]					//proverava da li je element tu
	if exist{										//ako jeste premesti ga na pocetak i vrati vrednost
		c.lrulist.MoveToFront(element)
		return element.Value.(*CacheEntry).value
	}
	return nil										//ako nije, vrati nil
	
}


func (c*LRUCache)Insert(key string, value interface{}){
	
	element, exist:= c.cache[key]					//proveri da li vec postoji element sa tim key-em
	if exist{										//ako postoji azuriraj vrendost i pomeri na pocetak 
		element.Value.(*CacheEntry).value = value
		c.lrulist.MoveToFront(element)

	}else{											//ako nije onda proveri da li duzina kesa prevazilazi kapacitet
		if(len(c.cache)) >= c.capacity{				//ako da, onda treba ukloniti najdalje koriscen element iz liste kao i iz kesa
			lastElement:= c.lrulist.Back()
			delete(c.cache, lastElement.Value.(*CacheEntry).key)
			c.lrulist.Remove(lastElement)
		}
													//ako nije onda treba postaviti taj novi element na pocetak liste i ubaciti u kes
		newElement := c.lrulist.PushFront(&CacheEntry{key, value})
		c.cache[key] = newElement
	}

}
