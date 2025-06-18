package types

type Record struct {
	Key       []byte
	Value 	  []byte
	TombStone bool
}

func NewRecord(key []byte,value []byte,tombStone bool) *Record {
	return &Record {
		Key : key, Value : value, TombStone: tombStone,
	}
}
