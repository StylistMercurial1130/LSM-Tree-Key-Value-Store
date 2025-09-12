package types

const (
	// Table Errors
	TABLE_KEY_SEARCH_NOT_FOUND          = 1
	TABLE_KEY_FILE_SEEK_ERR             = 2
	TABLE_RECORD_READ_ERROR             = 3
	TABLE_FILE_CREATION_ERROR           = 4
	TABLE_FILE_OPEN_ERROR               = 5
	TABLE_READ_FILE_ERROR               = 6
	BUFFER_READ_ERROR                   = 7
	TABLE_MERGE_ERROR                   = 8
	INDEX_BLOCK_DECODE_ERROR            = 9
	LEVEL_GET_RANGE_OUT_OF_BOUNDS_ERROR = 10
	LEVEL_GET_ERROR                     = 11
	BIT_VECTOR_OUT_OF_BOUNDS            = 12
	BIT_VECTOR_SEARCH_ERROR             = 13
	TABLE_FILE_DELETE_ERROR             = 14
	DISKMANAGER_KEY_NOT_FOUND_ERROR     = 15
)

type EngineError struct {
	errCode int
	msg     string
}

func (e *EngineError) Error() string {
	return e.msg
}

func (e *EngineError) GetErrorCode() int {
	return e.errCode
}

func NewEngineError(errCode int, msg string) error {
	return &EngineError{
		errCode: errCode, msg: msg,
	}
}
