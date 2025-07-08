package types

const (
	// Table Errors
	TABLE_KEY_SEARCH_NOT_FOUND = 1
	TABLE_KEY_FILE_SEEK_ERR    = 2
	TABLE_RECORD_READ_ERROR    = 3
	TABLE_FILE_CREATION_ERROR  = 4
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
