package engine

const (
	memTableSize             = 8_000
	bloomFilterErrorRate     = 0.01
	bloomFilterElementsCount = 10_000
)

type StorageEngine interface {
}

type storageEngineOpts struct {
	memTableSize             int
	bloomFilterErrorRate     float64
	bloomFilterElementsCount float64
}

type StorageEngineOption func(*storageEngineOpts)

func WithMemTableSize(memTableSize int) StorageEngineOption {
	return func(seo *storageEngineOpts) { seo.memTableSize = memTableSize }
}

func WithBloomFilterErrorRate(errorRate float64) StorageEngineOption {
	return func(seo *storageEngineOpts) { seo.bloomFilterErrorRate = errorRate }
}

func WithBloomFilterElementsCount(elementCount float64) StorageEngineOption {
	return func(seo *storageEngineOpts) { seo.bloomFilterElementsCount = elementCount }
}

func defaultOptions() StorageEngineOption {
	return func(seo *storageEngineOpts) {
		seo.bloomFilterElementsCount = bloomFilterElementsCount
		seo.bloomFilterErrorRate = bloomFilterErrorRate
		seo.memTableSize = memTableSize
	}
}

type storageEngine struct {
	storageEngineOpts
}

func CreateNewEngine(opts ...StorageEngineOption) StorageEngine {
	var o storageEngineOpts

	if len(opts) == 0 {
		defaultOptions()(&o)
	} else {
		for _, option := range opts {
			option(&o)
		}
	}

	engine := storageEngine{o}

	return engine
}
