package engine

import (
	"LsmStorageEngine/disk"
	"LsmStorageEngine/mem"
	"LsmStorageEngine/types"
)

const (
	memTableSize             = 8_000
	bloomFilterErrorRate     = 0.01
	bloomFilterElementsCount = 10_000
	l0Target                 = 4
	levelRatio               = 10
	dir                      = "./data"
)

type StorageEngine interface {
	Get(key []byte) <-chan Result
	Put(record types.Record) <-chan Result
	Delete(key []byte) <-chan Result
}

type storageEngineOpts struct {
	memTableSize             int
	bloomFilterErrorRate     float64
	bloomFilterElementsCount float64
	levelRatio               int
	l0Target                 int
	dir                      string
}

type Result struct {
	Record types.Record
	Err    error
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

func WithLevelRatio(ratio int) StorageEngineOption {
	return func(seo *storageEngineOpts) { seo.levelRatio = ratio }
}

func WithL0Target(target int) StorageEngineOption {
	return func(seo *storageEngineOpts) { seo.l0Target = target }
}

func WithDataDirLocation(dirLocaiton string) StorageEngineOption {
	return func(seo *storageEngineOpts) { seo.dir = dirLocaiton }
}

func defaultOptions() StorageEngineOption {
	return func(seo *storageEngineOpts) {
		seo.bloomFilterElementsCount = bloomFilterElementsCount
		seo.bloomFilterErrorRate = bloomFilterErrorRate
		seo.memTableSize = memTableSize
		seo.levelRatio = levelRatio
		seo.l0Target = l0Target
		seo.dir = dir
	}
}

type storageEngine struct {
	storageEngineOpts
	m  *mem.Memtable
	dm *disk.DiskManager
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

	engine := &storageEngine{
		storageEngineOpts: o,
	}

	engine.m = mem.NewMemtable(engine.memTableSize)
	engine.dm = disk.CreateDiskManager(
		engine.levelRatio,
		engine.l0Target,
		engine.dir,
	)

	return engine
}

func (engine *storageEngine) Get(key []byte) <-chan Result {
	c := make(chan Result, 1)

	go func() {
		record, err := engine.m.Get(key, engine.dm)
		c <- Result{Record: record, Err: err}
	}()

	return c
}

func (engine *storageEngine) Put(record types.Record) <-chan Result {
	c := make(chan Result, 1)

	go func() {
		err := engine.m.Put(record, engine.dm)
		c <- Result{Err: err}
	}()

	return c
}

func (engine *storageEngine) Delete(key []byte) <-chan Result {
	c := make(chan Result, 1)

	go func() {
		err := engine.m.Delete(key, engine.dm)
		c <- Result{Err: err}
	}()

	return c
}
