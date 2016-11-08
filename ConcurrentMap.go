package suhconcurrentmap

import "BlindsTheCrawler/util"

const (
	MAP_REQUEST_TYPE_GET = 1
	MAP_REQUEST_TYPE_PUT = 2
	MAP_REQUEST_TYPE_DEL = 3
	MAP_REQUEST_TYPE_HAS = 4
)

type GetCallback func(key string, val interface{})

type MapRequest struct {
	Type     int
	RawKey   string
	Key      uint64
	Callback GetCallback
	Payload  interface{}
}

type ConcurrentMapShard struct {
	data map[uint64]interface{}
}

func (cms *ConcurrentMapShard) Get(key uint64) (val interface{}, ok bool) {
	val, ok = cms.data[key]
	return
}

func (cms *ConcurrentMapShard) Put(key uint64, val interface{}) {
	cms.data[key] = val
}

func (cms *ConcurrentMapShard) Delete(key uint64) {
	delete(cms.data, key)
}

type ConcurrentMap struct {
	shardCount     int
	levels         int
	shards         []*ConcurrentMapShard
	shardsChannels []chan *MapRequest
}

func NewConcurrentMap(shardCount int, levels int) *ConcurrentMap {
	return &ConcurrentMap{
		shardCount:     shardCount,
		levels:         levels,
		shards:         make([]*ConcurrentMapShard, shardCount),
		shardsChannels: make([]chan *MapRequest, shardCount),
	}
}

func (cm *ConcurrentMap) Start() {
	ready := make(chan int, cm.shardCount)
	for i := 0; i < cm.shardCount; i++ {
		// ready <- 1
		go func(idx int) {
			cm.shards[idx] = &ConcurrentMapShard{
				data: make(map[uint64]interface{}, 1000),
			}
			shard := cm.shards[idx]
			cm.shardsChannels[idx] = make(chan *MapRequest, cm.levels)
			ready <- 1
			for req := range cm.shardsChannels[idx] {
				switch req.Type {
				case MAP_REQUEST_TYPE_GET:
					if val, ok := shard.Get(req.Key); ok {
						req.Callback(req.RawKey, val)
					} else {
						req.Callback(req.RawKey, nil)
					}
				case MAP_REQUEST_TYPE_PUT:
					shard.Put(req.Key, req.Payload)
				case MAP_REQUEST_TYPE_DEL:
					shard.Delete(req.Key)
				}

			}
		}(i)
	}
	for j := 0; j < cm.shardCount; j++ {
		<-ready
	}
}

func (cm *ConcurrentMap) Get(key string, callback GetCallback) {
	hashKey := util.CalculateHash(key)
	shardIndex := hashKey % uint64(cm.shardCount)
	shard := cm.shardsChannels[shardIndex]
	shard <- &MapRequest{
		RawKey:   key,
		Key:      hashKey,
		Type:     MAP_REQUEST_TYPE_GET,
		Callback: callback,
	}
}

func (cm *ConcurrentMap) Put(key string, val interface{}) {
	hashKey := util.CalculateHash(key)
	shardIndex := hashKey % uint64(cm.shardCount)
	shard := cm.shardsChannels[shardIndex]
	shard <- &MapRequest{
		RawKey:  key,
		Key:     hashKey,
		Type:    MAP_REQUEST_TYPE_PUT,
		Payload: val,
	}
}
