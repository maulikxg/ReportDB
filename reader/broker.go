package reader

import (
	"log"
	. "packx/models"
	. "packx/utils"
)

type Reader struct {
	id           uint8
	queryChannel chan Query
}

func DistributeQuery(queryCh chan Query, readers []*Reader) {
	go func() {
		numReaders := uint8(GetReaders())
		for query := range queryCh {
			index := uint8(query.QueryID % uint64(numReaders))
			if index >= numReaders || index < 0 {
				log.Printf("DistributeQuery error: Query index is out of range")
				continue
			}
			readers[index].queryChannel <- query
		}
	}()
}
