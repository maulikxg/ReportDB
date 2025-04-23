package reader

import (
	"packx/models"
	"sync"
)

func Reader(queryReceiveCh <-chan models.Query, queryResultCh chan<- models.QueryResponse, shutDownWg *sync.WaitGroup) {

	defer shutDownWg.Done()

	for query := range queryReceiveCh {

	}

}
