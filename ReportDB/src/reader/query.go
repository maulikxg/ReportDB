package reader

import (
	"packx/models"
	"packx/utils"
	"sync"
)

func InitQueryEngine(queryReceiveCh <-chan models.Query, queryResultCh chan<- models.QueryResponse, shutDownWg *sync.WaitGroup) {

	defer shutDownWg.Done()

	var readersWaitGroup sync.WaitGroup

	Readers := utils.GetReaders()

	readersWaitGroup.Add(Readers)

	for range Readers {

		go Reader(queryReceiveCh, queryResultCh, &readersWaitGroup)

	}

	readersWaitGroup.Wait()

	close(queryResultCh)

}
