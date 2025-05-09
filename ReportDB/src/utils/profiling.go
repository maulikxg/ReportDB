package utils

import (
	"log"
	"net/http"
)

func InitProfiling() {

	err := http.ListenAndServe("localhost:1234", nil)

	if err != nil {

		log.Println("Error starting profiling:", err)

	}
}
