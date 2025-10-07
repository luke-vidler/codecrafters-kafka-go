package main

import (
	"fmt"
	"log"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/server"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	srv := server.New("0.0.0.0:9092")
	if err := srv.Start(); err != nil {
		log.Printf("Server error: %v", err)
		os.Exit(1)
	}
}
