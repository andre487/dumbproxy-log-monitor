package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

func main() {
	filePath := "../testData/dumbproxy-big.log"
	logLineGeneral, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Read file %s error: %v", filePath, err)
	}
	logLines := strings.Split(strings.TrimSpace(string(logLineGeneral)), "\n")

	for {
		for _, logLine := range logLines {
			fmt.Println(logLine)
			time.Sleep(10 * time.Millisecond)
		}
	}
}
