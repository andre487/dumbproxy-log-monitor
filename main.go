package main

import (
	"log"
)

func main() {
	reader, err := NewLogReader(LogReaderParams{
		JournalDCommand: "go run .",
		ExecDir:         "testJournalD",
	})
	if err != nil {
		log.Fatalf("ERROR Unable to create new log reader: %s\n", err)
	}
	//goland:noinspection GoUnhandledErrorResult
	defer reader.Stop()

	ch := make(chan *LogLineData)
	go reader.ReadLogStreamToChannel(ch)

	for msg := range ch {
		if msg != nil {
			//log.Printf("MSG: %+v\n", msg)
		}
	}

	log.Println("Reading finished")
}
