package main

import (
	"log"
	"time"
)

func main() {
	db, err := NewLogDb("/tmp/test.db")
	if err != nil {
		log.Fatalf("ERROR Unable to create DB: %s", err)
	}
	defer db.Close()

	reader, err := NewLogReader(LogReaderParams{
		JournalDCommand: "go run .",
		ExecDir:         "testJournalD",
	})
	if err != nil {
		log.Fatalf("ERROR Unable to create new log reader: %s\n", err)
	}
	defer reader.Stop()

	ch := make(chan *LogLineData)
	go reader.ReadLogStreamToChannel(ch)
	go db.WriteRecordsFromChannel(ch)

	time.Sleep(1 * time.Minute)

	log.Println("Reading finished")
}
