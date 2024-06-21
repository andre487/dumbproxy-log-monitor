package main

import "log"

func main() {
	reader, err := NewLogReader(LogReaderParams{
		JournalDCommand: "go run .",
		ExecDir:         "testJournalD",
	})
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := reader.Stop(); err != nil {
			log.Printf("WARN Close reader error: %s", err)
		}
	}()

	ch := make(chan *LogLineData)
	go reader.ReadLogStreamToChannel(ch)

	for msg := range ch {
		if msg != nil {
			//log.Printf("MSG: %+v\n", msg)
		}
	}

	log.Println("Reading finished")
}
