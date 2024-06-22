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

	scheduler, err := NewScheduler(db, 1*time.Second)
	if err != nil {
		log.Fatalf("ERROR Unable to create new log reader: %s\n", err)
	}
	defer scheduler.Stop()

	reporter, err := NewLogReporter(db, 10*time.Minute)
	if err != nil {
		log.Fatalf("ERROR Unable to create new reporter: %s\n", err)
	}

	err = scheduler.ScheduleExactTime(SchedulerJobExactTimeDescription{
		TaskName: "CreateReport",
		Hour:     -1,
		Minute:   -1,
		Second:   0,
		Task: func() error {
			report, err := reporter.GenerateReport()
			if err != nil {
				return err
			}
			log.Println(report)
			return nil
		},
	})
	if err != nil {
		log.Fatalf("ERROR Unable to schedule report task: %s\n", err)
	}

	scheduler.Schedule(SchedulerJobDescription{
		TaskName: "VacuumCleanLogRecords",
		Interval: 1 * time.Hour,
		Task: func() error {
			recsDeleted, err := db.LogRecordsVacuumClean(48 * time.Hour)
			log.Printf("INFO Vacuum clean records deleted: %d\n", recsDeleted)
			return err
		},
	})

	ch := make(chan *LogLineData)
	go reader.ReadLogStreamToChannel(ch)
	go db.WriteRecordsFromChannel(ch)
	go scheduler.Run()

	html, err := reporter.GenerateReport()
	if err != nil {
		log.Fatalf("ERROR Unable to create report: %s\n", err)
	}
	log.Println(len(html))

	time.Sleep(300 * time.Second)
	log.Println("Reading finished")
}
