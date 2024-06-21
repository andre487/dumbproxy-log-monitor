package main

import (
	"encoding/json"
	"log"
	"time"
)

type LogReporter struct {
	db             *LogDb
	reportInterval time.Duration
}

func NewLogReporter(db *LogDb, reportInterval time.Duration) *LogReporter {
	return &LogReporter{db: db, reportInterval: reportInterval}
}

func (t *LogReporter) GenerateReport() {
	srcIpData, err := t.db.GetSrcIpReportData(0)
	if err != nil {
		log.Fatalln(err)
	}

	js, err := json.MarshalIndent(srcIpData, "", "  ")
	log.Println(string(js))
}
