package main

import (
	"bytes"
	"fmt"
	"html/template"
	"time"
)

type LogReporter struct {
	db             *LogDb
	reportInterval time.Duration
	tmpl           *template.Template
}

func NewLogReporter(db *LogDb, reportInterval time.Duration) (*LogReporter, error) {
	res := &LogReporter{db: db, reportInterval: reportInterval}
	err := res.loadTemplates()
	if err != nil {
		return nil, fmt.Errorf("error when loading templates: %s", err)
	}
	return res, nil
}

func (t *LogReporter) GenerateReport() (string, error) {
	srcIpData, err := t.db.GetSrcIpReportData(0)
	if err != nil {
		return "", err
	}

	userData, err := t.db.GetUsersReportData(0)
	if err != nil {
		return "", err
	}

	tplWriter := bytes.NewBufferString("")
	err = t.tmpl.ExecuteTemplate(tplWriter, "report.html.tmpl", map[string]any{
		"SrcIpData": srcIpData,
		"UserData":  userData,
	})
	if err != nil {
		return "", err
	}

	return tplWriter.String(), nil
}

func (t *LogReporter) loadTemplates() error {
	tmpl, err := template.New("").ParseGlob("templates/*.tmpl")
	if err != nil {
		return err
	}
	t.tmpl = tmpl

	return nil
}
