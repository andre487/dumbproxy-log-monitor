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
	lastId, err := t.db.GetLastId()
	if err != nil {
		return "", err
	}

	srcIpData, err := t.db.GetSrcIpReportData(lastId)
	if err != nil {
		return "", err
	}

	userData, err := t.db.GetUsersReportData(lastId)
	if err != nil {
		return "", err
	}

	destHostsData, err := t.db.GetDestHostsReportData(lastId)
	if err != nil {
		return "", err
	}

	tplWriter := bytes.NewBufferString("")
	err = t.tmpl.ExecuteTemplate(tplWriter, "report.html.tmpl", map[string]any{
		"SrcIpData":     srcIpData,
		"UserData":      userData,
		"DestHostsData": destHostsData,
	})
	if err != nil {
		return "", err
	}

	var newLastId int
	for _, data := range srcIpData {
		newLastId = max(newLastId, data.LastId)
	}
	for _, data := range userData {
		newLastId = max(newLastId, data.LastId)
	}
	for _, data := range destHostsData {
		newLastId = max(newLastId, data.LastId)
	}

	err = t.db.SetLastId(newLastId)
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
