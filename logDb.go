package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type LogDb struct {
	db               *sql.DB
	insertQuery      *sql.Stmt
	selectSrcIpQuery *sql.Stmt
}

type SrcIpReportData struct {
	SrcIp   string
	Reqs    int
	LastId  int
	FirstTs int
	LastTs  int
}

func NewLogDb(dbPath string) (*LogDb, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	res := &LogDb{db: db}
	if err := res.Init(); err != nil {
		return nil, err
	}
	return res, nil
}

func (t *LogDb) Init() error {
	tablesSql := []string{
		`CREATE TABLE IF NOT EXISTS schema_meta (
			name TEXT NOT NULL PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS log_records (
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
			ts INTEGER NOT NULL,
			log_type INTEGER NOT NULL,
			date_time TEXT NOT NULL,
			logger_name TEXT NOT NULL,
			level TEXT NOT NULL,
			src_ip TEXT NOT NULL,
			dest_ip TEXT NOT NULL,
			dest_host TEXT NOT NULL,
			user TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS id_src_ip ON log_records (id, src_ip)`,
		`REPLACE INTO schema_meta (name, value) VALUES ("version", "1")`,
	}

	for _, tableQuery := range tablesSql {
		if _, err := t.db.Exec(tableQuery); err != nil {
			return err
		}
	}

	row := t.db.QueryRow(`SELECT CAST(value AS INTEGER) FROM schema_meta WHERE name == "version" LIMIT 1`)
	if row.Err() != nil {
		return row.Err()
	}

	var version int
	if err := row.Scan(&version); err != nil {
		return err
	}

	if version != 1 {
		log.Fatalf("ERROR Version is incorrect: %d\n", version)
	}

	insertQuery, err := t.db.Prepare(`
		INSERT INTO
			log_records (ts, log_type, date_time, logger_name, level, src_ip, dest_ip, dest_host, user)
		VALUES 
		    (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	t.insertQuery = insertQuery

	selectSrcIpQuery, err := t.db.Prepare(`
		SELECT 
		    src_ip AS ip,
		    COUNT(*) AS reqs,
		    MAX(id) AS last_id,
		    MIN(ts) AS first_ts,
		    MAX(ts) AS last_ts
		FROM 
		    log_records
		WHERE
			id > ?
			AND src_ip != ""
		GROUP BY 
		    src_ip
		HAVING
		    reqs >= 10
		ORDER BY
		    reqs DESC
	`)
	if err != nil {
		return err
	}
	t.selectSrcIpQuery = selectSrcIpQuery

	return nil
}

func (t *LogDb) GetSrcIpReportData(fromId int) ([]SrcIpReportData, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()

	res, err := t.selectSrcIpQuery.QueryContext(ctx, fromId)
	if err != nil {
		return nil, fmt.Errorf("error when executing query: %s", err)
	}

	var items []SrcIpReportData
	for res.Next() {
		var curData SrcIpReportData
		if err := res.Scan(&curData.SrcIp, &curData.Reqs, &curData.LastId, &curData.FirstTs, &curData.LastTs); err != nil {
			return nil, fmt.Errorf("error when fetching element: %s", err)
		}
		items = append(items, curData)
	}

	return items, nil
}

func (t *LogDb) Close() {
	if err := t.db.Close(); err != nil {
		log.Printf("WARN Close DB error: %s\n", err)
	}
}

func (t *LogDb) WriteRecordsFromChannel(ch chan *LogLineData) {
	for item := range ch {
		_, err := t.insertQuery.Exec(
			time.Now().UnixMilli(),
			item.LogLineType,
			item.DateTime.Format(time.RFC3339),
			item.LoggerName,
			item.Level,
			item.SrcIp.String(),
			item.DestIp.String(),
			item.DestHost,
			item.User,
		)

		if err != nil {
			log.Fatalf("ERROR Can not insert data: %s\n", err)
		}
	}
}
