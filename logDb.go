package main

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type LogDb struct {
	db *sql.DB
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
			ts INTEGER,
			log_type INTEGER,
			date_time TEXT,
			logger_name TEXT,
			level TEXT,
			src_ip TEXT,
			dest_ip TEXT,
			dest_host TEXT,
			user TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS ts ON log_records (ts)`,
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
		log.Fatalf("ERROR Version is incorrect: %s\n", version)
	}

	return nil
}

func (t *LogDb) Close() {
	if err := t.db.Close(); err != nil {
		log.Printf("WARN Close DB error: %s\n", err)
	}
}

func (t *LogDb) WriteRecordsFromChannel(ch chan *LogLineData) {
	insertQuery, err := t.db.Prepare(`
		INSERT INTO
			log_records (ts, log_type, date_time, logger_name, level, src_ip, dest_ip, dest_host, user)
		VALUES 
		    (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		log.Fatalf("ERROR Ubable to prepare DB request: %s\n", err)
	}

	// TODO: Sometimes dates are absent. Fix this
	for item := range ch {
		_, err := insertQuery.Exec(
			item.DateTime.Unix(),
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
