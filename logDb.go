package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type LogDb struct {
	db                        *sql.DB
	insertQuery               *sql.Stmt
	selectSrcIpQuery          *sql.Stmt
	selectUsersQuery          *sql.Stmt
	selectDestHostsQuery      *sql.Stmt
	insertKvDataValueQuery    *sql.Stmt
	selectKvDataStrValueQuery *sql.Stmt
	selectKvDataIntValueQuery *sql.Stmt
	vacuumCleanLogRecords     *sql.Stmt
}

type BasicGroupReportData struct {
	Reqs      int
	LastId    int
	FirstTs   int
	LastTs    int
	FirstTime string
	LastTime  string
}

type SrcIpReportData struct {
	BasicGroupReportData
	SrcIp string
}

type UsersReportData struct {
	BasicGroupReportData
	User string
}

type DestHostsReportData struct {
	BasicGroupReportData
	DestHost string
}

const QueryTimeout = 10 * time.Second

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
	err := t.initSchema()
	if err != nil {
		return err
	}

	err = t.checkSchemaVersion()
	if err != nil {
		return err
	}

	return t.prepareQueries()
}

func (t *LogDb) GetSrcIpReportData(fromId int) ([]SrcIpReportData, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), QueryTimeout)
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
		curData.FirstTime = time.Unix(int64(curData.FirstTs/1000), 0).UTC().Format(time.RFC3339)
		curData.LastTime = time.Unix(int64(curData.LastTs/1000), 0).UTC().Format(time.RFC3339)
		items = append(items, curData)
	}

	return items, nil
}

func (t *LogDb) GetUsersReportData(fromId int) ([]UsersReportData, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), QueryTimeout)
	defer cancelFunc()

	res, err := t.selectUsersQuery.QueryContext(ctx, fromId)
	if err != nil {
		return nil, fmt.Errorf("error when executing query: %s", err)
	}

	var items []UsersReportData
	for res.Next() {
		var curData UsersReportData
		if err := res.Scan(&curData.User, &curData.Reqs, &curData.LastId, &curData.FirstTs, &curData.LastTs); err != nil {
			return nil, fmt.Errorf("error when fetching element: %s", err)
		}
		curData.FirstTime = time.Unix(int64(curData.FirstTs/1000), 0).UTC().Format(time.RFC3339)
		curData.LastTime = time.Unix(int64(curData.LastTs/1000), 0).UTC().Format(time.RFC3339)
		items = append(items, curData)
	}

	return items, nil
}

func (t *LogDb) GetDestHostsReportData(fromId int) ([]DestHostsReportData, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), QueryTimeout)
	defer cancelFunc()

	res, err := t.selectDestHostsQuery.QueryContext(ctx, fromId)
	if err != nil {
		return nil, fmt.Errorf("error when executing query: %s", err)
	}

	var items []DestHostsReportData
	for res.Next() {
		var curData DestHostsReportData
		if err := res.Scan(&curData.DestHost, &curData.Reqs, &curData.LastId, &curData.FirstTs, &curData.LastTs); err != nil {
			return nil, fmt.Errorf("error when fetching element: %s", err)
		}
		curData.FirstTime = time.Unix(int64(curData.FirstTs/1000), 0).UTC().Format(time.RFC3339)
		curData.LastTime = time.Unix(int64(curData.LastTs/1000), 0).UTC().Format(time.RFC3339)
		items = append(items, curData)
	}

	return items, nil
}

func (t *LogDb) SetLastId(lastId int) error {
	key := "lastId"
	return t.SetKvRecord(key, lastId)
}

func (t *LogDb) GetLastId() (int, error) {
	return t.GetKvIntRecord("lastId")
}

func (t *LogDb) LogRecordsVacuumClean(maxSecondsAge int64) (int64, error) {
	borderTs := time.Now().Unix() - maxSecondsAge
	res, err := t.vacuumCleanLogRecords.Exec(borderTs)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (t *LogDb) GetKvIntRecord(key string) (int, error) {
	res := t.selectKvDataIntValueQuery.QueryRow(key)
	err := res.Err()
	if err != nil {
		return 0, err
	}

	var val int
	err = res.Scan(&val)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	return val, nil
}

func (t *LogDb) GetKvStrRecord(key string) (string, error) {
	res := t.selectKvDataStrValueQuery.QueryRow(key)
	err := res.Err()
	if err != nil {
		return "", err
	}

	var val string
	err = res.Scan(&val)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		return "", nil
	} else if err != nil {
		return "", err
	}

	return val, nil
}

func (t *LogDb) SetKvRecord(key string, val interface{}) error {
	if _, err := t.insertKvDataValueQuery.Exec(key, val); err != nil {
		return err
	}
	return nil
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

func (t *LogDb) Close() {
	if err := t.db.Close(); err != nil {
		log.Printf("WARN Close DB error: %s\n", err)
	}
}

func (t *LogDb) initSchema() error {
	tablesSql := []string{
		`CREATE TABLE IF NOT EXISTS kv_data (
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
		`CREATE INDEX IF NOT EXISTS id_user ON log_records (id, user)`,
		`CREATE INDEX IF NOT EXISTS id_dest_host ON log_records (id, dest_host)`,
		`CREATE INDEX IF NOT EXISTS ts ON log_records (ts)`,

		`REPLACE INTO kv_data (name, value) VALUES ("schema_version", "1")`,
	}

	for _, tableQuery := range tablesSql {
		if _, err := t.db.Exec(tableQuery); err != nil {
			return err
		}
	}

	return nil
}

func (t *LogDb) checkSchemaVersion() error {
	row := t.db.QueryRow(`SELECT CAST(value AS INTEGER) FROM kv_data WHERE name == "schema_version" LIMIT 1`)
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

	return nil
}

func (t *LogDb) prepareQueries() error {
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
		ORDER BY
		    reqs DESC
	`)
	if err != nil {
		return err
	}
	t.selectSrcIpQuery = selectSrcIpQuery

	selectUsersQuery, err := t.db.Prepare(`
		SELECT 
		    user,
		    COUNT(*) AS reqs,
		    MAX(id) AS last_id,
		    MIN(ts) AS first_ts,
		    MAX(ts) AS last_ts
		FROM 
		    log_records
		WHERE
			id > ?
			AND user != ""
		GROUP BY 
		    user
		ORDER BY
		    reqs DESC
	`)
	if err != nil {
		return err
	}
	t.selectUsersQuery = selectUsersQuery

	selectHostsQuery, err := t.db.Prepare(`
		SELECT 
		    dest_host,
		    COUNT(*) AS reqs,
		    MAX(id) AS last_id,
		    MIN(ts) AS first_ts,
		    MAX(ts) AS last_ts
		FROM 
		    log_records
		WHERE
			id > ?
			AND dest_host != ""
		GROUP BY 
		    dest_host
		ORDER BY
		    reqs DESC
	`)
	if err != nil {
		return err
	}
	t.selectDestHostsQuery = selectHostsQuery

	insertKvDataValueQuery, err := t.db.Prepare(`REPLACE INTO kv_data (name, value) VALUES (?, CAST(? AS TEXT))`)
	if err != nil {
		return err
	}
	t.insertKvDataValueQuery = insertKvDataValueQuery

	selectKvDataStrValueQuery, err := t.db.Prepare(`SELECT value FROM kv_data WHERE name=? LIMIT 1`)
	if err != nil {
		return err
	}
	t.selectKvDataStrValueQuery = selectKvDataStrValueQuery

	selectKvDataIntValueQuery, err := t.db.Prepare(`SELECT CAST(value AS INTEGER) FROM kv_data WHERE name=? LIMIT 1`)
	if err != nil {
		return err
	}
	t.selectKvDataIntValueQuery = selectKvDataIntValueQuery

	vacuumCleanLogRecords, err := t.db.Prepare(`DELETE FROM log_records WHERE ts < ?`)
	if err != nil {
		return err
	}
	t.vacuumCleanLogRecords = vacuumCleanLogRecords

	return nil
}
