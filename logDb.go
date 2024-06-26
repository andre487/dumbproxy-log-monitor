package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
)

type LogDb struct {
	logDb   *sql.DB
	kvDb    *sql.DB
	cacheDb *sql.DB
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
	Username string
}

type DestHostsReportData struct {
	BasicGroupReportData
	DestIp   string
	DestHost string
}

const QueryTimeout = 10 * time.Second

func NewLogDb(dbDir string) (*LogDb, error) {
	dbDirStat, err := os.Stat(dbDir)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(dbDir, 0755); err != nil {
				return nil, errors.Join(errors.New("unable to create dbDir"), err)
			}
		} else {
			return nil, errors.Join(errors.New("unable to open new DB because of dbDir issues"), err)
		}
	} else if !dbDirStat.IsDir() {
		return nil, errors.Join(fmt.Errorf("dbDir is not a dir: %s", dbDirStat), err)
	}

	logDbPath := path.Join(dbDir, "log.db")
	kvDbPath := path.Join(dbDir, "kv.db")

	logDb, err := sql.Open("sqlite3", logDbPath)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("unable to execute sql.Open for logDb: %s", logDbPath), err)
	}

	kvDb, err := sql.Open("sqlite3", kvDbPath)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("unable to execute sql.Open for kvDb: %s", kvDbPath), err)
	}

	cacheDb, err := sql.Open("sqlite3", "file:cacheDb?mode=memory")
	if err != nil {
		return nil, errors.Join(fmt.Errorf("unable to execute sql.Open for cacheDb: %s", kvDbPath), err)
	}

	res := &LogDb{
		logDb:   logDb,
		kvDb:    kvDb,
		cacheDb: cacheDb,
	}
	if err := res.Init(); err != nil {
		return nil, err
	}
	return res, nil
}

func (t *LogDb) Close() {
	CloseOrWarn(t.logDb)
	CloseOrWarn(t.kvDb)
	CloseOrWarn(t.cacheDb)
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

	return nil
}

func (t *LogDb) GetSrcIpReportData(fromId int) ([]SrcIpReportData, error) {
	log.Tracef("Executing GetSrcIpReportData(%d)", fromId)

	ctx, cancelFunc := context.WithTimeout(context.Background(), QueryTimeout)
	defer cancelFunc()

	res, err := t.logDb.QueryContext(
		ctx,
		`
		SELECT 
		    SrcIp,
		    COUNT(*) AS Reqs,
		    MAX(Id) AS LastId,
		    MIN(Ts) AS FirstTs,
		    MAX(Ts) AS LastTs
		FROM 
		    LogRecords
		WHERE
			Id > ?
			AND LogLineType == "LogLineTypeProxyRequest"
		GROUP BY 
		    SrcIp
		HAVING
		    Reqs >= 5
		ORDER BY
		    Reqs DESC
		`,
		fromId,
	)
	if err != nil {
		return nil, fmt.Errorf("error when executing GetSrcIpReportData query: %s", err)
	}
	defer func() {
		log.Trace("Closing query for GetSrcIpReportData")
		if err := res.Close(); err != nil {
			log.Warnf("Unable to close query for GetSrcIpReportData: %s", err)
		}
	}()

	var items []SrcIpReportData
	for res.Next() {
		var curData SrcIpReportData
		if err := res.Scan(&curData.SrcIp, &curData.Reqs, &curData.LastId, &curData.FirstTs, &curData.LastTs); err != nil {
			return nil, fmt.Errorf("error when fetching element in GetUsersReportData: %s", err)
		}
		if curData.SrcIp == "" {
			curData.SrcIp = "<empty>"
		}
		curData.FirstTime = time.Unix(int64(curData.FirstTs), 0).UTC().Format(time.RFC3339)
		curData.LastTime = time.Unix(int64(curData.LastTs), 0).UTC().Format(time.RFC3339)
		items = append(items, curData)
	}

	return items, nil
}

func (t *LogDb) GetUsersReportData(fromId int) ([]UsersReportData, error) {
	log.Tracef("Executing GetUsersReportData(%d)", fromId)

	ctx, cancelFunc := context.WithTimeout(context.Background(), QueryTimeout)
	defer cancelFunc()

	res, err := t.logDb.QueryContext(
		ctx,
		`
		SELECT 
		    Username,
		    COUNT(*) AS Reqs,
		    MAX(Id) AS LastId,
		    MIN(Ts) AS FirstTs,
		    MAX(Ts) AS LastTs
		FROM 
		    LogRecords
		WHERE
			Id > ?
			AND LogLineType == "LogLineTypeProxyRequest"
		GROUP BY 
		    Username
		ORDER BY
		    Reqs DESC
		`,
		fromId,
	)
	if err != nil {
		return nil, errors.Join(errors.New("error when executing GetUsersReportData query"), err)
	}
	defer func() {
		log.Trace("Closing query for GetUsersReportData")
		if err := res.Close(); err != nil {
			log.Warnf("Unable to close query for GetUsersReportData: %s", err)
		}
	}()

	var items []UsersReportData
	for res.Next() {
		var curData UsersReportData
		if err := res.Scan(&curData.Username, &curData.Reqs, &curData.LastId, &curData.FirstTs, &curData.LastTs); err != nil {
			return nil, errors.Join(errors.New("error when fetching element in GetUsersReportData"), err)
		}
		if curData.Username == "" {
			curData.Username = "<empty>"
		}
		curData.FirstTime = time.Unix(int64(curData.FirstTs), 0).UTC().Format(time.RFC3339)
		curData.LastTime = time.Unix(int64(curData.LastTs), 0).UTC().Format(time.RFC3339)
		items = append(items, curData)
	}

	return items, nil
}

func (t *LogDb) GetDestHostsReportData(fromId int) ([]DestHostsReportData, error) {
	log.Tracef("Executing GetDestHostsReportData(%d)", fromId)

	ctx, cancelFunc := context.WithTimeout(context.Background(), QueryTimeout)
	defer cancelFunc()

	res, err := t.logDb.QueryContext(
		ctx,
		`
		SELECT 
		    DestIp,
		    COUNT(*) AS Reqs,
		    MAX(Id) AS LastId,
		    MIN(Ts) AS FirstTs,
		    MAX(Ts) AS LastTs
		FROM 
		    LogRecords
		WHERE
			Id > ?
		  	AND LogLineType == "LogLineTypeProxyRequest"
		GROUP BY 
		    DestIp
		HAVING
		    Reqs >= 5
		ORDER BY
		    Reqs DESC
		`,
		fromId,
	)
	if err != nil {
		return nil, errors.Join(errors.New("error when executing GetDestHostsReportData query"), err)
	}
	defer func() {
		log.Trace("Closing query for GetDestHostsReportData")
		if err := res.Close(); err != nil {
			log.Warnf("Unable to close query for GetDestHostsReportData: %s", err)
		}
	}()

	var items []DestHostsReportData
	for res.Next() {
		var curData DestHostsReportData
		if err := res.Scan(&curData.DestIp, &curData.Reqs, &curData.LastId, &curData.FirstTs, &curData.LastTs); err != nil {
			return nil, errors.Join(errors.New("error when fetching element in GetDestHostsReportData"), err)
		}
		if curData.DestIp == "" {
			curData.DestIp = "<empty>"
		}
		curData.FirstTime = time.Unix(int64(curData.FirstTs), 0).UTC().Format(time.RFC3339)
		curData.LastTime = time.Unix(int64(curData.LastTs), 0).UTC().Format(time.RFC3339)
		items = append(items, curData)
	}

	return items, nil
}

func (t *LogDb) SetLastHandledLogTimeNow() error {
	return t.SetLastHandledLogTime(time.Now())
}

func (t *LogDb) SetLastHandledLogTime(lastTime time.Time) error {
	log.Tracef("Executing SetLastHandledLogTime(%s)", lastTime)
	return t.SetKvRecord("LastLogTime", lastTime.Unix())
}

func (t *LogDb) GetLastHandledTime() (time.Time, error) {
	log.Trace("Executing GetLastHandledTime()")
	ts, err := t.GetKvIntRecord("LastLogTime")
	if err != nil {
		return time.Time{}, errors.Join(errors.New("unable to GetLastHandledTime"), err)
	}

	tm := time.Unix(int64(ts), 0)
	if tm.Year() < 2024 {
		tm = time.Unix(time.Now().Unix()-int64(7*24*time.Hour/time.Second), 0)
	}
	return tm, nil
}

func (t *LogDb) SetLastId(lastId int) error {
	log.Tracef("Executing SetLastId(%d)", lastId)
	return t.SetKvRecord("LastId", lastId)
}

func (t *LogDb) GetLastId() (int, error) {
	log.Trace("Executing GetLastId()")
	return t.GetKvIntRecord("LastId")
}

func (t *LogDb) GetKvIntRecord(key string) (int, error) {
	log.Tracef("Executing GetKvIntRecord(%s)", key)

	res := t.kvDb.QueryRow(`SELECT CAST(Value AS INTEGER) FROM KvData WHERE Name=? LIMIT 1`, key)
	err := res.Err()
	if err != nil {
		return 0, errors.Join(errors.New("unable to execute GetKvIntRecord query"), err)
	}

	var val int
	err = res.Scan(&val)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	} else if err != nil {
		return 0, errors.Join(errors.New("unable to fetch element in GetKvIntRecord"), err)
	}

	return val, nil
}

func (t *LogDb) GetKvStrRecord(key string) (string, error) {
	log.Tracef("Executing GetKvStrRecord(%s)", key)

	res := t.kvDb.QueryRow(`SELECT Value FROM KvData WHERE Name=? LIMIT 1`, key)
	err := res.Err()
	if err != nil {
		return "", errors.Join(errors.New("unable to execute GetKvStrRecord query"), err)
	}

	var val string
	err = res.Scan(&val)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		return "", nil
	} else if err != nil {
		return "", errors.Join(errors.New("unable to fetch element in GetKvStrRecord"), err)
	}

	return val, nil
}

func (t *LogDb) SetKvRecord(key string, val interface{}) error {
	log.Tracef("Executing SetKvRecord(%s, %v)", key, val)
	if _, err := t.kvDb.Exec(`REPLACE INTO KvData (Name, Value) VALUES (?, CAST(? AS TEXT))`, key, val); err != nil {
		return errors.Join(errors.New("unable to execute SetKvRecord query"), err)
	}
	return nil
}

func (t *LogDb) LogRecordsVacuumClean(maxAge time.Duration) (int64, error) {
	log.Tracef("Executing LogRecordsVacuumClean(%d)", maxAge)
	borderTs := time.Now().Unix() - int64(maxAge/time.Second)
	res, err := t.logDb.Exec(`DELETE FROM LogRecords WHERE Ts < ?`, borderTs)
	if err != nil {
		return 0, errors.Join(errors.New("unable to execute LogRecordsVacuumClean query"), err)
	}
	return res.RowsAffected()
}

func (t *LogDb) CacheDataVacuumClean(maxAge time.Duration) (int64, error) {
	log.Tracef("Executing CachedDataVacuumClean(%d)", maxAge)
	borderTs := time.Now().Unix() - int64(maxAge/time.Second)
	res, err := t.cacheDb.Exec(`DELETE FROM CacheData WHERE Ts < ?`, borderTs)
	if err != nil {
		return 0, errors.Join(errors.New("unable to execute CacheDataVacuumClean query"), err)
	}
	return res.RowsAffected()
}

func (t *LogDb) WriteRecordsFromChannel(logCh chan *LogLineData) {
	log.Trace("Executing WriteRecordsFromChannel(logCh, wg)")
	defer log.Infoln("WriteRecordsFromChannel is finished")

	insertQuery, err := t.logDb.Prepare(`
		INSERT INTO
			LogRecords (
				Ts, 
				LogLineType, 
				LogLine, 
				LogTime, 
				IsError, 
				HasRequestInfo, 
				Host, 
				Pid, 
				FileName, 
				FileLine, 
				SrcIp, 
				DestIp, 
				DestPort, 
				Username, 
				Proto, 
				Method, 
				Url, 
				Status, 
				ErrorMessage
			)
		VALUES 
			(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		log.Panicf("unable to prepare insert query in WriteRecordsFromChannel: %s", err)
	}
	defer CloseOrWarn(insertQuery)

	for item := range logCh {
		_, err := insertQuery.Exec(
			time.Now().Unix(),
			item.LogLineType.String(),
			item.LogLine,
			item.LogTime.Unix(),
			item.IsError,
			item.HasRequestInfo,
			item.Host,
			item.Pid,
			item.FileName,
			item.FileLine,
			item.SrcIp,
			item.DestIp,
			item.DestPort,
			item.Username,
			item.Proto,
			item.Method,
			item.Url,
			item.Status,
			item.ErrorMessage,
		)
		if err != nil {
			log.Errorf("Can not insert data: %s", err)
		}
	}
}

func (t *LogDb) GetCached(cacheKey string, getter func() (string, error)) (string, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), QueryTimeout)
	defer cancelFunc()

	var tx *sql.Tx
	var err error
	var _ interface{}

	if tx, err = t.cacheDb.BeginTx(ctx, &sql.TxOptions{}); err != nil {
		return "", errors.Join(errors.New("unable to start CacheData transaction"), err)
	}

	res := tx.QueryRow("SELECT Value FROM CacheData WHERE Key == ? LIMIT 1", cacheKey)
	if err = res.Err(); err != nil {
		WarnIfErr(tx.Rollback())
		return "", errors.Join(errors.New("unable to start CacheData query"), err)
	}

	var value string
	haveData := true
	if err = res.Scan(&value); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			WarnIfErr(tx.Rollback())
			return "", errors.Join(errors.New("unable to get CacheData item"), err)
		}
		haveData = false
	}

	now := time.Now().Unix()

	commitTx := func() error {
		if err = tx.Commit(); err != nil {
			return errors.Join(errors.New("unable to commit CacheData transaction"), err)
		}
		return nil
	}

	if haveData {
		if _, err = tx.Exec("UPDATE CacheData SET Ts = ? WHERE Key == ? LIMIT 1", now, cacheKey); err != nil {
			WarnIfErr(tx.Rollback())
			return "", err
		}
		if err = commitTx(); err != nil {
			WarnIfErr(tx.Rollback())
			return "", err
		}
		return value, nil
	}

	if value, err = getter(); err != nil {
		WarnIfErr(tx.Rollback())
		return "", errors.Join(errors.New("unable to get new value"), err)
	}

	if _, err = tx.Exec("INSERT INTO CacheData (Value, Ts) VALUES (?, ?)", value, now); err != nil {
		WarnIfErr(tx.Rollback())
		return "", errors.Join(errors.New("unable to set new value to DB"), err)
	}

	if err = commitTx(); err != nil {
		return "", err
	}
	return value, nil
}

func (t *LogDb) initSchema() error {
	logTablesSql := []string{
		`CREATE TABLE IF NOT EXISTS LogRecords (
			Id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
			Ts INTEGER NOT NULL,
			LogLineType TEXT NOT NULL,
			LogLine TEXT NOT NULL,
			LogTime INTEGER NOT NULL,
			IsError INTEGER NOT NULL,
			HasRequestInfo INTEGER NOT NULL,
			Host TEXT NOT NULL,
			Pid INTEGER NOT NULL,
			FileName TEXT NOT NULL,
			FileLine TEXT NOT NULL,
			SrcIp TEXT NOT NULL,
			DestIp TEXT NOT NULL,
			DestPort INTEGER NOT NULL,
			Username TEXT NOT NULL,
			Proto TEXT NOT NULL,
			Method TEXT NOT NULL,
			Url TEXT NOT NULL,
			Status INTEGER NOT NULL,
			ErrorMessage TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS Id_LogLineType ON LogRecords (Id, LogLineType)`,
		`CREATE INDEX IF NOT EXISTS Ts ON LogRecords (Ts)`,
	}

	for _, tableQuery := range logTablesSql {
		if _, err := t.logDb.Exec(tableQuery); err != nil {
			return errors.Join(fmt.Errorf("unable to init schema for logDb, query %s", tableQuery), err)
		}
	}

	kvTablesSql := []string{
		`CREATE TABLE IF NOT EXISTS KvData (
			Name TEXT NOT NULL PRIMARY KEY,
			Value TEXT NOT NULL
		)`,
		`REPLACE INTO KvData (Name, Value) VALUES ("SchemaVersion", "2")`,
	}

	for _, tableQuery := range kvTablesSql {
		if _, err := t.kvDb.Exec(tableQuery); err != nil {
			return errors.Join(fmt.Errorf("unable to init schema for kvDb, query %s", tableQuery), err)
		}
	}

	cacheTablesSql := []string{
		`CREATE TABLE IF NOT EXISTS CacheData (
			Key TEXT NOT NULL PRIMARY KEY,
			Value TEXT NOT NULL,
			Ts INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS Ts ON CacheData (Ts)`,
	}

	for _, tableQuery := range cacheTablesSql {
		if _, err := t.cacheDb.Exec(tableQuery); err != nil {
			return errors.Join(fmt.Errorf("unable to init schema for cacheDb, query %s", tableQuery), err)
		}
	}

	return nil
}

func (t *LogDb) checkSchemaVersion() error {
	version, err := t.GetKvIntRecord("SchemaVersion")
	if err != nil {
		return err
	}

	if version == 1 {
		log.Fatalf("Version %d is not supported. Please backup the old DB and delete ir", version)
	} else if version != 2 {
		log.Fatalf("Version is incorrect: %d", version)
	}

	return nil
}
