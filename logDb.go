package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
)

type LogDb struct {
	logDb   *sqlx.DB
	kvDb    *sqlx.DB
	cacheDb *sqlx.DB
}

type BasicGroupReportData struct {
	Reqs      int    `db:"Reqs"`
	LastId    int    `db:"LastId"`
	FirstTs   int    `db:"FirstTs"`
	LastTs    int    `db:"LastTs"`
	FirstTime string `db:"FirstTime"`
	LastTime  string `db:"LastTime"`
}

type SrcIpReportData struct {
	BasicGroupReportData
	SrcIp string `db:"SrcIp"`
}

type UsersReportData struct {
	BasicGroupReportData
	Username string `db:"Username"`
}

type DestHostsReportData struct {
	BasicGroupReportData
	DestIp   string `db:"DestIp"`
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

	logDb, err := sqlx.Open("sqlite3", logDbPath)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("unable to execute sql.Open for logDb: %s", logDbPath), err)
	}

	kvDb, err := sqlx.Open("sqlite3", kvDbPath)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("unable to execute sql.Open for kvDb: %s", kvDbPath), err)
	}

	cacheDb, err := sqlx.Open("sqlite3", "file:cacheDb?mode=memory")
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
	ctx, cancel := context.WithTimeout(context.Background(), QueryTimeout)
	defer cancel()

	var items []SrcIpReportData
	err := t.logDb.SelectContext(
		ctx,
		&items,
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
		return nil, errors.Join(errors.New("error when GetSrcIpReportData"), err)
	}

	for i := 0; i < len(items); i++ {
		items[i].SrcIp = StrDef(items[i].SrcIp, "<empty>")
		setTimes(&items[i].BasicGroupReportData)
	}
	return items, nil
}

func (t *LogDb) GetUsersReportData(fromId int) ([]UsersReportData, error) {
	log.Tracef("Executing GetUsersReportData(%d)", fromId)
	ctx, cancel := context.WithTimeout(context.Background(), QueryTimeout)
	defer cancel()

	var items []UsersReportData
	err := t.logDb.SelectContext(
		ctx,
		&items,
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
		return nil, errors.Join(errors.New("error when GetUsersReportData"), err)
	}

	for i := 0; i < len(items); i++ {
		items[i].Username = StrDef(items[i].Username, "<empty>")
		setTimes(&items[i].BasicGroupReportData)
	}
	return items, nil
}

func (t *LogDb) GetDestHostsReportData(fromId int) ([]DestHostsReportData, error) {
	log.Tracef("Executing GetDestHostsReportData(%d)", fromId)
	ctx, cancel := context.WithTimeout(context.Background(), QueryTimeout)
	defer cancel()

	var items []DestHostsReportData
	err := t.logDb.SelectContext(
		ctx,
		&items,
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
		return nil, errors.Join(errors.New("error when GetDestHostsReportData"), err)
	}

	for i := 0; i < len(items); i++ {
		items[i].DestIp = StrDef(items[i].DestIp, "<empty>")
		setTimes(&items[i].BasicGroupReportData)
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
	ctx, cancel := context.WithTimeout(context.Background(), QueryTimeout)
	defer cancel()

	var val int
	err := t.kvDb.GetContext(ctx, &val, `SELECT CAST(Value AS INTEGER) FROM KvData WHERE Name == ? LIMIT 1`, key)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	} else if err != nil {
		return 0, errors.Join(errors.New("unable to GetKvIntRecord"), err)
	}

	return val, nil
}

func (t *LogDb) GetKvStrRecord(key string) (string, error) {
	log.Tracef("Executing GetKvStrRecord(%s)", key)
	ctx, cancel := context.WithTimeout(context.Background(), QueryTimeout)
	defer cancel()

	var val string
	err := t.kvDb.GetContext(ctx, &val, `SELECT Value FROM KvData WHERE Name == ? LIMIT 1`, key)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		return "", nil
	} else if err != nil {
		return "", errors.Join(errors.New("unable to GetKvIntRecord"), err)
	}

	return val, nil
}

func (t *LogDb) SetKvRecord(key string, val interface{}) error {
	log.Tracef("Executing SetKvRecord(%s, %v)", key, val)
	ctx, cancel := context.WithTimeout(context.Background(), QueryTimeout)
	defer cancel()

	if _, err := t.kvDb.ExecContext(ctx, `REPLACE INTO KvData (Name, Value) VALUES (?, CAST(? AS TEXT))`, key, val); err != nil {
		return errors.Join(errors.New("unable to execute SetKvRecord query"), err)
	}
	return nil
}

func (t *LogDb) LogRecordsVacuumClean(maxAge time.Duration) (int64, error) {
	log.Tracef("Executing LogRecordsVacuumClean(%d)", maxAge)
	ctx, cancel := context.WithTimeout(context.Background(), QueryTimeout)
	defer cancel()

	borderTs := time.Now().Unix() - int64(maxAge/time.Second)
	res, err := t.logDb.ExecContext(ctx, `DELETE FROM LogRecords WHERE Ts < ?`, borderTs)
	if err != nil {
		return 0, errors.Join(errors.New("unable to execute LogRecordsVacuumClean query"), err)
	}
	return res.RowsAffected()
}

func (t *LogDb) CacheDataVacuumClean(maxAge time.Duration) (int64, error) {
	log.Tracef("Executing CachedDataVacuumClean(%d)", maxAge)
	ctx, cancel := context.WithTimeout(context.Background(), QueryTimeout)
	defer cancel()

	borderTs := time.Now().Unix() - int64(maxAge/time.Second)
	res, err := t.cacheDb.ExecContext(ctx, `DELETE FROM CacheData WHERE Ts < ?`, borderTs)
	if err != nil {
		return 0, errors.Join(errors.New("unable to execute CacheDataVacuumClean query"), err)
	}
	return res.RowsAffected()
}

func (t *LogDb) WriteRecordsFromChannel(logCh chan *LogLineData) {
	log.Trace("Executing WriteRecordsFromChannel(logCh, wg)")
	defer log.Infoln("WriteRecordsFromChannel is finished")

	insertQuery, err := t.logDb.PrepareNamed(`
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
			(
			 	:Ts, 
				:LogLineType, 
				:LogLine, 
				:LogTime, 
				:IsError, 
				:HasRequestInfo, 
				:Host, 
				:Pid, 
				:FileName, 
				:FileLine, 
				:SrcIp, 
				:DestIp, 
				:DestPort, 
				:Username, 
				:Proto, 
				:Method, 
				:Url, 
				:Status, 
				:ErrorMessage
			)
	`)
	if err != nil {
		log.Panicf("unable to prepare insert query in WriteRecordsFromChannel: %s", err)
	}
	defer CloseOrWarn(insertQuery)

	for item := range logCh {
		if err := insertLogRecord(insertQuery, item); err != nil {
			log.Errorf("Can not insert data: %s", err)
		}
	}
}

func (t *LogDb) GetCached(cacheKey string, getter func() (string, error)) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), QueryTimeout)
	defer cancel()

	var tx *sqlx.Tx
	var err error
	var _ interface{}

	if tx, err = t.cacheDb.BeginTxx(ctx, &sql.TxOptions{}); err != nil {
		return "", errors.Join(errors.New("unable to start CacheData transaction"), err)
	}

	var value string
	err = tx.GetContext(ctx, &value, "SELECT Value FROM CacheData WHERE Key == ? LIMIT 1", cacheKey)

	haveData := true
	if err != nil {
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
		if _, err = tx.ExecContext(ctx, "UPDATE CacheData SET Ts = ? WHERE Key == ?", now, cacheKey); err != nil {
			WarnIfErr(tx.Rollback())
			return "", errors.Join(errors.New("unable to start CacheData update"), err)
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

	if _, err = tx.ExecContext(ctx, "INSERT INTO CacheData (Key, Value, Ts) VALUES (?, ?, ?)", cacheKey, value, now); err != nil {
		WarnIfErr(tx.Rollback())
		return "", errors.Join(errors.New("unable to set new value to DB"), err)
	}

	if err = commitTx(); err != nil {
		return "", err
	}
	return value, nil
}

func (t *LogDb) initSchema() error {
	var err error
	err = t.execInitQueries(
		t.logDb,
		[]string{
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
		},
	)
	if err != nil {
		return err
	}

	err = t.execInitQueries(
		t.kvDb,
		[]string{
			`CREATE TABLE IF NOT EXISTS KvData (
				Name TEXT NOT NULL PRIMARY KEY,
				Value TEXT NOT NULL
			)`,
			`REPLACE INTO KvData (Name, Value) VALUES ("SchemaVersion", "2")`,
		},
	)
	if err != nil {
		return err
	}

	err = t.execInitQueries(
		t.cacheDb,
		[]string{
			`CREATE TABLE IF NOT EXISTS CacheData (
				Key TEXT NOT NULL PRIMARY KEY,
				Value TEXT NOT NULL,
				Ts INTEGER NOT NULL
			)`,
			`CREATE INDEX IF NOT EXISTS Ts ON CacheData (Ts)`,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (t *LogDb) execInitQueries(db *sqlx.DB, initQueries []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), QueryTimeout)
	defer cancel()

	for _, query := range initQueries {
		if _, err := db.ExecContext(ctx, query); err != nil {
			return errors.Join(fmt.Errorf("unable to init schema for DB \"%+v\", query %s", db, query), err)
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

func setTimes(val *BasicGroupReportData) {
	val.FirstTime = time.Unix(int64(val.FirstTs), 0).UTC().Format(time.RFC3339)
	val.LastTime = time.Unix(int64(val.LastTs), 0).UTC().Format(time.RFC3339)
}

func insertLogRecord(insertQuery *sqlx.NamedStmt, item *LogLineData) error {
	ctx, cancel := context.WithTimeout(context.Background(), QueryTimeout)
	defer cancel()

	_, err := insertQuery.ExecContext(
		ctx,
		map[string]interface{}{
			"Ts":             time.Now().Unix(),
			"LogLineType":    item.LogLineType.String(),
			"LogLine":        item.LogLine,
			"LogTime":        item.LogTime.Unix(),
			"IsError":        item.IsError,
			"HasRequestInfo": item.HasRequestInfo,
			"Host":           item.Host,
			"Pid":            item.Pid,
			"FileName":       item.FileName,
			"FileLine":       item.FileLine,
			"SrcIp":          item.SrcIp,
			"DestIp":         item.DestIp,
			"DestPort":       item.DestPort,
			"Username":       item.Username,
			"Proto":          item.Proto,
			"Method":         item.Method,
			"Url":            item.Url,
			"Status":         item.Status,
			"ErrorMessage":   item.ErrorMessage,
		},
	)
	return err
}
