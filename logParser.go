package main

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/oriser/regroup"
	log "github.com/sirupsen/logrus"
)

var systemDLogRe = regroup.MustCompile("^(?P<month>\\w+)\\s+(?P<day>\\d+)\\s+(?P<hour>\\d+):(?P<minute>\\d+):(?P<sec>\\d+)\\s+(?P<host>\\S+)\\s+(?P<unit>[\\w.-]+)\\[(?P<pid>\\d+)]:\\s+(?P<logRecord>.+)$")
var dumbProxyLogRe = regroup.MustCompile("^(?P<logger>\\w+)\\s+:\\s+(?P<year>\\d+)/(?P<month>\\d+)/(?P<day>\\d+)\\s+(?P<hour>\\d+):(?P<minute>\\d+):(?P<sec>\\d+)\\s+(?P<fileName>[^:]+):(?P<line>\\d+):(?:\\s+(?P<levelName>[A-Z]+))?\\s+(?P<logRecord>.+)$")
var dumbProxyRequestLineRe = regroup.MustCompile("^Request:\\s+(?P<srcIp>\\S+):\\d+\\s+=>\\s+(?P<destIp>\\S+):(?P<destPort>\\d+)\\s+\"(?P<username>[^\"]*)\"\\s+(?P<proto>\\S+)\\s+(?P<method>[A-Z]+)\\s+(?P<url>.+)$")

type SystemDLogLineRecord struct {
	Month     string `regroup:"month"`
	Day       int    `regroup:"day"`
	Hour      int    `regroup:"hour"`
	Minute    int    `regroup:"minute"`
	Sec       int    `regroup:"sec"`
	LogTime   time.Time
	Host      string `regroup:"host"`
	Unit      string `regroup:"unit"`
	Pid       int    `regroup:"pid"`
	LogRecord string `regroup:"logRecord"`
}

type DumbProxyLogLineRecord struct {
	Year      int        `regroup:"year"`
	Month     time.Month `regroup:"month"`
	Day       int        `regroup:"day"`
	Hour      int        `regroup:"hour"`
	Minute    int        `regroup:"minute"`
	Sec       int        `regroup:"sec"`
	LogTime   time.Time
	Logger    string `regroup:"logger"`
	FileName  string `regroup:"fileName"`
	FileLine  int    `regroup:"line"`
	LevelName string `regroup:"levelName"`
	LogRecord string `regroup:"logRecord"`
}

type LogLineType uint64

const (
	LogLineTypeUnmatched LogLineType = iota
	LogLineTypeOtherUnit
	LogLineTypeProxyUnknown

	LogLineTypeProxyRequest
	LogLineTypeProxyRequestHttpInfo
	LogLineTypeProxyRequestError
	LogLineTypeHttpSrvError

	LogLineTypeRuntimeLog
	LogLineTypeAuthModuleLog
)

func (t LogLineType) String() string {
	switch t {
	case LogLineTypeUnmatched:
		return "LogLineTypeUnmatched"
	case LogLineTypeOtherUnit:
		return "LogLineTypeOtherUnit"
	case LogLineTypeProxyUnknown:
		return "LogLineTypeProxyUnknown"
	case LogLineTypeProxyRequest:
		return "LogLineTypeProxyRequest"
	case LogLineTypeProxyRequestHttpInfo:
		return "LogLineTypeProxyRequestHttpInfo"
	case LogLineTypeProxyRequestError:
		return "LogLineTypeProxyRequestError"
	case LogLineTypeHttpSrvError:
		return "LogLineTypeHttpSrvError"
	case LogLineTypeRuntimeLog:
		return "LogLineTypeHttpSrvError"
	case LogLineTypeAuthModuleLog:
		return "LogLineTypeAuthModuleLog"
	default:
		return "LogLineType__UNKNOWN"
	}
}

var monthMap = map[string]time.Month{
	"Jan": time.January,
	"Feb": time.February,
	"Mar": time.March,
	"Apr": time.April,
	"May": time.May,
	"Jun": time.June,
	"Jul": time.July,
	"Aug": time.August,
	"Sep": time.September,
	"Oct": time.October,
	"Nov": time.November,
	"Dec": time.December,
}

type LogLineData struct {
	LogLineType    LogLineType
	LogLine        string
	LogTime        time.Time
	IsError        bool
	HasRequestInfo bool
	Host           string
	Pid            int
	FileName       string
	FileLine       int
	SrcIp          string
	DestIp         string
	DestPort       int
	Username       string
	Proto          string
	Method         string
	Url            string
	Status         int
	ErrorMessage   string
}

type requestLogRecord struct {
	SrcIp    string `regroup:"srcIp"`
	DestIp   string `regroup:"destIp"`
	DestPort int    `regroup:"destPort"`
	Username string `regroup:"username"`
	Proto    string `regroup:"proto"`
	Method   string `regroup:"method"`
	Url      string `regroup:"url"`
}

var ErrorParse = errors.New("parse error")

func ParseLogLine(logLine string) (*LogLineData, error) {
	res := new(LogLineData)
	res.LogTime = time.Now()
	res.LogLine = logLine

	sysLogRes, err := ParseSystemDLogLine(logLine)
	if err != nil {
		if errors.Is(err, ErrorParse) {
			res.LogLineType = LogLineTypeUnmatched
			return res, nil
		}
		return nil, errors.Join(errors.New("SystemD parse error"), err)
	}

	res.LogTime = sysLogRes.LogTime
	res.Host = sysLogRes.Host
	res.Pid = sysLogRes.Pid
	if sysLogRes.Unit != "dumbproxy" {
		res.LogLineType = LogLineTypeOtherUnit
		return res, nil
	}

	dumbProxyRes, err := ParseDumbProxyLogLine(sysLogRes.LogRecord)
	res.LogLineType = LogLineTypeProxyUnknown
	if err != nil {
		if errors.Is(err, ErrorParse) {
			return res, nil
		}
		return nil, errors.Join(errors.New("dumbproxy parse error"), err)
	}

	res.LogLineType = LogLineTypeProxyUnknown
	res.LogTime = dumbProxyRes.LogTime
	res.FileName = dumbProxyRes.FileName
	res.FileLine = dumbProxyRes.FileLine

	switch dumbProxyRes.Logger {
	case "PROXY":
		if dumbProxyRes.LevelName == "INFO" {
			if strings.HasPrefix(dumbProxyRes.LogRecord, "Request: ") {
				var curData requestLogRecord
				if err := dumbProxyRequestLineRe.MatchToTarget(dumbProxyRes.LogRecord, &curData); err != nil {
					return res, nil
				}
				res.LogLineType = LogLineTypeProxyRequest
				res.SrcIp = curData.SrcIp
				res.DestIp = curData.DestIp
				res.DestPort = curData.DestPort
				res.Username = curData.Username
				res.Proto = curData.Proto
				res.Method = curData.Method
				res.Url = curData.Url
				res.HasRequestInfo = true
			} else {
				parts := strings.Split(dumbProxyRes.LogRecord, " ")
				if len(parts) != 5 {
					return res, nil
				}
				res.LogLineType = LogLineTypeProxyRequestHttpInfo
				res.SrcIp = parts[0][:strings.LastIndex(parts[0], ":")]
				res.Method = parts[1]
				res.Url = parts[2]
				if res.Status, err = strconv.Atoi(parts[3]); err != nil {
					log.Errorf("Unable to parse status code: %s", err)
				}
				res.HasRequestInfo = true
			}
		} else {
			res.IsError = true
			res.LogLineType = LogLineTypeProxyRequestError
			res.ErrorMessage = dumbProxyRes.LogRecord
		}
		break
	case "HTTPSRV":
		res.IsError = true
		res.LogLineType = LogLineTypeHttpSrvError
		res.ErrorMessage = dumbProxyRes.LogRecord
		break
	case "MAIN":
		res.IsError = dumbProxyRes.LevelName == "ERROR" || dumbProxyRes.LevelName == "CRITICAL"
		res.LogLineType = LogLineTypeRuntimeLog
		res.ErrorMessage = dumbProxyRes.LogRecord
		break
	case "AUTH":
		res.IsError = dumbProxyRes.LevelName == "ERROR" || dumbProxyRes.LevelName == "CRITICAL"
		res.LogLineType = LogLineTypeAuthModuleLog
		res.ErrorMessage = dumbProxyRes.LogRecord
		break
	}

	return res, nil
}

func ParseSystemDLogLine(logLine string) (*SystemDLogLineRecord, error) {
	var data SystemDLogLineRecord
	if err := systemDLogRe.MatchToTarget(logLine, &data); err != nil {
		return nil, errors.Join(errors.New("invalid SystemD log record format"), ErrorParse, err)
	}

	now := time.Now()
	month, ok := monthMap[data.Month]
	if !ok {
		month = now.Month()
	}

	year := now.Year()
	if now.Month() == time.January && month == time.December {
		year--
	}
	data.LogTime = time.Date(year, month, data.Day, data.Hour, data.Minute, data.Sec, 0, time.Local)

	return &data, nil
}

func ParseDumbProxyLogLine(systemDLogLine string) (*DumbProxyLogLineRecord, error) {
	var data DumbProxyLogLineRecord
	if err := dumbProxyLogRe.MatchToTarget(systemDLogLine, &data); err != nil {
		return nil, errors.Join(errors.New("dumbproxy log parse error"), err)
	}
	data.LogTime = time.Date(data.Year, data.Month, data.Day, data.Hour, data.Minute, data.Sec, 0, time.Local)

	return &data, nil
}
