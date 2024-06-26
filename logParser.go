package main

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/oriser/regroup"
	log "github.com/sirupsen/logrus"
)

var systemDLogRe = regroup.MustCompile("^(?P<month>\\w+)\\s+(?P<day>\\d+)\\s+(?P<hour>\\d+):(?P<minute>\\d+):(?P<sec>\\d+)\\s+(?P<host>\\S+)\\s+(?P<unit>[\\w.-]+)\\[(?P<pid>\\d+)]:\\s+(?P<logRecord>.+)$")
var dumbProxyLogRe = regroup.MustCompile("^(?P<logger>\\w+)\\s+:\\s+(?P<year>\\d+)/(?P<month>\\d+)/(?P<day>\\d+)\\s+(?P<hour>\\d+):(?P<minute>\\d+):(?P<sec>\\d+)\\s+(?P<fileName>[^:]+):(?P<line>\\d+):(?:\\s+(?P<levelName>[A-Z]+))?\\s+(?P<logRecord>.+)$")
var dumbProxyRequestLineRe = regroup.MustCompile("^Request:\\s+(?P<srcIp>\\S+):\\d+\\s+=>\\s+(?P<destIp>\\S+):(?P<destPort>\\d+)\\s+\"(?P<username>[^\"]*)\"\\s+(?P<proto>\\S+)\\s+(?P<method>[A-Z]+)\\s+(?P<url>.+)$")

var logLineGeneralRe = regexp.MustCompile(".+dumbproxy\\[\\d+]:\\s+(?P<logger>[\\w-]+)\\s+: (?P<year>[0-9]+)/(?P<month>[0-9]+)/(?P<day>[0-9]+) (?P<hour>[0-9]+):(?P<min>[0-9]+):(?P<sec>[0-9]+) [\\w.-]+:\\d+: (?P<level>[A-Z]+)\\s+(?P<ip>\\S+):\\d+ [A-Z]+")
var logLineRequestRe = regexp.MustCompile(".+dumbproxy\\[\\d+]:\\s+(?P<logger>[\\w-]+)\\s+: (?P<year>[0-9]+)/(?P<month>[0-9]+)/(?P<day>[0-9]+) (?P<hour>[0-9]+):(?P<min>[0-9]+):(?P<sec>[0-9]+) [\\w.-]+:\\d+: (?P<level>[A-Z]+)\\s+Request:\\s+(?P<ip>\\S+):\\d+ => (?P<dest>\\S+?)(?::\\d+)? \"(?P<user>[\\w-]*)\"(?:\\s+(?:HTTP/[\\d.]+)?\\s*[A-Z]+ (?:https?:)?//(?P<host>[\\w.-]+?)(?::\\d+)?/)?")
var logLineErrorRe = regexp.MustCompile(".+dumbproxy\\[\\d+]:\\s+(?P<logger>[\\w-]+)\\s+: (?P<year>[0-9]+)/(?P<month>[0-9]+)/(?P<day>[0-9]+) (?P<hour>[0-9]+):(?P<min>[0-9]+):(?P<sec>[0-9]+) .+error from (?P<ip>\\S+):\\d+")
var logLineDnsRe = regexp.MustCompile(".+lookup (?P<host>\\S+): Temporary failure in name resolution.*")
var logLineConnectionRefusedRe = regexp.MustCompile(".+dial tcp (?P<dest>\\S+):\\d+: connect: connection refused.*")
var logLineCantDialRe = regexp.MustCompile(".+Can't satisfy \\S+ request: dial tcp:? (?:lookup )?(?P<host>\\S+):.*")
var internalErrorRe = regexp.MustCompile(".*No such file or directory|Main process exited|Failed with result 'exit-code'.*")
var serviceMessageRe = regexp.MustCompile(".+Stopping Dumb Proxy|Deactivated successfully|Started Dumb Proxy|Stopped Dumb Proxy|dumbproxy.service: Consumed|reloading password file|password file reloaded|Starting proxy server|Proxy server started.+")

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

	// LogLineTypeGeneral TODO: REMOVE
	LogLineTypeGeneral
	LogLineTypeRequest
	LogLineTypeError
	LogLineTypeDnsError
	LogLineTypeConnectionRefusedError
	LogLineTypeCantDialError
	LogLineTypeInternalError
	LogLintTypeJustMessage
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

type LogLineData2 struct {
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

type LogLineData struct {
	LogLineType LogLineType
	DateTime    time.Time
	LoggerName  string
	Level       string
	SrcIp       net.IP
	DestIp      net.IP
	DestHost    string
	User        string
}

var ErrorLogLineNotMatch = errors.New("log line doesn't match")
var ErrorParse = errors.New("parse error")

func ParseLogLine2(logLine string) (*LogLineData2, error) {
	res := new(LogLineData2)
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

// ParseLogLine TODO: Remove
func ParseLogLine(logLine string) (*LogLineData, error) {
	res, err := ParseLogLineGeneral(logLine)
	if err == nil {
		return res, nil
	} else if !errors.Is(err, ErrorLogLineNotMatch) {
		return nil, err
	}

	res, err = ParseLogLineRequest(logLine)
	if err == nil {
		return res, nil
	} else if !errors.Is(err, ErrorLogLineNotMatch) {
		return nil, err
	}

	res, err = ParseLogLineError(logLine)
	if err == nil {
		return res, nil
	} else if !errors.Is(err, ErrorLogLineNotMatch) {
		return nil, err
	}

	if resMap := parseLogLineWithRe(logLineDnsRe, logLine); resMap != nil {
		return &LogLineData{
			LogLineType: LogLineTypeDnsError,
			DestHost:    resMap["host"],
		}, nil
	}

	if resMap := parseLogLineWithRe(logLineConnectionRefusedRe, logLine); resMap != nil {
		destIp, ipErr := parseIp(resMap["dest"])
		if ipErr != nil {
			return nil, ipErr
		}
		return &LogLineData{
			LogLineType: LogLineTypeConnectionRefusedError,
			DestIp:      destIp,
		}, nil
	}

	if resMap := parseLogLineWithRe(logLineCantDialRe, logLine); resMap != nil {
		host := resMap["host"]
		if strings.HasSuffix(host, ":443") {
			host = host[:len(host)-4]
		} else if strings.HasSuffix(host, ":80") {
			host = host[:len(host)-3]
		}
		return &LogLineData{
			LogLineType: LogLineTypeCantDialError,
			DestHost:    host,
		}, nil
	}

	if internalErrorRe.MatchString(logLine) {
		return &LogLineData{LogLineType: LogLineTypeInternalError}, nil
	}

	if serviceMessageRe.MatchString(logLine) {
		return &LogLineData{LogLineType: LogLintTypeJustMessage}, nil
	}

	return nil, errors.Join(ErrorLogLineNotMatch, fmt.Errorf("unexpected log line: \"%s\"", logLine))
}

func ParseLogLineGeneral(logLine string) (*LogLineData, error) {
	resMap := parseLogLineWithRe(logLineGeneralRe, logLine)
	if resMap == nil {
		return nil, errors.Join(ErrorLogLineNotMatch, fmt.Errorf("unexpected log line: \"%s\"", logLine))
	}

	var err *multierror.Error
	dt, err := parseDateTime(resMap, err)
	err = validateLogEntries(resMap, err)

	ip, ipErr := parseIp(resMap["ip"])
	if ipErr != nil {
		err = multierror.Append(err, ipErr)
	}

	resErr := err.Unwrap()
	if resErr != nil {
		return nil, resErr
	}

	res := LogLineData{
		LogLineType: LogLineTypeGeneral,
		DateTime:    dt,
		LoggerName:  resMap["logger"],
		Level:       resMap["level"],
		SrcIp:       ip,
	}
	return &res, nil
}

func ParseLogLineRequest(logLine string) (*LogLineData, error) {
	resMap := parseLogLineWithRe(logLineRequestRe, logLine)
	if resMap == nil {
		return nil, errors.Join(ErrorLogLineNotMatch, fmt.Errorf("unexpected log line: \"%s\"", logLine))
	}

	var err *multierror.Error
	dt, err := parseDateTime(resMap, err)
	err = validateLogEntries(resMap, err)

	srcIp, srcIpErr := parseIp(resMap["ip"])
	if srcIpErr != nil {
		err = multierror.Append(err, errors.Join(srcIpErr, errors.New("src IP")))
	}
	destIp, destIpErr := parseIp(resMap["dest"])
	if destIpErr != nil {
		err = multierror.Append(err, errors.Join(destIpErr, errors.New("dest IP")))
	}

	resErr := err.Unwrap()
	if resErr != nil {
		return nil, resErr
	}

	res := LogLineData{
		LogLineType: LogLineTypeRequest,
		DateTime:    dt,
		LoggerName:  resMap["logger"],
		Level:       resMap["level"],
		SrcIp:       srcIp,
		DestIp:      destIp,
		User:        resMap["user"],
		DestHost:    resMap["host"],
	}
	return &res, nil
}

func ParseLogLineError(logLine string) (*LogLineData, error) {
	resMap := parseLogLineWithRe(logLineErrorRe, logLine)
	if resMap == nil {
		return nil, errors.Join(ErrorLogLineNotMatch, fmt.Errorf("unexpected log line: \"%s\"", logLine))
	}

	var err *multierror.Error
	dt, err := parseDateTime(resMap, err)
	if resMap["logger"] == "" {
		err = multierror.Append(err, errors.Join(ErrorParse, errors.New("empty logger")))
	}

	ip, ipErr := parseIp(resMap["ip"])
	if ipErr != nil {
		err = multierror.Append(err, ipErr)
	}

	resErr := err.Unwrap()
	if resErr != nil {
		return nil, resErr
	}

	res := LogLineData{
		LogLineType: LogLineTypeError,
		DateTime:    dt,
		LoggerName:  resMap["logger"],
		Level:       resMap["level"],
		SrcIp:       ip,
	}
	return &res, nil
}

func parseLogLineWithRe(re *regexp.Regexp, logLine string) map[string]string {
	matchRes := re.FindStringSubmatch(logLine)
	if matchRes == nil {
		return nil
	}

	names := re.SubexpNames()
	res := make(map[string]string, len(names))
	for idx, name := range names {
		res[name] = matchRes[idx]
	}

	return res
}

func parseDateTime(resMap map[string]string, err *multierror.Error) (time.Time, *multierror.Error) {
	var year, month, day, hour, minute, sec int
	var convErr error
	if year, convErr = strconv.Atoi(resMap["year"]); convErr != nil {
		err = multierror.Append(err, errors.Join(ErrorParse, fmt.Errorf("year parse error: %s", convErr)))
	}
	if month, convErr = strconv.Atoi(resMap["month"]); convErr != nil {
		err = multierror.Append(err, errors.Join(ErrorParse, fmt.Errorf("month parse error: %s", convErr)))
	}
	if day, convErr = strconv.Atoi(resMap["day"]); convErr != nil {
		err = multierror.Append(err, errors.Join(ErrorParse, fmt.Errorf("day parse error: %s", convErr)))
	}
	if hour, convErr = strconv.Atoi(resMap["hour"]); convErr != nil {
		err = multierror.Append(err, errors.Join(ErrorParse, fmt.Errorf("hour parse error: %s", convErr)))
	}
	if minute, convErr = strconv.Atoi(resMap["min"]); convErr != nil {
		err = multierror.Append(err, errors.Join(ErrorParse, fmt.Errorf("minute parse error: %s", convErr)))
	}
	if sec, convErr = strconv.Atoi(resMap["sec"]); convErr != nil {
		err = multierror.Append(err, errors.Join(ErrorParse, fmt.Errorf("sec parse error: %s", convErr)))
	}

	dt := time.Date(year, time.Month(month), day, hour, minute, sec, 0, time.UTC)
	return dt, err
}

func validateLogEntries(resMap map[string]string, err *multierror.Error) *multierror.Error {
	if resMap["logger"] == "" {
		err = multierror.Append(err, errors.Join(ErrorParse, errors.New("empty logger")))
	}

	if resMap["level"] == "" {
		err = multierror.Append(err, errors.Join(ErrorParse, errors.New("empty level")))
	}
	return err
}

func parseIp(ipStr string) (net.IP, error) {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return nil, errors.Join(ErrorParse, fmt.Errorf("wrong IP: '%s'", ipStr))
	}
	return ip, nil
}
