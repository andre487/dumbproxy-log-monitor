package main

import (
	"os"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestParseSystemDLogLine(t *testing.T) {
	logLine1 := readFileToString("test/data/log-line-request-http-info.txt")
	logLine2 := readFileToString("test/data/log-line-request.txt")
	logLine3 := readFileToString("test/data/log-line-httpsrv-error.txt")
	logLine4 := readFileToString("test/data/log-line-cant-dial.txt")

	var rec *SystemDLogLineRecord
	var err error
	now := time.Now()

	rec, err = ParseSystemDLogLine(logLine1)
	assert.NoError(t, err)
	assert.Equal(t, &SystemDLogLineRecord{
		Month:     "Jun",
		Day:       21,
		Hour:      13,
		Minute:    0,
		Sec:       47,
		LogTime:   time.Date(now.Year(), time.June, 21, 13, 0, 47, 0, now.Location()),
		Host:      "p487-2-am.jethelix.ru",
		Unit:      "dumbproxy",
		Pid:       111654,
		LogRecord: "PROXY   : 2024/06/21 13:00:47 handler.go:106: INFO     143.178.232.21:57190 POST http://e5.o.lencr.org/ 200 OK",
	}, rec)

	rec, err = ParseSystemDLogLine(logLine2)
	assert.NoError(t, err)
	assert.Equal(t, &SystemDLogLineRecord{
		Month:     "Jun",
		Day:       18,
		Hour:      0,
		Minute:    7,
		Sec:       26,
		LogTime:   time.Date(now.Year(), time.June, 18, 0, 7, 26, 0, now.Location()),
		Host:      "p487-2-am.jethelix.ru",
		Unit:      "dumbproxy",
		Pid:       82403,
		LogRecord: "PROXY   : 2024/06/18 00:07:26 handler.go:138: INFO     Request: 143.178.228.182:64154 => 2.56.204.64:443 \"andre487\" HTTP/1.1 GET http://ifconfig.co/",
	}, rec)

	rec, err = ParseSystemDLogLine(logLine3)
	assert.NoError(t, err)
	assert.Equal(t, &SystemDLogLineRecord{
		Month:     "Jun",
		Day:       21,
		Hour:      13,
		Minute:    0,
		Sec:       18,
		LogTime:   time.Date(now.Year(), time.June, 21, 13, 0, 18, 0, now.Location()),
		Host:      "p487-2-am.jethelix.ru",
		Unit:      "dumbproxy",
		Pid:       111654,
		LogRecord: "HTTPSRV : 2024/06/21 13:00:18 server.go:3195: http: TLS handshake error from 143.178.232.21:57019: EOF",
	}, rec)

	rec, err = ParseSystemDLogLine(logLine4)
	assert.NoError(t, err)
	assert.Equal(t, &SystemDLogLineRecord{
		Month:     "Jun",
		Day:       18,
		Hour:      11,
		Minute:    18,
		Sec:       52,
		LogTime:   time.Date(now.Year(), time.June, 18, 11, 18, 52, 0, now.Location()),
		Host:      "p487-2-am.jethelix.ru",
		Unit:      "dumbproxy",
		Pid:       96234,
		LogRecord: "PROXY   : 2024/06/18 11:18:52 handler.go:51: ERROR    Can't satisfy CONNECT request: dial tcp [2a02:6b8::5d7]:443: connect: network is unreachable",
	}, rec)

	rec, err = ParseSystemDLogLine("FOO")
	assert.ErrorContains(t, err, "invalid SystemD log record format")
	assert.Nil(t, rec)
}

func TestParseDumbProxyLogLine(t *testing.T) {
	logLine1 := readFileToString("test/data/log-line-request-http-info.txt")
	logLine2 := readFileToString("test/data/log-line-request.txt")
	logLine3 := readFileToString("test/data/log-line-httpsrv-error.txt")
	logLine4 := readFileToString("test/data/log-line-cant-dial.txt")

	var rec *DumbProxyLogLineRecord
	var err error
	now := time.Now()

	rec, err = ParseDumbProxyLogLine(Must1(ParseSystemDLogLine(logLine1)).LogRecord)
	assert.NoError(t, err)
	assert.Equal(t, &DumbProxyLogLineRecord{
		Year:      2024,
		Month:     time.June,
		Day:       21,
		Hour:      13,
		Minute:    0,
		Sec:       47,
		LogTime:   time.Date(2024, time.June, 21, 13, 0, 47, 0, time.Local),
		Logger:    "PROXY",
		FileName:  "handler.go",
		FileLine:  106,
		LevelName: "INFO",
		LogRecord: "143.178.232.21:57190 POST http://e5.o.lencr.org/ 200 OK",
	}, rec)

	rec, err = ParseDumbProxyLogLine(Must1(ParseSystemDLogLine(logLine2)).LogRecord)
	assert.NoError(t, err)
	assert.Equal(t, &DumbProxyLogLineRecord{
		Year:      2024,
		Month:     time.June,
		Day:       18,
		Hour:      0,
		Minute:    7,
		Sec:       26,
		LogTime:   time.Date(2024, time.June, 18, 0, 7, 26, 0, time.Local),
		Logger:    "PROXY",
		FileName:  "handler.go",
		FileLine:  138,
		LevelName: "INFO",
		LogRecord: "Request: 143.178.228.182:64154 => 2.56.204.64:443 \"andre487\" HTTP/1.1 GET http://ifconfig.co/",
	}, rec)

	rec, err = ParseDumbProxyLogLine(Must1(ParseSystemDLogLine(logLine3)).LogRecord)
	assert.NoError(t, err)
	assert.Equal(t, &DumbProxyLogLineRecord{
		Year:      2024,
		Month:     time.June,
		Day:       21,
		Hour:      13,
		Minute:    0,
		Sec:       18,
		LogTime:   time.Date(now.Year(), time.June, 21, 13, 0, 18, 0, time.Local),
		Logger:    "HTTPSRV",
		FileName:  "server.go",
		FileLine:  3195,
		LevelName: "",
		LogRecord: "http: TLS handshake error from 143.178.232.21:57019: EOF",
	}, rec)

	rec, err = ParseDumbProxyLogLine(Must1(ParseSystemDLogLine(logLine4)).LogRecord)
	assert.NoError(t, err)
	assert.Equal(t, &DumbProxyLogLineRecord{
		Year:      2024,
		Month:     time.June,
		Day:       18,
		Hour:      11,
		Minute:    18,
		Sec:       52,
		LogTime:   time.Date(now.Year(), time.June, 18, 11, 18, 52, 0, time.Local),
		Logger:    "PROXY",
		FileName:  "handler.go",
		FileLine:  51,
		LevelName: "ERROR",
		LogRecord: "Can't satisfy CONNECT request: dial tcp [2a02:6b8::5d7]:443: connect: network is unreachable",
	}, rec)

	rec, err = ParseDumbProxyLogLine("FOO")
	assert.ErrorContains(t, err, "dumbproxy log parse error")
	assert.Nil(t, rec)
}

func TestParseLogLine2(t *testing.T) {
	logLineReq := readFileToString("test/data/log-line-request.txt")
	logLineHttpInfo := readFileToString("test/data/log-line-request-http-info.txt")
	logLineReqError := readFileToString("test/data/log-line-request-error.txt")
	logLineHttpSrvError := readFileToString("test/data/log-line-httpsrv-error.txt")

	var res *LogLineData
	var err error

	res, err = ParseLogLine(logLineReq)
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, &LogLineData{
		LogLineType:    LogLineTypeProxyRequest,
		LogLine:        "Jun 18 00:07:26 p487-2-am.jethelix.ru dumbproxy[82403]: PROXY   : 2024/06/18 00:07:26 handler.go:138: INFO     Request: 143.178.228.182:64154 => 2.56.204.64:443 \"andre487\" HTTP/1.1 GET http://ifconfig.co/",
		LogTime:        time.Date(2024, 6, 18, 0, 7, 26, 0, time.Local),
		Host:           "p487-2-am.jethelix.ru",
		Pid:            82403,
		HasRequestInfo: true,
		FileName:       "handler.go",
		FileLine:       138,
		SrcIp:          "143.178.228.182",
		DestIp:         "2.56.204.64",
		DestPort:       443,
		Username:       "andre487",
		Proto:          "HTTP/1.1",
		Method:         "GET",
		Url:            "http://ifconfig.co/",
	}, res)

	res, err = ParseLogLine(logLineHttpInfo)
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, &LogLineData{
		LogLineType:    LogLineTypeProxyRequestHttpInfo,
		LogLine:        "Jun 21 13:00:47 p487-2-am.jethelix.ru dumbproxy[111654]: PROXY   : 2024/06/21 13:00:47 handler.go:106: INFO     143.178.232.21:57190 POST http://e5.o.lencr.org/ 200 OK",
		LogTime:        time.Date(2024, 6, 21, 13, 0, 47, 0, time.Local),
		Host:           "p487-2-am.jethelix.ru",
		Pid:            111654,
		HasRequestInfo: true,
		FileName:       "handler.go",
		FileLine:       106,
		SrcIp:          "143.178.232.21",
		Method:         "POST",
		Url:            "http://e5.o.lencr.org/",
		Status:         200,
	}, res)

	res, err = ParseLogLine(logLineReqError)
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, &LogLineData{
		LogLineType:  LogLineTypeProxyRequestError,
		IsError:      true,
		LogLine:      "Jun 18 00:42:21 p487-2-am.jethelix.ru dumbproxy[90996]: PROXY   : 2024/06/18 00:42:21 handler.go:51: ERROR    Can't satisfy CONNECT request: dial tcp [2a02:6b8::5d7]:443: connect: network is unreachable",
		LogTime:      time.Date(2024, 6, 18, 0, 42, 21, 0, time.Local),
		FileName:     "handler.go",
		FileLine:     51,
		Host:         "p487-2-am.jethelix.ru",
		Pid:          90996,
		ErrorMessage: "Can't satisfy CONNECT request: dial tcp [2a02:6b8::5d7]:443: connect: network is unreachable",
	}, res)

	res, err = ParseLogLine(logLineHttpSrvError)
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, &LogLineData{
		LogLineType:  LogLineTypeHttpSrvError,
		IsError:      true,
		LogLine:      "Jun 21 13:00:18 p487-2-am.jethelix.ru dumbproxy[111654]: HTTPSRV : 2024/06/21 13:00:18 server.go:3195: http: TLS handshake error from 143.178.232.21:57019: EOF",
		LogTime:      time.Date(2024, 6, 21, 13, 0, 18, 0, time.Local),
		Host:         "p487-2-am.jethelix.ru",
		Pid:          111654,
		FileName:     "server.go",
		FileLine:     3195,
		ErrorMessage: "http: TLS handshake error from 143.178.232.21:57019: EOF",
	}, res)
}

func TestBigLog2(t *testing.T) {
	logText := readFileToString("test/data/dumbproxy-big.log")
	for _, logLine := range strings.Split(logText, "\n") {
		res, err := ParseLogLine(logLine)
		if err != nil || res == nil {
			t.Fatalf("error %s: unable to parse log line: %s", err, logLine)
		}
		if res.LogLineType == LogLineTypeUnmatched || res.LogLineType == LogLineTypeProxyUnknown {
			t.Fatalf("wrond line tyoe %v for line %s", res.LogLineType, logLine)
		}
		if res.LogLineType.String() == "LogLineType__UNKNOWN" {
			t.Fatalf("unknown log line tyoe %v for line %s", res.LogLineType, logLine)
		}
	}
}

func readFileToString(filePath string) string {
	logLineGeneral, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Read file %s error: %v", filePath, err)
	}
	return strings.TrimSpace(string(logLineGeneral))
}
