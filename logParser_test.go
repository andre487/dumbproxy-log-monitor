package main

import (
	"net"
	"os"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestParseSystemDLogLine(t *testing.T) {
	logLine1 := readFileToString("testData/log-line-general.txt")
	logLine2 := readFileToString("testData/log-line-request.txt")
	logLine3 := readFileToString("testData/log-line-error.txt")
	logLine4 := readFileToString("testData/log-line-cant-dial.txt")

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
	logLine1 := readFileToString("testData/log-line-general.txt")
	logLine2 := readFileToString("testData/log-line-request.txt")
	logLine3 := readFileToString("testData/log-line-error.txt")
	logLine4 := readFileToString("testData/log-line-cant-dial.txt")

	var rec *DumbProxyLogLineRecord
	var err error
	now := time.Now()

	rec, err = ParseDumbProxyLogLine(logLine1)
	assert.NoError(t, err)
	assert.Equal(t, &DumbProxyLogLineRecord{
		dumbProxyLogLineRecord: dumbProxyLogLineRecord{
			Year:      2024,
			Month:     time.June,
			Day:       21,
			Hour:      13,
			Minute:    0,
			Sec:       47,
			LogTime:   time.Date(2024, time.June, 21, 13, 0, 47, 0, time.Local),
			Logger:    "PROXY",
			FileName:  "handler.go",
			Line:      106,
			LevelName: "INFO",
			LogRecord: "143.178.232.21:57190 POST http://e5.o.lencr.org/ 200 OK",
		},
		SystemDData: &SystemDLogLineRecord{
			Month:     "Jun",
			Day:       21,
			Hour:      13,
			Minute:    0,
			Sec:       47,
			LogTime:   time.Date(now.Year(), time.June, 21, 13, 0, 47, 0, time.Local),
			Host:      "p487-2-am.jethelix.ru",
			Unit:      "dumbproxy",
			Pid:       111654,
			LogRecord: "PROXY   : 2024/06/21 13:00:47 handler.go:106: INFO     143.178.232.21:57190 POST http://e5.o.lencr.org/ 200 OK",
		},
	}, rec)

	rec, err = ParseDumbProxyLogLine(logLine2)
	assert.NoError(t, err)
	assert.Equal(t, &DumbProxyLogLineRecord{
		dumbProxyLogLineRecord: dumbProxyLogLineRecord{
			Year:      2024,
			Month:     time.June,
			Day:       18,
			Hour:      0,
			Minute:    7,
			Sec:       26,
			LogTime:   time.Date(2024, time.June, 18, 0, 7, 26, 0, time.Local),
			Logger:    "PROXY",
			FileName:  "handler.go",
			Line:      138,
			LevelName: "INFO",
			LogRecord: "Request: 143.178.228.182:64154 => 2.56.204.64:443 \"andre487\" HTTP/1.1 GET http://ifconfig.co/",
		},
		SystemDData: &SystemDLogLineRecord{
			Month:     "Jun",
			Day:       18,
			Hour:      0,
			Minute:    7,
			Sec:       26,
			LogTime:   time.Date(now.Year(), time.June, 18, 0, 7, 26, 0, time.Local),
			Host:      "p487-2-am.jethelix.ru",
			Unit:      "dumbproxy",
			Pid:       82403,
			LogRecord: "PROXY   : 2024/06/18 00:07:26 handler.go:138: INFO     Request: 143.178.228.182:64154 => 2.56.204.64:443 \"andre487\" HTTP/1.1 GET http://ifconfig.co/",
		},
	}, rec)

	rec, err = ParseDumbProxyLogLine(logLine3)
	assert.NoError(t, err)
	assert.Equal(t, &DumbProxyLogLineRecord{
		dumbProxyLogLineRecord: dumbProxyLogLineRecord{
			Year:      2024,
			Month:     time.June,
			Day:       21,
			Hour:      13,
			Minute:    0,
			Sec:       18,
			LogTime:   time.Date(now.Year(), time.June, 21, 13, 0, 18, 0, time.Local),
			Logger:    "HTTPSRV",
			FileName:  "server.go",
			Line:      3195,
			LevelName: "",
			LogRecord: "http: TLS handshake error from 143.178.232.21:57019: EOF",
		},
		SystemDData: &SystemDLogLineRecord{
			Month:     "Jun",
			Day:       21,
			Hour:      13,
			Minute:    0,
			Sec:       18,
			LogTime:   time.Date(now.Year(), time.June, 21, 13, 0, 18, 0, time.Local),
			Host:      "p487-2-am.jethelix.ru",
			Unit:      "dumbproxy",
			Pid:       111654,
			LogRecord: "HTTPSRV : 2024/06/21 13:00:18 server.go:3195: http: TLS handshake error from 143.178.232.21:57019: EOF",
		},
	}, rec)

	rec, err = ParseDumbProxyLogLine(logLine4)
	assert.NoError(t, err)
	assert.Equal(t, &DumbProxyLogLineRecord{
		dumbProxyLogLineRecord: dumbProxyLogLineRecord{
			Year:      2024,
			Month:     time.June,
			Day:       18,
			Hour:      11,
			Minute:    18,
			Sec:       52,
			LogTime:   time.Date(now.Year(), time.June, 18, 11, 18, 52, 0, time.Local),
			Logger:    "PROXY",
			FileName:  "handler.go",
			Line:      51,
			LevelName: "ERROR",
			LogRecord: "Can't satisfy CONNECT request: dial tcp [2a02:6b8::5d7]:443: connect: network is unreachable",
		},
		SystemDData: &SystemDLogLineRecord{
			Month:     "Jun",
			Day:       18,
			Hour:      11,
			Minute:    18,
			Sec:       52,
			LogTime:   time.Date(now.Year(), time.June, 18, 11, 18, 52, 0, time.Local),
			Host:      "p487-2-am.jethelix.ru",
			Unit:      "dumbproxy",
			Pid:       96234,
			LogRecord: "PROXY   : 2024/06/18 11:18:52 handler.go:51: ERROR    Can't satisfy CONNECT request: dial tcp [2a02:6b8::5d7]:443: connect: network is unreachable",
		},
	}, rec)

	rec, err = ParseDumbProxyLogLine("Jun 18 00:07:26 p487-2-am.jethelix.ru dumbproxy[82403]: FOO")
	assert.ErrorContains(t, err, "dumbproxy log parse error")
	assert.Nil(t, rec)
}

func TestBigLog(t *testing.T) {
	logText := readFileToString("testData/dumbproxy-big.log")
	for _, logLine := range strings.Split(logText, "\n") {
		res, err := ParseLogLine(logLine)
		if err != nil || res == nil {
			t.Errorf("error %s: unable to parse log line: %s", err, logLine)
		}
	}
}

func TestParseLogLineGeneral(t *testing.T) {
	logLine := readFileToString("testData/log-line-general.txt")
	res, err := ParseLogLineGeneral(logLine)
	if err != nil {
		t.Errorf("parse error: %v", err.Error())
	}
	assert.Equal(
		t,
		LogLineData{
			LogLineType: LogLineTypeGeneral,
			DateTime:    time.Date(2024, 6, 21, 13, 0, 47, 0, time.UTC),
			LoggerName:  "PROXY",
			Level:       "INFO",
			SrcIp:       net.IPv4(143, 178, 232, 21),
		},
		*res,
	)

	res, err = ParseLogLineGeneral("")
	assert.ErrorIs(t, err, ErrorLogLineNotMatch)

	res, err = ParseLogLineGeneral(strings.Replace(logLine, "143.178.232.21", "328.1.1.1", 1))
	assert.ErrorIs(t, err, ErrorParse)
	assert.Contains(t, err.Error(), "wrong IP")
}

func TestParseLogLineRequest(t *testing.T) {
	logLine := readFileToString("testData/log-line-request.txt")
	res, err := ParseLogLineRequest(logLine)
	if err != nil {
		t.Errorf("parse error: %v", err.Error())
	}
	assert.Equal(
		t,
		LogLineData{
			LogLineType: LogLineTypeRequest,
			DateTime:    time.Date(2024, 6, 18, 0, 7, 26, 0, time.UTC),
			LoggerName:  "PROXY",
			Level:       "INFO",
			SrcIp:       net.IPv4(143, 178, 228, 182),
			DestIp:      net.IPv4(2, 56, 204, 64),
			DestHost:    "ifconfig.co",
			User:        "andre487",
		},
		*res,
	)

	res, err = ParseLogLineRequest("")
	assert.ErrorIs(t, err, ErrorLogLineNotMatch)

	res, err = ParseLogLineRequest(strings.Replace(logLine, "143.178.228.182", "328.1.1.1", 1))
	assert.ErrorIs(t, err, ErrorParse)
	assert.Contains(t, err.Error(), "wrong IP")
	assert.Contains(t, err.Error(), "src IP")

	res, err = ParseLogLineRequest(strings.Replace(logLine, "2.56.204.64", "328.1.1.1", 1))
	assert.ErrorIs(t, err, ErrorParse)
	assert.Contains(t, err.Error(), "wrong IP")
	assert.Contains(t, err.Error(), "dest IP")
}

func TestParseLogLineError(t *testing.T) {
	logLine := readFileToString("testData/log-line-error.txt")
	res, err := ParseLogLineError(logLine)
	if err != nil {
		t.Errorf("parse error: %v", err.Error())
	}
	assert.Equal(
		t,
		LogLineData{
			LogLineType: LogLineTypeError,
			DateTime:    time.Date(2024, 6, 21, 13, 0, 18, 0, time.UTC),
			LoggerName:  "HTTPSRV",
			SrcIp:       net.IPv4(143, 178, 232, 21),
		},
		*res,
	)

	res, err = ParseLogLineError("")
	assert.ErrorIs(t, err, ErrorLogLineNotMatch)

	res, err = ParseLogLineError(strings.Replace(logLine, "143.178.232.21", "328.1.1.1", 1))
	assert.ErrorIs(t, err, ErrorParse)
	assert.Contains(t, err.Error(), "wrong IP")
}

func TestParseLogLine(t *testing.T) {
	logLineGeneral := readFileToString("testData/log-line-general.txt")
	logLineRequest := readFileToString("testData/log-line-request.txt")
	logLineError := readFileToString("testData/log-line-error.txt")

	res, err := ParseLogLine(logLineGeneral)
	assert.Nil(t, err)
	assert.Equal(t, res.LogLineType, LogLineTypeGeneral)

	res, err = ParseLogLine(logLineRequest)
	assert.Nil(t, err)
	assert.Equal(t, res.LogLineType, LogLineTypeRequest)

	res, err = ParseLogLine(logLineError)
	assert.Nil(t, err)
	assert.Equal(t, res.LogLineType, LogLineTypeError)

	res, err = ParseLogLine("")
	assert.Nil(t, res)
	assert.ErrorIs(t, err, ErrorLogLineNotMatch)
}

func TestParseLogLineCantDial(t *testing.T) {
	logLine := readFileToString("testData/log-line-cant-dial.txt")
	res, err := ParseLogLine(logLine)
	if err != nil {
		t.Errorf("parse error: %v", err.Error())
	}
	assert.Equal(
		t,
		LogLineData{
			LogLineType: LogLineTypeCantDialError,
			DateTime:    time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
			DestHost:    "[2a02:6b8::5d7]",
		},
		*res,
	)
}

func readFileToString(filePath string) string {
	logLineGeneral, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Read file %s error: %v", filePath, err)
	}
	return strings.TrimSpace(string(logLineGeneral))
}
