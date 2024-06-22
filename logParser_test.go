package main

import (
	"log"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
	assert.ErrorIs(t, ErrorLogLineNotMatch, err)

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
	assert.ErrorIs(t, ErrorLogLineNotMatch, err)

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
	assert.ErrorIs(t, ErrorLogLineNotMatch, err)

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
