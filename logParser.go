package main

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"time"

	"github.com/hashicorp/go-multierror"
)

var logLineGeneralRe = regexp.MustCompile(".+dumbproxy\\[\\d+]: (?P<logger>[\\w-]+)\\s+: (?P<year>[0-9]+)/(?P<month>[0-9]+)/(?P<day>[0-9]+) (?P<hour>[0-9]+):(?P<min>[0-9]+):(?P<sec>[0-9]+) [\\w.-]+:\\d+: (?P<level>[A-Z]+)\\s+(?P<ip>\\S+):\\d+ [A-Z]+")
var logLineRequestRe = regexp.MustCompile(".+dumbproxy\\[\\d+]: (?P<logger>[\\w-]+)\\s+: (?P<year>[0-9]+)/(?P<month>[0-9]+)/(?P<day>[0-9]+) (?P<hour>[0-9]+):(?P<min>[0-9]+):(?P<sec>[0-9]+) [\\w.-]+:\\d+: (?P<level>[A-Z]+)\\s+Request:\\s+(?P<ip>\\S+):\\d+ => (?P<dest>\\S+?)(?::\\d+)? \"(?P<user>[\\w-]+)\"\\s")
var logLineErrorRe = regexp.MustCompile(".+dumbproxy\\[\\d+]: (?P<logger>[\\w-]+)\\s+: (?P<year>[0-9]+)/(?P<month>[0-9]+)/(?P<day>[0-9]+) (?P<hour>[0-9]+):(?P<min>[0-9]+):(?P<sec>[0-9]+) .+error from (?P<ip>\\S+):\\d+")

type LogLineType uint64

const (
	LogLineTypeGeneral LogLineType = iota
	LogLineTypeRequest
	LogLineTypeError
)

type LogLineData struct {
	LogLineType LogLineType
	DateTime    time.Time
	LoggerName  string
	Level       string
	SrcIp       net.IP
	DestIp      net.IP
	User        string
}

var ErrorLogLineNotMatch = errors.New("log line doesn't match")
var ErrorParse = errors.New("parse error")

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

	return nil, ErrorLogLineNotMatch
}

func ParseLogLineGeneral(logLine string) (*LogLineData, error) {
	resMap := parseLogLineWithRe(logLineGeneralRe, logLine)
	if resMap == nil {
		return nil, ErrorLogLineNotMatch
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
		return nil, ErrorLogLineNotMatch
	}

	var err *multierror.Error
	dt, err := parseDateTime(resMap, err)
	err = validateLogEntries(resMap, err)
	err = validateUser(resMap, err)

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
	}
	return &res, nil
}

func ParseLogLineError(logLine string) (*LogLineData, error) {
	resMap := parseLogLineWithRe(logLineErrorRe, logLine)
	if resMap == nil {
		return nil, ErrorLogLineNotMatch
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

func validateUser(resMap map[string]string, err *multierror.Error) *multierror.Error {
	if resMap["user"] == "" {
		err = multierror.Append(err, errors.Join(ErrorParse, errors.New("empty logger")))
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
