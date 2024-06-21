package main

import (
	"bufio"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
)

type LogReaderParams struct {
	JournalDUnit        string
	JournalDCommand     string
	ExecDir             string
	ProcessRestartLimit int
}

type LogReader struct {
	LogReaderParams
	stopped bool
	cmd     *exec.Cmd
	stdout  io.ReadCloser
}

func NewLogReader(params LogReaderParams) (*LogReader, error) {
	if params.JournalDUnit == "" {
		params.JournalDUnit = "dumbproxy.service"
	}
	if params.JournalDCommand == "" {
		params.JournalDCommand = "sudo journald -fu " + params.JournalDUnit
	}
	if params.ProcessRestartLimit == 0 {
		params.ProcessRestartLimit = 3
	}

	res := &LogReader{LogReaderParams: params}
	if err := res.launchProcess(); err != nil {
		return nil, err
	}
	return res, nil
}

func (t *LogReader) ReadLogStreamToChannel(ch chan *LogLineData) {
	runNum := 0
	for {
		scanner := bufio.NewScanner(t.stdout)
		for scanner.Scan() {
			data, err := ParseLogLine(scanner.Text())
			if err == nil {
				ch <- data
				runNum = 0
			} else {
				log.Printf("WARN Parse log error: %s", err)
			}
		}
		if err := scanner.Err(); err != nil {
			log.Printf("WARN Scanner close error: %s", err)
		}

		processState, waitErr := t.cmd.Process.Wait()
		if waitErr != nil {
			log.Printf("WARN Wait process error: %s", waitErr)
		}

		runNum++
		if processState.ExitCode() == -1 || runNum >= t.ProcessRestartLimit {
			break
		}

		log.Printf("INFO Waiting for timeout before restarting log reader")
		time.Sleep(2 * time.Second)
		log.Printf("INFO Restarting log reader")

		if err := t.launchProcess(); err != nil {
			log.Printf("ERROR Restart process error: %s", err)
			break
		}
	}
	close(ch)
}

func (t *LogReader) Stop() error {
	t.stopped = true

	var resErr error
	if t.stdout != nil {
		if err := t.stdout.Close(); err != nil {
			resErr = multierror.Append(resErr, err)
		}
	}

	if t.cmd != nil && t.cmd.Process != nil {
		if err := t.cmd.Process.Kill(); err != nil {
			resErr = multierror.Append(resErr, err)
		}

		if _, err := t.cmd.Process.Wait(); err != nil {
			resErr = multierror.Append(resErr, err)
		}
	}

	return resErr
}

func (t *LogReader) IsAlive() bool {
	return !t.stopped
}

func (t *LogReader) launchProcess() error {
	cmdParts := strings.Split(t.JournalDCommand, " ")

	cmd := exec.Command(cmdParts[0], cmdParts[1:]...)
	cmd.Dir = t.ExecDir
	cmd.Env = append(os.Environ(), "IN_LOG_READER=1")

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	t.cmd = cmd
	t.stdout = stdout

	if err := cmd.Start(); err != nil {
		return err
	}

	return nil
}
