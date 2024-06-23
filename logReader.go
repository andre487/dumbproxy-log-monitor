package main

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	log "github.com/sirupsen/logrus"
)

type LogReaderParams struct {
	JournalDCommand     string
	ExecDir             string
	ProcessRestartLimit int
	lastHandledTime     time.Time
}

type LogReader struct {
	LogReaderParams
	running bool
	cmd     *exec.Cmd
	stdout  io.ReadCloser
}

func NewLogReader(params LogReaderParams) (*LogReader, error) {
	if params.JournalDCommand == "" {
		params.JournalDCommand = "sudo journald -fu dumbproxy.service"
	}
	if params.ProcessRestartLimit == 0 {
		params.ProcessRestartLimit = 3
	}

	res := &LogReader{LogReaderParams: params, running: true}
	if err := res.launchProcess(); err != nil {
		return nil, err
	}
	return res, nil
}

func (t *LogReader) ReadLogStreamToChannel(logCh chan *LogLineData, wg *sync.WaitGroup) {
	defer log.Infoln("ReadLogStreamToChannel is finished")
	defer close(logCh)
	defer wg.Done()

	runNum := 0
	for t.running {
		scanner := bufio.NewScanner(t.stdout)
		for t.running && scanner.Scan() {
			data, err := ParseLogLine(scanner.Text())
			if err == nil {
				logCh <- data
				runNum = 0
				t.lastHandledTime = time.Now().UTC()
			} else {
				log.Warnf("Parse log error: %s", err)
			}
		}

		if !t.running {
			return
		}

		if err := scanner.Err(); err != nil {
			log.Warnf("Scanner close error: %s", err)
		}

		_, waitErr := t.cmd.Process.Wait()
		if waitErr != nil {
			log.Warnf("Wait process error: %s", waitErr)
		}

		runNum++
		if runNum >= t.ProcessRestartLimit {
			break
		}

		log.Infoln("Waiting for timeout before restarting log reader")
		time.Sleep(2 * time.Second)
		log.Infoln("Restarting log reader")

		if err := t.launchProcess(); err != nil {
			log.Errorf("Restart process error: %s", err)
			break
		}
	}
}

func (t *LogReader) Stop() {
	t.running = false

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

	if resErr != nil {
		log.Warnf("Stop reader error: %s", resErr)
	}
}

func (t *LogReader) IsAlive() bool {
	return t.running
}

func (t *LogReader) launchProcess() error {
	cmdParts := strings.Split(t.JournalDCommand, " ")
	cmdParts = append(cmdParts, "--since", t.lastHandledTime.Format(time.RFC3339))
	log.Infof("Launching log process: %v", cmdParts)

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
