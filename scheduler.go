package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

type Scheduler struct {
	db           *LogDb
	scanInterval time.Duration
	jobs         map[string]SchedulerJobDescription
	running      bool
}

type SchedulerJobDescription struct {
	TaskName string
	Task     func() error
	Interval time.Duration
}

type SchedulerJobExecTimeParams struct {
	Inited bool
	Hour   int
	Minute int
	Second int
}

type SchedulerJobExactTimeDescription struct {
	TaskName string
	Task     func() error
	Hour     int
	Minute   int
	Second   int
}

func NewScheduler(db *LogDb, scanInterval time.Duration) (*Scheduler, error) {
	return &Scheduler{
		db:           db,
		scanInterval: scanInterval,
		jobs:         make(map[string]SchedulerJobDescription),
		running:      true,
	}, nil
}

func (t *Scheduler) Schedule(job SchedulerJobDescription) {
	t.jobs[job.TaskName] = job
}

func (t *Scheduler) ScheduleExactTime(jobEt SchedulerJobExactTimeDescription) error {
	taskName := jobEt.TaskName
	job := SchedulerJobDescription{TaskName: taskName, Task: jobEt.Task}

	execTimeParamsKey := createTaskExecTimeParamsKey(taskName)
	timeParamsStr, err := t.db.GetKvStrRecord(execTimeParamsKey)
	if err != nil {
		return err
	}

	var tp SchedulerJobExecTimeParams
	if timeParamsStr != "" {
		WarnIfErr(json.Unmarshal([]byte(timeParamsStr), &tp))
	}

	changed := false
	if tp.Inited {
		changed = tp.Hour != jobEt.Hour || tp.Minute != jobEt.Minute || tp.Second != jobEt.Second
	}

	now := time.Now().UTC()
	nowDuration := time.Duration(now.Unix()) * time.Second

	interval := time.Hour
	if jobEt.Hour >= 0 {
		interval = 24 * time.Hour
	} else if jobEt.Minute >= 0 {
		interval = 1 * time.Hour
	} else if jobEt.Second >= 0 {
		interval = 1 * time.Minute
	}
	job.Interval = interval

	hourDiff := time.Duration(now.Hour()-max(0, jobEt.Hour)) * time.Hour
	minuteDiff := time.Duration(now.Minute()-max(0, jobEt.Minute)) * time.Minute
	secondDiff := time.Duration(now.Second()-max(0, jobEt.Second)) * time.Second
	needExecDuration := nowDuration - hourDiff - minuteDiff - secondDiff
	needExecTime := time.Unix(int64(needExecDuration/time.Second), 0).UTC()

	lastExecKey := createTaskLastExecTimeKey(job.TaskName)
	lastExecTs, err := t.db.GetKvIntRecord(lastExecKey)
	if err != nil {
		return errors.Join(errors.New("unable to fer last exec time from DB"), err)
	}
	lastExecTime := time.Unix(int64(lastExecTs), 0).UTC()

	if needExecTime.Sub(lastExecTime) > interval || changed {
		newLastExecTime := (needExecDuration - interval) / time.Second
		if err = t.db.SetKvRecord(lastExecKey, newLastExecTime); err != nil {
			return errors.Join(errors.New("unable to set last exec time to DB"), err)
		}
	}

	if changed || !tp.Inited {
		tp.Hour = jobEt.Hour
		tp.Minute = jobEt.Minute
		tp.Second = jobEt.Second
		tp.Inited = true

		tpBytes, err := json.Marshal(tp)
		if err != nil {
			return errors.Join(errors.New("unable to create time params JSON"), err)
		}

		err = t.db.SetKvRecord(execTimeParamsKey, tpBytes)
		if err != nil {
			return errors.Join(errors.New("unable to write time params JSON to DB"), err)
		}
	}

	t.jobs[job.TaskName] = job
	return nil
}

func (t *Scheduler) Run() {
	defer log.Info("Scheduler is finished")
	for t.running {
		time.Sleep(t.scanInterval)
		nowDuration := time.Duration(time.Now().Unix()) * time.Second
		for _, job := range t.jobs {
			t.executeTask(job, nowDuration)
		}
	}
}

func (t *Scheduler) executeTask(job SchedulerJobDescription, nowDuration time.Duration) {
	taskName := job.TaskName

	lastExecKey := createTaskLastExecTimeKey(taskName)
	lastExecTime, err := t.db.GetKvIntRecord(lastExecKey)
	if err != nil {
		log.Warnf("Can't get last exec time for %s: %s", lastExecKey, err)
		return
	}

	lastExecDuration := time.Duration(lastExecTime) * time.Second
	if nowDuration-lastExecDuration > job.Interval {
		log.Infof("Executing %s", taskName)
		if err := job.Task(); err != nil {
			log.Warnf("Can't exec task %s: %s", taskName, err)
		}
		if err := t.db.SetKvRecord(lastExecKey, nowDuration/time.Second); err != nil {
			log.Warnf("Can't update last exec time for task %s: %s", lastExecKey, err)
		}
	}
}

func createTaskLastExecTimeKey(taskName string) string {
	return fmt.Sprintf("scheduler:%s:LastExecTime", taskName)
}

func createTaskExecTimeParamsKey(taskName string) string {
	return fmt.Sprintf("scheduler:%s:ExecTimeParams", taskName)
}

func (t *Scheduler) Stop() {
	t.running = false
}
