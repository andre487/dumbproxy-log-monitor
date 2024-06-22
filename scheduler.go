package main

import (
	"fmt"
	"log"
	"time"
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
	job := SchedulerJobDescription{TaskName: jobEt.TaskName, Task: jobEt.Task}

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
		return err
	}
	lastExecTime := time.Unix(int64(lastExecTs), 0).UTC()

	if needExecTime.Sub(lastExecTime) > interval {
		newLastExecTime := (needExecDuration - interval) / time.Second
		if err := t.db.SetKvRecord(lastExecKey, newLastExecTime); err != nil {
			return err
		}
	}

	t.jobs[job.TaskName] = job
	return nil
}

func (t *Scheduler) Run() {
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
		log.Printf("WARN Can't get last exec time for %s: %s\n", lastExecKey, err)
		return
	}

	lastExecDuration := time.Duration(lastExecTime) * time.Second
	if nowDuration-lastExecDuration > job.Interval {
		log.Printf("INFO Executing %s\n", taskName)
		if err := job.Task(); err != nil {
			log.Printf("WARN Can't exec task %s: %s", lastExecKey, err)
		}
		if err := t.db.SetKvRecord(lastExecKey, nowDuration/time.Second); err != nil {
			log.Printf("WARN Can't update last exec time for task %s: %s\n", lastExecKey, err)
		}
	}
}

func createTaskLastExecTimeKey(taskName string) string {
	return fmt.Sprintf("scheduler:%s:last_exec_time", taskName)
}

func (t *Scheduler) Stop() {
	t.running = false
}
