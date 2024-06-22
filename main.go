package main

import (
	"flag"
	"log"
	"os"
	"path"
	"regexp"
	"strconv"
	"time"
)

type cliArgs struct {
	dbPath     string
	logCmd     string
	logCmdDir  string
	reportTime string

	reportHour   int
	reportMinute int
	reportSecond int
}

func main() {
	args := getArgs()
	handleArgs(&args)

	db, err := NewLogDb(args.dbPath)
	if err != nil {
		log.Fatalf("ERROR Unable to create DB: %s", err)
	}
	defer db.Close()

	reader, err := NewLogReader(LogReaderParams{
		JournalDCommand: args.logCmd,
		ExecDir:         args.logCmdDir,
	})
	if err != nil {
		log.Fatalf("ERROR Unable to create new log reader: %s\n", err)
	}
	defer reader.Stop()

	scheduler, err := NewScheduler(db, 1*time.Second)
	if err != nil {
		log.Fatalf("ERROR Unable to create new log reader: %s\n", err)
	}
	defer scheduler.Stop()

	reporter, err := NewLogReporter(db, 10*time.Minute)
	if err != nil {
		log.Fatalf("ERROR Unable to create new reporter: %s\n", err)
	}

	Must0(
		scheduler.ScheduleExactTime(SchedulerJobExactTimeDescription{
			TaskName: "CreateReport",
			Hour:     args.reportHour,
			Minute:   args.reportMinute,
			Second:   args.reportSecond,
			Task: func() error {
				report, err := reporter.GenerateReport()
				if err != nil {
					return err
				}
				log.Println(report)
				return nil
			},
		}),
	)

	scheduler.Schedule(SchedulerJobDescription{
		TaskName: "VacuumCleanLogRecords",
		Interval: 1 * time.Hour,
		Task: func() error {
			recsDeleted, err := db.LogRecordsVacuumClean(48 * time.Hour)
			log.Printf("INFO Vacuum clean records deleted: %d\n", recsDeleted)
			return err
		},
	})

	ch := make(chan *LogLineData)
	go reader.ReadLogStreamToChannel(ch)
	go db.WriteRecordsFromChannel(ch)
	go scheduler.Run()

	time.Sleep(2 * time.Hour)
	log.Println("Reading finished")
}

func getArgs() cliArgs {
	var args cliArgs
	flag.StringVar(&args.dbPath, "db-path", "/tmp/dumbproxy-log-monitor-test.db", "DB path")
	flag.StringVar(&args.logCmd, "log-cmd", "sudo journald -fu dumbproxy.service", "CMD for logs")
	flag.StringVar(&args.logCmdDir, "log-cmd-dir", ".", "CWD for log CMD")
	flag.StringVar(&args.reportTime, "report-time", "22:00:00", "Report UTC time in format 22:00:00")
	flag.Parse()
	return args
}

func handleArgs(args *cliArgs) {
	if args.dbPath == "" {
		log.Fatalln("-db-path is required")
	}
	dbDir := path.Base(args.dbPath)
	if _, err := os.Stat(dbDir); err != nil && os.IsNotExist(err) {
		if err := os.MkdirAll(dbDir, 0755); err != nil {
			log.Fatalf("Could not create DB dir: %v\n", err)
		}
	}

	if args.logCmd == "" {
		log.Fatalln("-log-cmd is required")
	}

	matches := Must1(regexp.Compile("^(-?\\d{1,2}):(-?\\d{1,2}):(-?\\d{1,2})$")).FindStringSubmatch(args.reportTime)
	if len(matches) != 4 {
		log.Fatalf("Invalid value for -report-time: %s\n", args.reportTime)
	}
	args.reportHour = Must1(strconv.Atoi(matches[1]))
	args.reportMinute = Must1(strconv.Atoi(matches[2]))
	args.reportSecond = Must1(strconv.Atoi(matches[3]))
}
