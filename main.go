package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"regexp"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type cliArgs struct {
	dbPath           string
	logCmd           string
	logCmdDir        string
	reportTime       string
	reportMail       string
	mailerConfigPath string

	reportHour   int
	reportMinute int
	reportSecond int
	printReport  bool
}

func main() {
	args := getArgs()
	handleArgs(&args)

	var mailer *Mailer
	if args.reportMail != "" {
		mailer = Must1(NewMailer(args.mailerConfigPath))
	}

	db := Must1(NewLogDb(args.dbPath))
	defer db.Close()

	reader := Must1(NewLogReader(LogReaderParams{
		JournalDCommand: args.logCmd,
		ExecDir:         args.logCmdDir,
	}))
	scheduler := Must1(NewScheduler(db, 1*time.Second))
	reporter := Must1(NewLogReporter(db))

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

				if mailer != nil {
					if err := mailer.SendMessage(args.reportMail, "Proxy usage report", report); err != nil {
						return err
					}
					log.Printf("INFO Report was successfully sent to %s\n", args.reportMail)
				}

				if args.printReport {
					fmt.Printf("==========\nReport:\n%s\n==========\n", report)
				}

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

	var workersGroup sync.WaitGroup
	logChan := make(chan *LogLineData)
	go reader.ReadLogStreamToChannel(logChan, &workersGroup)
	go db.WriteRecordsFromChannel(logChan, &workersGroup)
	go scheduler.Run(&workersGroup)
	workersGroup.Add(3)

	stopSignalChan := make(chan os.Signal, 1)
	signal.Notify(stopSignalChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-stopSignalChan
		log.Printf("INFO Stopping on signal %v\n", sig)
		reader.Stop()
		scheduler.Stop()
	}()

	workersGroup.Wait()
	log.Println("INFO Work is finished")
}

func getArgs() cliArgs {
	var args cliArgs
	flag.StringVar(&args.dbPath, "dbPath", "/tmp/dumbproxy-log-monitor-test.db", "DB path")
	flag.StringVar(&args.logCmd, "logCmd", "sudo journald -fu dumbproxy.service", "CMD for logs")
	flag.StringVar(&args.logCmdDir, "logCmdDir", ".", "CWD for log CMD")
	flag.StringVar(&args.reportTime, "reportTime", "22:00:00", "Report UTC time in format 22:00:00")
	flag.StringVar(&args.reportMail, "reportMail", "", "Email to send reports")
	flag.StringVar(&args.mailerConfigPath, "mailerConfig", "secrets/mailer.json", "Config for mailer")
	flag.BoolVar(&args.printReport, "printReport", false, "Print report to STDOUT")
	flag.Parse()
	return args
}

func handleArgs(args *cliArgs) {
	if args.dbPath == "" {
		log.Fatalln("-db-path is required")
	}
	dbDir := path.Dir(args.dbPath)
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

	if _, err := os.Stat(args.mailerConfigPath); err != nil {
		log.Fatalf("ERROR: Unable to read -mailer-config: %s\n", err)
	}
}
