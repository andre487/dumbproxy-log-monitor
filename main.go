package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
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
	origLogLevel := setupLogger()

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

	createReport := func() error {
		report, err := reporter.GenerateReport()
		if err != nil {
			return fmt.Errorf("enable to generate report: %s", err)
		}

		if mailer != nil {
			hostname, err := os.Hostname()
			if err != nil {
				log.Warnf("Unable to get hostname: %s", err)
				hostname = "Unknown host"
			}
			subject := hostname + ": Proxy usage report"
			if err := mailer.SendMessage(args.reportMail, subject, report); err != nil {
				return fmt.Errorf("unable to send email: %s", err)
			}
			log.Infof("Report was successfully sent to %s", args.reportMail)
		}

		if args.printReport {
			fmt.Printf("==========\nReport:\n%s\n==========\n", report)
		}

		return nil
	}

	Must0(
		scheduler.ScheduleExactTime(SchedulerJobExactTimeDescription{
			TaskName: "CreateReport",
			Hour:     args.reportHour,
			Minute:   args.reportMinute,
			Second:   args.reportSecond,
			Task:     createReport,
		}),
	)

	scheduler.Schedule(SchedulerJobDescription{
		TaskName: "VacuumCleanLogRecords",
		Interval: 1 * time.Hour,
		Task: func() error {
			recsDeleted, err := db.LogRecordsVacuumClean(48 * time.Hour)
			log.Infof("Vacuum clean records deleted: %d", recsDeleted)
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
	usrSignalChan := make(chan os.Signal)
	signal.Notify(stopSignalChan, syscall.SIGTERM, syscall.SIGINT)
	signal.Notify(usrSignalChan, syscall.SIGUSR1, syscall.SIGUSR2)

	go func() {
		sig := <-stopSignalChan
		log.Infof("Stopping on signal %v", sig)
		close(usrSignalChan)
		reader.Stop()
		scheduler.Stop()
	}()

	go func() {
		for sig := range usrSignalChan {
			switch sig {
			case syscall.SIGUSR1:
				log.Infof("Sending report on signal %v", sig)
				if err := createReport(); err != nil {
					log.Warnf("Error when creating report on signal: %s", err)
				}
				break
			case syscall.SIGUSR2:
				if log.GetLevel() == log.TraceLevel {
					log.Infof("Disabling tracing by signal %v", sig)
					log.SetLevel(origLogLevel)
				} else {
					log.Infof("Enabling tracing by signal %v", sig)
					log.SetLevel(log.TraceLevel)
				}
				break
			}
		}
	}()

	workersGroup.Wait()
	log.Info("Work is finished")
}

func setupLogger() log.Level {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:          true,
		TimestampFormat:        "2006-01-02 15:04:05",
		DisableLevelTruncation: true,
	})

	var level log.Level
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "trace":
		level = log.TraceLevel
		break
	case "debug":
		level = log.DebugLevel
		break
	case "info":
		level = log.InfoLevel
		break
	case "warn":
		level = log.WarnLevel
		break
	case "error":
		level = log.ErrorLevel
		break
	case "fatal":
		level = log.FatalLevel
		break
	case "panic":
		level = log.PanicLevel
		break
	default:
		level = log.InfoLevel
	}
	log.SetLevel(level)

	return level
}

func getArgs() cliArgs {
	var args cliArgs
	flag.StringVar(&args.dbPath, "dbPath", "/tmp/dumbproxy-log-monitor-test.db", "DB path")
	flag.StringVar(&args.logCmd, "logCmd", "sudo journalctl -fu dumbproxy.service", "CMD for logs")
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
			log.Fatalf("Could not create DB dir: %v", err)
		}
	}

	if args.logCmd == "" {
		log.Fatalln("-log-cmd is required")
	}

	matches := Must1(regexp.Compile("^(-?\\d{1,2}):(-?\\d{1,2}):(-?\\d{1,2})$")).FindStringSubmatch(args.reportTime)
	if len(matches) != 4 {
		log.Fatalf("Invalid value for -report-time: %s", args.reportTime)
	}
	args.reportHour = Must1(strconv.Atoi(matches[1]))
	args.reportMinute = Must1(strconv.Atoi(matches[2]))
	args.reportSecond = Must1(strconv.Atoi(matches[3]))

	if _, err := os.Stat(args.mailerConfigPath); err != nil {
		log.Fatalf("Unable to read -mailer-config: %s", err)
	}
}
