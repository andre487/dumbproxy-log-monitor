# DumbProxy log monitor

A simple application for creating a report over [dumbproxy](https://github.com/SenseUnit/dumbproxy)
logs and sending it to an email.

## Usage

```
Usage of ./dumbproxy-log-monitor:
  -dbPath string
    	DB path (default "/tmp/dumbproxy-log-monitor-test.db")
  -logCmd string
    	CMD for logs (default "sudo journald -fu dumbproxy.service")
  -logCmdDir string
    	CWD for log CMD (default ".")
  -mailerConfig string
    	Config for mailer (default "secrets/mailer.json")
  -printReport
    	Print report to STDOUT
  -reportMail string
    	Email to send reports
  -reportTime string
    	Report UTC time in format 22:00:00 (default "22:00:00")
```

## Configs

### secrets/mailer.json

```json
{
  "host": "smtp.example.com",
  "port": "587",
  "user": "SMTP user",
  "password": "SMTP password",
  "sender": "my-email@example.com"
}
```

## Report example

[Report example](testData/report-example.html)

## Build

Latest [Linux x86_64](https://github.com/andre487/dumbproxy-log-monitor/releases/latest/download/dumbproxy-log-monitor-linux-x86_64.tar.gz)
