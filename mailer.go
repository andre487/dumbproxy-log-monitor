package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"

	"gopkg.in/gomail.v2"
)

type Mailer struct {
	dialer *gomail.Dialer
	sender string
}

type mailerConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Sender   string
}

func (t *mailerConfig) GetIntPort() int {
	return Must1(strconv.Atoi(t.Port))
}

func NewMailer(configPath string) (*Mailer, error) {
	confFp, err := os.OpenFile(configPath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("unable to open mailer config file: %s", err)
	}
	defer CloseOrWarn(confFp)

	confContent, err := io.ReadAll(confFp)
	if err != nil {
		return nil, fmt.Errorf("unable to read mailer config file: %s", err)
	}

	var config mailerConfig
	err = json.Unmarshal(confContent, &config)
	if err != nil {
		return nil, fmt.Errorf("unable to parse mailer config: %s", err)
	}

	res := &Mailer{}
	res.dialer = gomail.NewDialer(config.Host, config.GetIntPort(), config.User, config.Password)
	res.sender = config.Sender

	return res, nil
}

func (t *Mailer) SendMessage(to string, subject string, message string) error {
	m := gomail.NewMessage()
	m.SetHeader("From", t.sender)
	m.SetHeader("To", to)
	m.SetHeader("Subject", subject)
	m.SetBody("text/html", message)
	if err := t.dialer.DialAndSend(m); err != nil {
		return fmt.Errorf("unable to send message: %s", err)
	}
	return nil
}
