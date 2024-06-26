package main

import (
	log "github.com/sirupsen/logrus"
)

func StrDef(val string, def string) string {
	if val == "" {
		return def
	}
	return val
}

func Must0(err error) {
	if err != nil {
		log.Fatalf("ERROR Unexpected error: %s", err)
	}
}

func Must1[T interface{}](arg T, err error) T {
	if err != nil {
		log.Fatalf("ERROR Unexpected error: %s", err)
	}
	return arg
}

func AutoClose(close func() error) {
	if err := close(); err != nil {
		log.Warnf("Error when closing: %s", err)
	}
}

func WarnIfErr(err error) {
	if err != nil {
		log.Warnf("Error occurred: %s", err)
	}
}
