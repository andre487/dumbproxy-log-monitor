package main

import (
	log "github.com/sirupsen/logrus"
)

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
	err := close()
	if err != nil {
		log.Warnf("Error when closing: %s", err)
	}
}
