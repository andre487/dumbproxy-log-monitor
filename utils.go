package main

import "log"

func Must0(err error) {
	if err != nil {
		log.Fatalf("ERROR Unexpected error: %s\n", err)
	}
}

func Must1[T interface{}](arg T, err error) T {
	if err != nil {
		log.Fatalf("ERROR Unexpected error: %s\n", err)
	}
	return arg
}
