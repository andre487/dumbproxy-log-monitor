package main

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

type DnsResolver struct {
	db *LogDb
}

func NewDnsResolver(db *LogDb) (*DnsResolver, error) {
	return &DnsResolver{db: db}, nil
}

func (t *DnsResolver) ResolveDomain(ipAddr string) (string, error) {
	if ipAddr == "<empty>" {
		return "<empty>", nil
	}

	return t.db.GetCached("DnsResolver:ResolveDomain:"+ipAddr, 24*time.Hour, func() (string, error) {
		finalName := fmt.Sprintf("<Unresolved: %s>", ipAddr)

		vals, err := net.LookupAddr(ipAddr)
		if err != nil {
			if strings.Contains(err.Error(), "no such host") {
				return finalName, nil
			}
			return finalName, errors.Join(errors.New("unable to resolve domain"), err)
		}

		if len(vals) == 0 {
			return finalName, nil
		}

		finalName = vals[0]
		for idx, val := range vals {
			if idx > 0 && len(val) < len(finalName) {
				finalName = val
			}
		}
		return finalName, nil
	})
}
