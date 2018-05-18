package util

import (
	"net"
	"bytes"
	"syscall"
	"strings"
)

func IsNetworkError(err error) bool {
	if strings.Contains(err.Error(), "use of closed network connection") {
		return true
	}

	if netError, ok := err.(net.Error); ok && netError.Timeout() {
		return true
	}

	switch t := err.(type) {
	case *net.OpError:
		if t.Op == "dial" {
			return true
		} else if t.Op == "read" {
			return true
		}

	case syscall.Errno:
		if t == syscall.ECONNREFUSED {
			return true
		}
	}

	return false
}

func IsPrivateAddress(ip string) bool {
	type IPRange struct {
		Min net.IP
		MAX net.IP
	}

	ranges := []IPRange{
		{Min: net.ParseIP("127.0.0.0"), MAX: net.ParseIP("127.255.255.255")},
		{Min: net.ParseIP("10.0.0.0"), MAX: net.ParseIP("10.255.255.255")},
		{Min: net.ParseIP("172.16.0.0"), MAX: net.ParseIP("172.31.255.255")},
		{Min: net.ParseIP("192.168.0.0"), MAX: net.ParseIP("192.168.255.255")},
	}

	check := func(ip string, ip1, ip2 net.IP) bool {
		trial := net.ParseIP(ip)
		if trial.To4() == nil {
			return false
		}
		if bytes.Compare(trial, ip1) >= 0 && bytes.Compare(trial, ip2) <= 0 {
			return true
		}
		return false
	}

	for _, rang := range ranges {
		if result := check(ip, rang.Min, rang.MAX); result {
			return true
		}
	}

	return false
}
