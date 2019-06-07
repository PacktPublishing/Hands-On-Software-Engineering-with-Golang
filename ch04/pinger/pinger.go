package pinger

import (
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"time"

	"golang.org/x/xerrors"
)

var rttRegex = regexp.MustCompile(`time[=<]([\d.]+)[ ]?ms`)

// RoundtripTime uses the ping command to measure the RTT to host.
func RoundtripTime(host string) (time.Duration, error) {
	var argList = []string{host}
	if runtime.GOOS == "windows" {
		argList = append(argList, "-n", "1", "-l", "32")
	} else {
		argList = append(argList, "-c", "1", "-s", "32")
	}

	out, err := exec.Command("ping", argList...).Output()
	if err != nil {
		return 0, xerrors.Errorf("command execution failed: %w", err)
	}

	return extractRTT(string(out))
}

func extractRTT(res string) (time.Duration, error) {
	matches := rttRegex.FindStringSubmatch(res)
	if len(matches) != 2 {
		return 0, xerrors.Errorf("error parsing ping response: unexpected content")
	}

	rtt, err := strconv.ParseFloat(matches[1], 32)
	if err != nil {
		return 0, xerrors.Errorf("error parsing ping response: %w", err)
	}

	return time.Duration(int64(rtt * 1e6)), nil
}
