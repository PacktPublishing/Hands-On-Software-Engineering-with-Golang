package pinger_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter04/pinger"
	"gopkg.in/check.v1"
)

var _ = check.Suite(new(PingerSuite))

type PingerSuite struct {
	origPath string
}

func (s *PingerSuite) SetUpTest(c *check.C) {
	s.origPath = os.Getenv("PATH")
}

func (s *PingerSuite) TearDownTest(c *check.C) {
	_ = os.Setenv("PATH", s.origPath)
}

func (s *PingerSuite) TestFakePing(c *check.C) {
	mock := "32 bytes from 127.0.0.1: icmp_seq=0 ttl=32 time=42000 ms"
	mockCmdOutput(c, "ping", mock, 0)

	got, err := pinger.RoundtripTime("127.0.0.1")
	c.Assert(err, check.IsNil)
	c.Assert(got, check.Equals, 42*time.Second)
}

func (s *PingerSuite) TestRealPing(c *check.C) {
	got, err := pinger.RoundtripTime("127.0.0.1")
	c.Assert(err, check.IsNil)

	if got > 500*time.Millisecond {
		c.Fatalf("ping to 127.0.0.1 took way longer than expected: %s", got)
	}
	c.Logf("ping(127.0.0.1) RTT: %s", got)
}

var (
	unixTemplate = `#!/bin/bash 
cat <<!!!EOF!!! | perl -pe 'chomp if eof'
%s
!!!EOF!!!
exit %d
`

	winTemplate = `@echo off
type %s
exit /B %d
`
)

func mockCmdOutput(c *check.C, cmdName, output string, exitCode int) {
	tmpDir := c.MkDir()
	pathToFakeBin := filepath.Join(tmpDir, cmdName)

	var template string
	switch runtime.GOOS {
	case "windows":
		pathToFakeBin += ".bat"
		template = winTemplate
		outFile := filepath.Join(tmpDir, "output.txt")
		err := ioutil.WriteFile(outFile, []byte(output), os.ModePerm)
		c.Assert(err, check.IsNil)

		output = outFile
	default:
		template = unixTemplate
	}

	fakeBin, err := os.OpenFile(
		pathToFakeBin,
		os.O_RDWR|os.O_CREATE|os.O_TRUNC,
		0777,
	)
	c.Assert(err, check.IsNil)

	_, err = fmt.Fprintf(fakeBin, template, output, exitCode)
	c.Assert(err, check.IsNil)

	err = fakeBin.Close()
	c.Assert(err, check.IsNil)

	newPath := fmt.Sprintf("%s%c%s", tmpDir, os.PathListSeparator, os.Getenv("PATH"))
	err = os.Setenv("PATH", newPath)
	c.Assert(err, check.IsNil)
}

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	check.TestingT(t)
}
