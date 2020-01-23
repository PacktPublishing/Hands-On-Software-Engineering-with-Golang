package partition

import (
	"net"
	"os"

	"golang.org/x/xerrors"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(DetectorTestSuite))

type DetectorTestSuite struct {
}

func (s *DetectorTestSuite) SetUpTest(c *gc.C) {
	getHostname = os.Hostname
	lookupSRV = net.LookupSRV
}

func (s *DetectorTestSuite) TearDownTest(c *gc.C) {
	getHostname = os.Hostname
	lookupSRV = net.LookupSRV
}

func (s *DetectorTestSuite) TestDetectFromSRVRecords(c *gc.C) {
	getHostname = func() (string, error) {
		return "web-1", nil
	}
	lookupSRV = func(service, proto, name string) (string, []*net.SRV, error) {
		c.Assert(service, gc.Equals, "")
		c.Assert(proto, gc.Equals, "")
		c.Assert(name, gc.Equals, "web-service")
		return "web-service", make([]*net.SRV, 4), nil
	}

	det := DetectFromSRVRecords("web-service")
	curPart, numPart, err := det.PartitionInfo()
	c.Assert(err, gc.IsNil)
	c.Assert(curPart, gc.Equals, 1)
	c.Assert(numPart, gc.Equals, 4)
}

func (s *DetectorTestSuite) TestDetectFromSRVRecordsWithNoDataAvailable(c *gc.C) {
	getHostname = func() (string, error) {
		return "web-1", nil
	}
	lookupSRV = func(service, proto, name string) (string, []*net.SRV, error) {
		return "", nil, xerrors.Errorf("host not found")
	}

	det := DetectFromSRVRecords("web-service")
	_, _, err := det.PartitionInfo()
	c.Assert(xerrors.Is(err, ErrNoPartitionDataAvailableYet), gc.Equals, true)
}

func (s *DetectorTestSuite) TestFixedDetector(c *gc.C) {
	det := Fixed{Partition: 1, NumPartitions: 4}

	curPart, numPart, err := det.PartitionInfo()
	c.Assert(err, gc.IsNil)
	c.Assert(curPart, gc.Equals, 1)
	c.Assert(numPart, gc.Equals, 4)
}
