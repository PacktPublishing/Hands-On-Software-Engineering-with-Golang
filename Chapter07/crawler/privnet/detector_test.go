package privnet_test

import (
	"testing"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter07/crawler/privnet"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(DetectorTestSuite))

type DetectorTestSuite struct{}

func Test(t *testing.T) { gc.TestingT(t) }

func (s *DetectorTestSuite) TestIPV4(c *gc.C) {
	specs := []struct {
		descr string
		input string
		exp   bool
	}{
		{
			descr: "loopback address",
			input: "127.0.0.1",
			exp:   true,
		},
		{
			descr: "private address (10.x.x.x)",
			input: "10.0.0.128",
			exp:   true,
		},
		{
			descr: "private address (192.x.x.x)",
			input: "192.168.0.127",
			exp:   true,
		},
		{
			descr: "private address (172.x.x.x)",
			input: "172.16.10.10",
			exp:   true,
		},
		{
			descr: "link-local address",
			input: "169.254.169.254",
			exp:   true,
		},
		{
			descr: "non-private address",
			input: "8.8.8.8",
			exp:   false,
		},
	}

	det, err := privnet.NewDetector()
	c.Assert(err, gc.IsNil)
	for specIndex, spec := range specs {
		c.Logf("[spec %d] %s", specIndex, spec.descr)
		isPrivate, err := det.IsPrivate(spec.input)
		c.Assert(err, gc.IsNil)
		c.Assert(isPrivate, gc.Equals, spec.exp)
	}
}

func (s *DetectorTestSuite) TestDetectorWithCustomCIDRs(c *gc.C) {
	det, err := privnet.NewDetectorFromCIDRs("8.8.8.8/16")
	c.Assert(err, gc.IsNil)

	isPrivate, err := det.IsPrivate("8.8.8.8")
	c.Assert(err, gc.IsNil)
	c.Assert(isPrivate, gc.Equals, true)
}
