package frontend

import (
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(HighlightTestSuite))

type HighlightTestSuite struct {
}

func (s *HighlightTestSuite) TestHighlightSentence(c *gc.C) {
	specs := []struct {
		in  string
		exp string
	}{
		{in: "Test KEYWORD1", exp: "Test <em>KEYWORD1</em>"},
		{in: "Data. KEYWORD2 lorem ipsum.KEYWORD1", exp: "Data. <em>KEYWORD2</em> lorem ipsum.<em>KEYWORD1</em>"},
		{in: "no match", exp: "no match"},
	}

	h := newMatchHighlighter("KEYWORD1 KEYWORD2")
	for specIndex, spec := range specs {
		c.Logf("spec %d", specIndex)
		got := h.Highlight(spec.in)
		c.Assert(got, gc.Equals, spec.exp)
	}
}
