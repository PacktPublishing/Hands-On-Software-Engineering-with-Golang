package partition

import (
	"github.com/google/uuid"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(RangeTestSuite))

type RangeTestSuite struct {
}

func (s *RangeTestSuite) TestNewRangeErrors(c *gc.C) {
	_, err := NewRange(
		uuid.MustParse("40000000-0000-0000-0000-000000000000"),
		uuid.MustParse("00000000-0000-0000-0000-000000000000"),
		1,
	)
	c.Assert(err, gc.ErrorMatches, "range start UUID must be less than the end UUID")

	_, err = NewRange(
		uuid.MustParse("00000000-0000-0000-0000-000000000000"),
		uuid.MustParse("40000000-0000-0000-0000-000000000000"),
		0,
	)
	c.Assert(err, gc.ErrorMatches, "number of partitions must be at least equal to 1")
}

func (s *RangeTestSuite) TestEvenSplit(c *gc.C) {
	r, err := NewFullRange(4)
	c.Assert(err, gc.IsNil)

	expExtents := [][2]uuid.UUID{

		{uuid.MustParse("00000000-0000-0000-0000-000000000000"), uuid.MustParse("40000000-0000-0000-0000-000000000000")},
		{uuid.MustParse("40000000-0000-0000-0000-000000000000"), uuid.MustParse("80000000-0000-0000-0000-000000000000")},
		{uuid.MustParse("80000000-0000-0000-0000-000000000000"), uuid.MustParse("c0000000-0000-0000-0000-000000000000")},
		{uuid.MustParse("c0000000-0000-0000-0000-000000000000"), uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")},
	}

	for i, exp := range expExtents {
		c.Logf("extent: %d", i)
		gotFrom, gotTo, err := r.PartitionExtents(i)
		c.Assert(err, gc.IsNil)
		c.Assert(gotFrom.String(), gc.Equals, exp[0].String())
		c.Assert(gotTo.String(), gc.Equals, exp[1].String())
	}
}

func (s *RangeTestSuite) TestOddSplit(c *gc.C) {
	r, err := NewFullRange(3)
	c.Assert(err, gc.IsNil)

	expExtents := [][2]uuid.UUID{
		{uuid.MustParse("00000000-0000-0000-0000-000000000000"), uuid.MustParse("55555555-5555-5555-5555-555555555555")},
		{uuid.MustParse("55555555-5555-5555-5555-555555555555"), uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")},
		{uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")},
	}

	for i, exp := range expExtents {
		c.Logf("extent: %d", i)
		gotFrom, gotTo, err := r.PartitionExtents(i)
		c.Assert(err, gc.IsNil)
		c.Assert(gotFrom.String(), gc.Equals, exp[0].String())
		c.Assert(gotTo.String(), gc.Equals, exp[1].String())
	}
}

func (s *RangeTestSuite) TestPartitionExtentsError(c *gc.C) {
	r, err := NewRange(
		uuid.MustParse("11111111-0000-0000-0000-000000000000"),
		uuid.MustParse("55555555-0000-0000-0000-000000000000"),
		1,
	)
	c.Assert(err, gc.IsNil)

	_, _, err = r.PartitionExtents(1)
	c.Assert(err, gc.ErrorMatches, "invalid partition index")
}

func (s *RangeTestSuite) TestPartitionLookup(c *gc.C) {
	r, err := NewFullRange(4)
	c.Assert(err, gc.IsNil)

	from, to := r.Extents()
	c.Assert(from, gc.Equals, uuid.MustParse("00000000-0000-0000-0000-000000000000"))
	c.Assert(to, gc.Equals, uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))

	specs := []struct {
		id  uuid.UUID
		exp int
	}{
		{uuid.MustParse("00000000-0000-0000-0000-000000000000"), 0},
		{uuid.MustParse("12345678-0000-0000-0000-000000000000"), 0},
		{uuid.MustParse("40000000-0000-0000-0000-000000000000"), 1},
		{uuid.MustParse("ffffffff-ffff-ffff-ffff-fffffffffffe"), 3},
	}

	for i, spec := range specs {
		c.Logf("spec: %d -> lookup partition for %v", i, spec.id)
		got, err := r.PartitionForID(spec.id)
		c.Assert(err, gc.IsNil)
		c.Assert(got, gc.Equals, spec.exp)
	}
}

func (s *RangeTestSuite) TestPartitionLookupError(c *gc.C) {
	r, err := NewRange(
		uuid.MustParse("11111111-0000-0000-0000-000000000000"),
		uuid.MustParse("55555555-0000-0000-0000-000000000000"),
		1,
	)
	c.Assert(err, gc.IsNil)

	_, err = r.PartitionForID(uuid.MustParse("11111110-ffff-ffff-ffff-ffffffffffff"))
	c.Assert(err, gc.ErrorMatches, "unable to detect partition.*")

	_, err = r.PartitionForID(uuid.MustParse("bead5555-0000-0000-0000-000000000000"))
	c.Assert(err, gc.ErrorMatches, "unable to detect partition.*")
}
