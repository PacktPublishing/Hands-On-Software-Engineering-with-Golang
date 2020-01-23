package compute

// Device is implemented by objects that can perform vector operations on
// slices containing single-precision floating point numbers.
type Device interface {
	Square([]float32) []float32
	Sum([]float32) float32
}

// SumOfSquares squares each entry on the in slice and returns the sum of all
// squared entries.
func SumOfSquares(c Device, in []float32) float32 {
	sq := c.Square(in)
	return c.Sum(sq)
}
