package compute_test

import (
	"math/rand"
	"os"
	"testing"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter04/compute"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter04/compute/gpu"
)

func TestSumOfSquares(t *testing.T) {
	var dev compute.Device
	if os.Getenv("USE_GPU") != "" {
		t.Log("using GPU device")
		dev = gpu.NewDevice()
	} else {
		t.Log("using CPU device")
		dev = cpuComputeDevice{}
	}
	// Generate deterministic sample data and return the expected sum
	in, expSum := genTestData(1024)
	if gotSum := compute.SumOfSquares(dev, in); gotSum != expSum {
		t.Fatalf("expected SumOfSquares to return %f; got %f", expSum, gotSum)
	}
}

func genTestData(n int) (data []float32, sum float32) {
	// Init input with random data using a fixed seed for repeatability
	r := rand.New(rand.NewSource(42))

	data = make([]float32, n)
	for i := 0; i < len(data); i++ {
		data[i] = r.Float32()
		sum += data[i] * data[i]
	}

	return data, sum
}

type cpuComputeDevice struct{}

func (d cpuComputeDevice) Square(in []float32) []float32 {
	for i := 0; i < len(in); i++ {
		in[i] *= in[i]
	}
	return in
}

func (d cpuComputeDevice) Sum(in []float32) (sum float32) {
	for _, v := range in {
		sum += v
	}
	return sum
}
