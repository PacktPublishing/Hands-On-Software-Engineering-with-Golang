package fizzbuzz_test

import (
	"testing"

	fizzbuzz "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter04/table-driven"
)

func TestFizzBuzzTableDriven(t *testing.T) {
	specs := []struct {
		descr string
		input int
		exp   string
	}{
		{descr: "evenly divisible by 3", input: 9, exp: "Fizz"},
		{descr: "evenly divisible by 5", input: 25, exp: "Buzz"},
		{descr: "evenly divisible by 3 and 5", input: 15, exp: "FizzBuzz"},
		{descr: "edge case", input: 0, exp: "0"},
	}

	for specIndex, spec := range specs {
		if got := fizzbuzz.Evaluate(spec.input); got != spec.exp {
			t.Errorf("[spec %d: %s] expected to get %q; got %q", specIndex, spec.descr, spec.exp, got)
		}
	}
}

func TestFizzBuzzSubtests(t *testing.T) {
	t.Run("evenly divisible by 3", func(t *testing.T) {
		if exp, got := "Fizz", fizzbuzz.Evaluate(9); got != exp {
			t.Errorf("expected to get %q; got %q", exp, got)
		}
	})
	t.Run("evenly divisible by 5", func(t *testing.T) {
		if exp, got := "Buzz", fizzbuzz.Evaluate(5); got != exp {
			t.Errorf("expected to get %q; got %q", exp, got)
		}
	})
	t.Run("evenly divisible by 3 and 5", func(t *testing.T) {
		if exp, got := "FizzBuzz", fizzbuzz.Evaluate(15); got != exp {
			t.Errorf("expected to get %q; got %q", exp, got)
		}
	})
	t.Run("edge case", func(t *testing.T) {
		if exp, got := "0", fizzbuzz.Evaluate(0); got != exp {
			t.Errorf("expected to get %q; got %q", exp, got)
		}
	})
}

func TestFizzBuzzTableDrivenSubtests(t *testing.T) {
	specs := []struct {
		descr string
		input int
		exp   string
	}{
		{descr: "evenly divisible by 3", input: 9, exp: "Fizz"},
		{descr: "evenly divisible by 5", input: 25, exp: "Buzz"},
		{descr: "evenly divisible by 3 and 5", input: 15, exp: "FizzBuzz"},
		{descr: "edge case", input: 0, exp: "0"},
	}

	for specIndex, spec := range specs {
		t.Run(spec.descr, func(t *testing.T) {
			if got := fizzbuzz.Evaluate(spec.input); got != spec.exp {
				t.Errorf("[spec %d: %s] expected to get %q; got %q", specIndex, spec.descr, spec.exp, got)
			}
		})
	}
}
