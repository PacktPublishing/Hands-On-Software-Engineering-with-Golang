package fizzbuzz

import "fmt"

// Evaluate implements the fizzbuzz logic for the integer value n and returns:
// - "Fizz" if n is divisible by 3
// - "Buzz" if n is divisible by 5
// - "FizzBuzz" if n is divisible by both 3 and 5
// - n otherwise
func Evaluate(n int) string {
	if n != 0 {
		switch {
		case n%3 == 0 && n%5 == 0:
			return "FizzBuzz"
		case n%3 == 0:
			return "Fizz"
		case n%5 == 0:
			return "Buzz"
		}
	}
	return fmt.Sprint(n)
}
