package captcha

import (
	"crypto/subtle"
	"image"
)

// Challenger is implemented by objects that can generate CAPTCHA image challenges.
type Challenger interface {
	Challenge() (img image.Image, imgText string)
}

// Prompter is implemented by objects that display a CAPTCHA image to the user,
// ask them to type their contents and return back their response.
type Prompter interface {
	Prompt(img image.Image) string
}

// ChallengeUser requests a challenge from c and prompts the user for an answer
// using p. If the user's answer matches the challenge then ChallengeUser
// returns true. All comparisons are performed using constant-time checks to
// prevent information leaks.
func ChallengeUser(c Challenger, p Prompter) bool {
	img, expAnswer := c.Challenge()
	userAnswer := p.Prompt(img)

	if subtle.ConstantTimeEq(int32(len(expAnswer)), int32(len(userAnswer))) == 0 {
		return false
	}

	return subtle.ConstantTimeCompare([]byte(userAnswer), []byte(expAnswer)) == 1
}
