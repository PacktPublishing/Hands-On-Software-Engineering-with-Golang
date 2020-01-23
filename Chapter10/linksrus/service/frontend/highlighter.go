package frontend

import (
	"fmt"
	"regexp"
	"strings"
)

type matchHighlighter struct {
	// A regex for matching terms as standalone words at the beginning, middle or
	// end of a sentence
	regexes []*regexp.Regexp
}

// newMatchHighlighter returns a matchHighlighter instance that can highlight
// the specified searchTerms in blocks of text.
func newMatchHighlighter(searchTerms string) *matchHighlighter {
	var regexes []*regexp.Regexp
	for _, token := range strings.Fields(strings.Trim(searchTerms, `"`)) {
		re, err := regexp.Compile(
			fmt.Sprintf(`(?i)%s`, regexp.QuoteMeta(token)),
		)
		if err != nil {
			continue
		}

		regexes = append(regexes, re)
	}

	return &matchHighlighter{regexes: regexes}
}

// Highlight the configured search terms in the provided sentence by wrapping
// them in <em> tags.
func (h *matchHighlighter) Highlight(sentence string) string {
	for _, re := range h.regexes {
		sentence = re.ReplaceAllStringFunc(sentence, func(match string) string {
			return "<em>" + match + "</em>"
		})
	}
	return sentence
}
