package frontend

import (
	"bufio"
	"bytes"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

type matchSnippet struct {
	ordinal    int
	text       string
	matchRatio float32
}

type matchSummarizer struct {
	// The list of terms in the search query
	terms []string

	// The maximum size of a summary in characters.
	maxSummaryLen int

	// A re-usable buffer for generating the summary.
	sumBuf bytes.Buffer
}

func newMatchSummarizer(searchTerms string, maxSummaryLen int) *matchSummarizer {
	return &matchSummarizer{
		terms:         strings.Fields(strings.Trim(searchTerms, `"`)),
		maxSummaryLen: maxSummaryLen,
	}
}

func (h *matchSummarizer) MatchSummary(content string) string {
	h.sumBuf.Reset()

	var lastOrdinal = -1
	for _, snippet := range h.snippetsForSummary(content) {
		// This snippet is *not* connected to the last one. Terminate with ellipsis
		// unless buffer already contains an ellipsis.
		if lastOrdinal != -1 && snippet.ordinal-lastOrdinal != 1 {
			_, _ = h.sumBuf.WriteString("..")
		}
		lastOrdinal = snippet.ordinal

		_, _ = h.sumBuf.WriteString(snippet.text)

		if !strings.HasSuffix(snippet.text, ".") {
			_ = h.sumBuf.WriteByte('.')
		}
	}

	return strings.TrimSpace(h.sumBuf.String())
}

func (h *matchSummarizer) snippetsForSummary(content string) []*matchSnippet {
	// Split content in sentences and keep the ones with at least one matching term.
	var matches []*matchSnippet
	scanner := bufio.NewScanner(strings.NewReader(content))
	scanner.Split(scanSentence)
	for ordinal := 0; scanner.Scan(); ordinal++ {
		sentence := scanner.Text()
		if matchRatio := h.matchRatio(sentence); matchRatio > 0 {
			matches = append(matches, &matchSnippet{ordinal: ordinal, text: sentence, matchRatio: matchRatio})
		}
	}

	// Sort by match ratio in descending order (higher quality matches first).
	sort.Slice(matches, func(l, r int) bool {
		return matches[l].matchRatio > matches[r].matchRatio
	})

	// Select matches from the sorted list until we exhaust the max summary length.
	var snippets []*matchSnippet
	for i, remainingLen := 0, h.maxSummaryLen; i < len(matches) && remainingLen > 0; i++ {
		// If we cannot fit the sentence, trim its end. Note: this may
		// result in summaries that do not contain any of the keywords!
		if sLen := len(matches[i].text); sLen > remainingLen {
			matches[i].text = string(([]rune(matches[i].text))[:remainingLen]) + "..."
		}
		remainingLen -= len(matches[i].text)
		snippets = append(snippets, matches[i])
	}

	// Sort selected snippets by ordinal in ascending order. This will ensure that
	// the sentences in the summary have the same order as the original document.
	sort.Slice(snippets, func(l, r int) bool {
		return snippets[l].ordinal < snippets[r].ordinal
	})
	return snippets
}

// matchRatio returns the ratio of matched terms to total words in a sentence.
func (h *matchSummarizer) matchRatio(sentence string) float32 {
	var wordCount, matchWordCount int
	scanner := bufio.NewScanner(strings.NewReader(sentence))
	scanner.Split(bufio.ScanWords)
	for ; scanner.Scan(); wordCount++ {
		word := scanner.Text()
		for _, term := range h.terms {
			if strings.EqualFold(term, word) {
				matchWordCount++
				break
			}
		}
	}

	if wordCount == 0 {
		wordCount = 1
	}

	return float32(matchWordCount) / float32(wordCount)
}

// scanSentence implements a bufio.SplitFunc that emits sentences (with the
// final period characer stripped off).
func scanSentence(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF {
		if len(data) == 0 {
			return 0, nil, nil
		}
		return len(data), data, nil
	}

	var seq [3]rune
	var index, skip int
	for i := 0; i < len(seq); i++ {
		if skip, seq[i] = scanRune(data[index:]); skip < 0 {
			return 0, nil, nil // need more data
		}
		index += skip
	}

	for index < len(data) {
		if breakSentenceAtMiddleChar(seq) {
			return index - skip, data[:index-skip], nil
		}

		// Check next triplet
		seq[0], seq[1] = seq[1], seq[2]
		if skip, seq[2] = scanRune(data[index:]); skip < 0 {
			return 0, nil, nil // need more data
		}
		index += skip
	}

	// Request more data.
	return 0, nil, nil
}

func breakSentenceAtMiddleChar(seq [3]rune) bool {
	return (unicode.IsLower(seq[0]) || unicode.IsSymbol(seq[0]) || unicode.IsNumber(seq[0]) || unicode.IsSpace(seq[0])) &&
		(seq[1] == '.' || seq[1] == '!' || seq[1] == '?') &&
		(unicode.IsPunct(seq[2]) || unicode.IsSpace(seq[2]) || unicode.IsSymbol(seq[0]) || unicode.IsNumber(seq[2]) || unicode.IsUpper(seq[2]))
}

func scanRune(data []byte) (int, rune) {
	if len(data) == 0 {
		return -1, 0
	}

	// Check for ASCII char
	if data[0] < utf8.RuneSelf {
		return 1, rune(data[0])
	}

	// Correct UTF-8 decode without error.
	r, width := utf8.DecodeRune(data)
	if width > 1 {
		return width, r
	}

	// Incomplete data
	return -1, 0
}
