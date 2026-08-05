package voicepipelinecore

import (
	"unicode"
	"unicode/utf8"
)

// Sentence-boundary detection for ResponseGuardProcessor. Deliberately
// separate from TTS's endsWithPunctuation (tts_processor.go), which is a
// unicode.IsPunct flush-on-commas heuristic tuned for TTS aggregation, not a
// sentence boundary.
//
// The key invariant: split the buffer; never test whether it merely ends
// with a terminator. LLM deltas do not align to sentences, so a chunk that
// ends mid-sentence (with a terminator earlier in the buffer) would have its
// interior boundary missed by an ends-with test, silently merging two
// sentences into one fragment.

// endsWithSentenceTerminator reports whether s, after trailing whitespace
// and closing delimiters are stripped, ends in a sentence terminator.
func endsWithSentenceTerminator(s string) bool {
	for len(s) > 0 {
		r, size := utf8.DecodeLastRuneInString(s)
		if r == utf8.RuneError && size <= 1 {
			return false
		}
		if unicode.IsSpace(r) || isClosingDelimiter(r) {
			s = s[:len(s)-size]
			continue
		}
		return isSentenceTerminator(r)
	}
	return false
}

// splitSentences cuts every complete sentence out of s and returns the
// leftover remainder (text after the last complete sentence, with no
// terminator of its own yet). Each returned sentence includes its
// terminator run and any trailing closing delimiters, but not the
// whitespace that separates it from the next sentence.
func splitSentences(s string) (sentences []string, remainder string) {
	start := 0
	i := 0
	for i < len(s) {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size <= 1 {
			break
		}
		if !isSentenceTerminator(r) {
			i += size
			continue
		}
		// Consume the whole terminator run, then any closing delimiters, then
		// the trailing whitespace that separates this sentence from the next.
		end := i + size
		for end < len(s) {
			r2, s2 := utf8.DecodeRuneInString(s[end:])
			if r2 == utf8.RuneError && s2 <= 1 {
				break
			}
			if isSentenceTerminator(r2) || isClosingDelimiter(r2) {
				end += s2
				continue
			}
			break
		}
		cut := end
		for cut < len(s) {
			r3, s3 := utf8.DecodeRuneInString(s[cut:])
			if r3 == utf8.RuneError && s3 <= 1 {
				break
			}
			if !unicode.IsSpace(r3) {
				break
			}
			cut += s3
		}
		sentences = append(sentences, s[start:end])
		start = cut
		i = cut
	}
	return sentences, s[start:]
}

// isSentenceTerminator reports whether r ends a sentence. Includes the
// Devanagari danda/double-danda: the model sometimes emits them even when
// committed history shows an ASCII period (Cartesia normalises it), so
// support here is load-bearing, not decorative.
func isSentenceTerminator(r rune) bool {
	switch r {
	case '.', '!', '?',
		'…', // … horizontal ellipsis
		'।', // । Devanagari danda
		'॥': // ॥ Devanagari double danda
		return true
	}
	return false
}

// isClosingDelimiter reports whether r is a closing quote/bracket that
// trails a sentence terminator, e.g. the `"` in `He said "stop."`.
func isClosingDelimiter(r rune) bool {
	switch r {
	case '"', '\'', ')', ']', '}',
		'”', // ” right double quote
		'’', // ’ right single quote
		'»': // » right guillemet
		return true
	}
	return false
}
