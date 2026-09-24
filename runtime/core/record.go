package core

import (
	"strconv"
	"unicode/utf8"
)

func recordText(message string) string {
	if utf8.ValidString(message) {
		return message
	}
	return strconv.QuoteToASCII(message)
}
