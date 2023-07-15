package parquetschemaparser

import (
	"fmt"
	"io"
	"strings"

	st "github.com/wolfeidau/stringtokenizer"
)

type SchemaTokenizer struct {
	buffer      []string
	line        int
	currentLine strings.Builder
	st          *st.StringTokenizer
}

func NewSchemaTokenizer(rdr io.Reader, delimiters string) *SchemaTokenizer {
	return &SchemaTokenizer{
		line: 1,
		st:   st.NewStringTokenizer(rdr, delimiters, true),
	}
}

//	PeakToken peeks at the next non-whitespace token without consuming it.
//
// It loops through the underlying StringTokenizer, appending each token to
// the buffer. If a non-whitespace token is found, it is returned.
//
// If the end of the schema is reached without finding a non-whitespace token,
// it panics with an "unexpected end of schema" error.
func (sct *SchemaTokenizer) PeakToken() string {
	for sct.st.HasMoreTokens() {
		t := sct.st.NextToken()

		sct.buffer = append(sct.buffer, t)

		if !isWhitespace(t) {
			return t
		}
	}

	panic("unexpected end of schema")
}

//	NextToken returns the next non-whitespace token from the tokenizer.
//
// It first checks if there are any tokens buffered in sct.buffer and pops
// the last one if so. If that token is not whitespace, it is returned.
//
// Otherwise, it loops through the underlying StringTokenizer, appending each
// token to sct.buffer and tracking line numbers in sct.line and sct.currentLine.
// If a non-whitespace token is encountered, it is returned.
//
// If the end of the input is reached without finding a non-whitespace token,
// it panics with an "unexpected end of schema" error.
func (sct *SchemaTokenizer) NextToken() string {
	if len(sct.buffer) > 0 {
		t := sct.pop()

		if !isWhitespace(t) {
			return t
		}
	}

	for sct.st.HasMoreTokens() {
		t := sct.st.NextToken()

		sct.appendToLine(t)

		if !isWhitespace(t) {
			return t
		}
	}

	panic("unexpected end of schema")
}

func (sct *SchemaTokenizer) pop() (t string) {
	t, sct.buffer = sct.buffer[len(sct.buffer)-1], sct.buffer[:len(sct.buffer)-1]
	return
}

func (sct *SchemaTokenizer) appendToLine(t string) {
	if t == "\n" {
		sct.line++
		sct.currentLine = strings.Builder{}
	} else {
		sct.currentLine.WriteString(t)
	}
}

func (sct *SchemaTokenizer) AssertNextToken(msg string, expected ...string) error {
	t := sct.NextToken()

	for _, e := range expected {
		if e == t {
			return nil
		}
	}

	return fmt.Errorf("%s, expected %s but found %s", msg, expected, t)
}

func (sct *SchemaTokenizer) GetLocation() string {
	return fmt.Sprintf("line %d : %s", sct.line, sct.currentLine.String())
}

func isWhitespace(t string) bool {
	return t == " " || t == "\t" || t == "\n" || t == "\r"
}
