package tail

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"time"
	"unicode/utf16"
	"unicode/utf8"
)

// Decoding json-file lines with encoding/json took 14 % of the prototype tailer's CPU
// and allocated a string per line. dockerd writes one fixed
// shape (daemon/logger/jsonfilelog/jsonlog, MarshalJSONBuf):
//
//	{"log":"...","stream":"stdout","attrs":{...},"time":"2026-10-21T08:00:00.123456789Z"}
//
// with "attrs" only when the container sets log labels or env, and the time in
// RFC 3339 with up to nine fraction digits, trailing zeros dropped, in UTC. The
// parser below reads exactly that, unescaping the text straight into the
// caller's buffer, and leaves anything else to encoding/json.

var (
	keyLog    = []byte(`"log":"`)
	keyStream = []byte(`"stream":"`)
	keyAttrs  = []byte(`"attrs":`)
	keyTime   = []byte(`"time":"`)
)

// parseJSONFile appends the unescaped log text of one line to dst. ok is false
// when the line is not in the shape above; dst is then returned as it was.
func parseJSONFile(dst, line []byte) (out []byte, stream string, ts time.Time, ok bool) {
	mark := len(dst)
	fail := func() ([]byte, string, time.Time, bool) { return dst[:mark], "", time.Time{}, false }
	p := line
	if len(p) < 2 || p[0] != '{' || p[len(p)-1] != '}' {
		return fail()
	}
	p = p[1 : len(p)-1]

	if bytes.HasPrefix(p, keyLog) {
		var rest []byte
		if dst, rest, ok = unquote(dst, p[len(keyLog):]); !ok || len(rest) == 0 || rest[0] != ',' {
			return fail()
		}
		p = rest[1:]
	}

	if !bytes.HasPrefix(p, keyStream) {
		return fail()
	}
	p = p[len(keyStream):]
	end := bytes.IndexByte(p, '"')
	if end < 0 {
		return fail()
	}
	switch string(p[:end]) { // compared without allocating
	case "stdout":
		stream = "stdout"
	case "stderr":
		stream = "stderr"
	default:
		return fail()
	}
	p = p[end+1:]
	if len(p) == 0 || p[0] != ',' {
		return fail()
	}
	p = p[1:]

	if bytes.HasPrefix(p, keyAttrs) {
		// The braces find the object's end; json.Valid, which does not
		// allocate, makes sure it is one: `{{}}` balances, and is not (found
		// by FuzzParseJSONFile).
		n := skipObject(p[len(keyAttrs):])
		if n < 0 || !json.Valid(p[len(keyAttrs):len(keyAttrs)+n]) {
			return fail()
		}
		p = p[len(keyAttrs)+n:]
		if len(p) == 0 || p[0] != ',' {
			return fail()
		}
		p = p[1:]
	}

	if !bytes.HasPrefix(p, keyTime) {
		return fail()
	}
	p = p[len(keyTime):]
	end = bytes.IndexByte(p, '"')
	if end < 0 || end != len(p)-1 {
		return fail()
	}
	if ts, ok = parseTime(p[:end]); !ok {
		return fail()
	}
	return dst, stream, ts, true
}

// unquote appends the JSON string starting at p (after its opening quote) to
// dst, unescaped, and returns what follows its closing quote.
func unquote(dst, p []byte) ([]byte, []byte, bool) {
	for {
		q := bytes.IndexByte(p, '"')
		if q < 0 {
			return dst, nil, false
		}
		b := bytes.IndexByte(p[:q], '\\')
		if b < 0 {
			if hasControl(p[:q]) {
				return dst, nil, false
			}
			return append(dst, p[:q]...), p[q+1:], true
		}
		if hasControl(p[:b]) {
			return dst, nil, false
		}
		dst = append(dst, p[:b]...)
		p = p[b:]
		if len(p) < 2 {
			return dst, nil, false
		}
		switch c := p[1]; c {
		case '"', '\\', '/':
			dst = append(dst, c)
		case 'n':
			dst = append(dst, '\n')
		case 'r':
			dst = append(dst, '\r')
		case 't':
			dst = append(dst, '\t')
		case 'b':
			dst = append(dst, '\b')
		case 'f':
			dst = append(dst, '\f')
		case 'u':
			r, n := unicodeEscape(p)
			if n == 0 {
				return dst, nil, false
			}
			dst = utf8.AppendRune(dst, r)
			p = p[n:]
			continue
		default:
			return dst, nil, false
		}
		p = p[2:]
	}
}

// hasControl reports a control character, which JSON allows in a string only
// escaped (found by FuzzParseJSONFile; dockerd always escapes them). Eight
// bytes at a time: x - 0x20 in each byte borrows, and sets the byte's top bit,
// only for a byte below 0x20 (one that had its top bit set is masked out by
// ^x). A byte at a time this check made the parser 45 % slower.
func hasControl(p []byte) bool {
	for len(p) >= 8 {
		x := binary.LittleEndian.Uint64(p)
		if (x-0x2020202020202020)&^x&0x8080808080808080 != 0 {
			return true
		}
		p = p[8:]
	}
	for _, c := range p {
		if c < 0x20 {
			return true
		}
	}
	return false
}

// unicodeEscape decodes \uXXXX at the start of p, and a following low
// surrogate if XXXX is a high one. n is the bytes consumed, 0 if malformed.
func unicodeEscape(p []byte) (rune, int) {
	r, ok := hex4(p)
	if !ok {
		return 0, 0
	}
	if utf16.IsSurrogate(r) {
		if r2, ok := hex4(p[6:]); ok && len(p) >= 12 && p[6] == '\\' && p[7] == 'u' {
			if d := utf16.DecodeRune(r, r2); d != utf8.RuneError {
				return d, 12
			}
		}
		return utf8.RuneError, 6
	}
	return r, 6
}

// hex4 reads the four hex digits of \uXXXX at the start of p.
func hex4(p []byte) (rune, bool) {
	if len(p) < 6 || p[0] != '\\' || p[1] != 'u' {
		return 0, false
	}
	var r rune
	for _, c := range p[2:6] {
		switch {
		case '0' <= c && c <= '9':
			c -= '0'
		case 'a' <= c && c <= 'f':
			c = c - 'a' + 10
		case 'A' <= c && c <= 'F':
			c = c - 'A' + 10
		default:
			return 0, false
		}
		r = r<<4 | rune(c)
	}
	return r, true
}

// skipObject returns the length of the JSON object at the start of p, or -1.
func skipObject(p []byte) int {
	if len(p) == 0 || p[0] != '{' {
		return -1
	}
	depth, inString := 0, false
	for i := 0; i < len(p); i++ {
		c := p[i]
		switch {
		case inString:
			switch c {
			case '\\':
				i++
			case '"':
				inString = false
			}
		case c == '"':
			inString = true
		case c == '{':
			depth++
		case c == '}':
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return -1
}

// parseTime reads an RFC 3339 time in UTC with 0-9 fraction digits without
// allocating, and hands anything else (an offset, a leap second) to time.Parse.
func parseTime(b []byte) (time.Time, bool) {
	if len(b) >= 20 && b[len(b)-1] == 'Z' && b[4] == '-' && b[7] == '-' && b[10] == 'T' && b[13] == ':' && b[16] == ':' {
		year, ok1 := digits(b[0:4])
		month, ok2 := digits(b[5:7])
		day, ok3 := digits(b[8:10])
		hour, ok4 := digits(b[11:13])
		minute, ok5 := digits(b[14:16])
		sec, ok6 := digits(b[17:19])
		frac := b[19 : len(b)-1]
		nsec, ok7 := 0, true
		if len(frac) > 0 {
			ok7 = frac[0] == '.' && len(frac) >= 2 && len(frac) <= 10
			if ok7 {
				nsec, ok7 = digits(frac[1:])
				for range 10 - len(frac) {
					nsec *= 10
				}
			}
		}
		if ok1 && ok2 && ok3 && ok4 && ok5 && ok6 && ok7 && month >= 1 && month <= 12 && day >= 1 &&
			day <= daysIn(time.Month(month), year) && hour < 24 && minute < 60 && sec < 60 {
			return time.Date(year, time.Month(month), day, hour, minute, sec, nsec, time.UTC), true
		}
	}
	t, err := time.Parse(time.RFC3339Nano, string(b))
	return t, err == nil
}

var monthDays = [13]int{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

// daysIn is the number of days of a month: time.Date would turn February 30th
// into March 2nd, where time.Parse refuses it (found by FuzzParseTime).
func daysIn(m time.Month, year int) int {
	if m == time.February && year%4 == 0 && (year%100 != 0 || year%400 == 0) {
		return 29
	}
	return monthDays[m]
}

func digits(b []byte) (int, bool) {
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}
