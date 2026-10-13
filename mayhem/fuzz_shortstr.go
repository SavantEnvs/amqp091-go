//go:build gofuzz
// +build gofuzz

package amqp091

import "fmt"

// shortstrSink is the io.Writer writeShortstr emits into: it checks the length prefix as it arrives.
type shortstrSink struct {
	want int
	got  []byte
}

func (k *shortstrSink) Write(p []byte) (int, error) {
	if len(k.got) == 0 && len(p) > 0 && int(p[0]) != k.want {
		panic(fmt.Sprintf("amqp: shortstr of %d bytes written with length prefix %d", k.want, p[0]))
	}
	k.got = append(k.got, p...)
	return len(p), nil
}

// FuzzShortstr feeds an arbitrary string to writeShortstr and requires either an error or a faithful
// encoding: a one-byte length followed by exactly the string.
func FuzzShortstr(data []byte) int {
	s := string(data)
	sink := &shortstrSink{want: len(s)}
	if err := writeShortstr(sink, s); err != nil {
		if len(s) <= 255 {
			panic(fmt.Sprintf("amqp: writeShortstr rejected a %d-byte string: %v", len(s), err))
		}
		return 0
	}
	if len(sink.got) != 1+len(s) || string(sink.got[1:]) != s {
		panic(fmt.Sprintf("amqp: writeShortstr wrote %d bytes for a %d-byte string", len(sink.got), len(s)))
	}
	return 1
}
