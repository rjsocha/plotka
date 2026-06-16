// Package protocol parses the plotka registration grammar shared by the raw,
// DNS, and HTTP channels. The leading ':' channel marker (raw/DNS) must be
// stripped by the caller before calling Parse.
package protocol

import (
	"fmt"
	"strings"
)

type Op int

const (
	OpRegister Op = iota
	OpDeregister
)

// Command is a parsed register/deregister request.
type Command struct {
	Op   Op
	Addr string // explicit address, "" => use connection source IP
	Name string
}

// Parse parses "<op>[addr].name". Op is the first byte (+/-). An optional
// [addr] follows, then a dot, then the name. Without [addr] the rest is name.
func Parse(s string) (Command, error) {
	if s == "" {
		return Command{}, fmt.Errorf("empty token")
	}
	var cmd Command
	switch s[0] {
	case '+':
		cmd.Op = OpRegister
	case '-':
		cmd.Op = OpDeregister
	default:
		return Command{}, fmt.Errorf("token %q: missing +/- op", s)
	}
	rest := s[1:]
	if strings.HasPrefix(rest, "[") {
		end := strings.IndexByte(rest, ']')
		if end < 0 {
			return Command{}, fmt.Errorf("token %q: unterminated [addr]", s)
		}
		cmd.Addr = rest[1:end]
		after := rest[end+1:]
		if !strings.HasPrefix(after, ".") {
			return Command{}, fmt.Errorf("token %q: expected '.' after [addr]", s)
		}
		cmd.Name = after[1:]
	} else {
		cmd.Name = rest
	}
	if cmd.Name == "" {
		return Command{}, fmt.Errorf("token %q: empty name", s)
	}
	return cmd, nil
}
