package protocol

import "testing"

func TestParse(t *testing.T) {
	cases := []struct {
		in       string
		wantOp   Op
		wantAddr string
		wantName string
		wantErr  bool
	}{
		{"+abc.pl", OpRegister, "", "abc.pl", false},
		{"-abc.pl", OpDeregister, "", "abc.pl", false},
		{"+host.lab", OpRegister, "", "host.lab", false},
		{"+[192.168.1.2].abc.pl", OpRegister, "192.168.1.2", "abc.pl", false},
		{"-[192.168.1.2].abc.pl", OpDeregister, "192.168.1.2", "abc.pl", false},
		{"+[2001:db8::1].v6.host", OpRegister, "2001:db8::1", "v6.host", false},
		{"", 0, "", "", true},
		{"abc.pl", 0, "", "", true},
		{"+", 0, "", "", true},
		{"+[10.0.0.1]", 0, "", "", true},
		{"+[bad.name", 0, "", "", true},
		{"+[10.0.0.1]abc", 0, "", "", true},
	}
	for _, c := range cases {
		got, err := Parse(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("Parse(%q): expected error, got %+v", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("Parse(%q): unexpected error %v", c.in, err)
			continue
		}
		if got.Op != c.wantOp || got.Addr != c.wantAddr || got.Name != c.wantName {
			t.Errorf("Parse(%q) = %+v, want op=%v addr=%q name=%q", c.in, got, c.wantOp, c.wantAddr, c.wantName)
		}
	}
}
