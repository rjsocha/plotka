package store

// Delta is the unit of replication: one record for one (name, family).
type Delta struct {
	Name    string `json:"n"`
	V6      bool   `json:"6,omitempty"` // false=A, true=AAAA
	IP      string `json:"i,omitempty"` // empty when Deleted
	TSNanos int64  `json:"t"`           // unix nanoseconds (LWW clock)
	Deleted bool   `json:"d,omitempty"` // tombstone
	Static  bool   `json:"s,omitempty"` // immutable static record (operator config)
}
