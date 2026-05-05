package main

import "testing"

func TestParseStatefulSetOrdinal(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"bigfleet-shard-0", 0, false},
		{"bigfleet-shard-2", 2, false},
		{"shard-17", 17, false},
		{"a-1234567", 1234567, false},
		{"shard-", 0, true},
		{"shard", 0, true},
		{"shard-abc", 0, true},
		{"", 0, true},
	}
	for _, c := range cases {
		got, err := parseStatefulSetOrdinal(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseStatefulSetOrdinal(%q) = %d, nil; want error", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseStatefulSetOrdinal(%q) error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("parseStatefulSetOrdinal(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}
