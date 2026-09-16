package cdc

import "testing"

func TestIsPublishable(t *testing.T) {
	cases := []struct {
		typ  EventType
		want bool
	}{
		{Insert, true},
		{Update, true},
		{Delete, true},
		{DDL, true},
		{Truncate, true},
		{Begin, false},
		{Commit, false},
		{Heartbeat, false},
	}
	for _, c := range cases {
		if got := IsPublishable(c.typ); got != c.want {
			t.Errorf("IsPublishable(%v) = %v, want %v", c.typ, got, c.want)
		}
	}
}
