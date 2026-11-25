package mysql

import (
	"errors"
	"strings"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

const (
	serverA = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	serverB = "4f22fb58-82db-22f2-af44-d91bb0530673"
)

func gtidSet(t *testing.T, s string) gomysql.GTIDSet {
	t.Helper()
	set, err := parseGTIDSet(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return set
}

func TestResolveStartFirstStartBeginsAtTheServersExecutedSet(t *testing.T) {
	executed := gtidSet(t, serverA+":1-10")
	start, fresh, err := resolveStart("", executed, gtidSet(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	if !fresh || start.String() != serverA+":1-10" {
		t.Fatalf("start = %v fresh = %v", start, fresh)
	}
}

func TestResolveStartResumesFromTheSavedSet(t *testing.T) {
	start, fresh, err := resolveStart(serverA+":1-7", gtidSet(t, serverA+":1-10"), gtidSet(t, serverA+":1-3"))
	if err != nil {
		t.Fatal(err)
	}
	if fresh || start.String() != serverA+":1-7" {
		t.Fatalf("start = %v fresh = %v", start, fresh)
	}
}

// The saved set is valid on a replica that has executed it, whatever its
// own uuid: that is what makes a failover keep the watcher's place.
func TestResolveStartAfterFailoverToAReplicaThatHasEverything(t *testing.T) {
	saved := serverA + ":1-7"
	executed := gtidSet(t, serverA+":1-9,"+serverB+":1-2")
	start, _, err := resolveStart(saved, executed, gtidSet(t, ""))
	if err != nil || start.String() != saved {
		t.Fatalf("start = %v err = %v", start, err)
	}
}

func TestResolveStartFailsWhenTheSavedGTIDWasPurged(t *testing.T) {
	_, _, err := resolveStart(serverA+":1-7", gtidSet(t, serverA+":1-20"), gtidSet(t, serverA+":1-12"))
	if !errors.Is(err, ErrGTIDPurged) {
		t.Fatalf("err = %v, want ErrGTIDPurged", err)
	}
	for _, hint := range []string{"reset_offset", serverA + ":1-7", serverA + ":1-12"} {
		if !strings.Contains(err.Error(), hint) {
			t.Errorf("error %q lacks %q", err, hint)
		}
	}
}

func TestResolveStartFailsWhenTheServerHasNotExecutedTheSavedSet(t *testing.T) {
	_, _, err := resolveStart(serverA+":1-7", gtidSet(t, serverA+":1-5"), gtidSet(t, ""))
	if !errors.Is(err, ErrOffsetAheadOfServer) {
		t.Fatalf("err = %v, want ErrOffsetAheadOfServer", err)
	}
}

func TestResolveStartFailsOnAnUnreadableSavedSet(t *testing.T) {
	_, _, err := resolveStart("not a gtid set", gtidSet(t, serverA+":1-5"), gtidSet(t, ""))
	if !errors.Is(err, ErrBadOffset) {
		t.Fatalf("err = %v, want ErrBadOffset", err)
	}
}

func TestParseGTIDSetToleratesTheServersLineBreaks(t *testing.T) {
	set := gtidSet(t, serverA+":1-5,\n"+serverB+":1-2")
	if !set.Contain(gtidSet(t, serverB+":2")) {
		t.Fatalf("set = %v", set)
	}
}
