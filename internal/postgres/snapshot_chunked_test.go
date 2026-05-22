package postgres

import "testing"

func TestBuildChunkQueryFirstChunk(t *testing.T) {
	q, args := buildChunkQuery(`"id","name"`, `"public"`, `"users"`, `"id"`, nil, 100)
	want := `SELECT "id","name" FROM "public"."users" ORDER BY "id" LIMIT $1`
	if q != want {
		t.Errorf("query = %q, want %q", q, want)
	}
	if len(args) != 1 || args[0] != 100 {
		t.Errorf("args = %v", args)
	}
}

func TestBuildChunkQuerySubsequentChunk(t *testing.T) {
	q, args := buildChunkQuery(`"id"`, `"s"`, `"t"`, `"id"`, 42, 50)
	want := `SELECT "id" FROM "s"."t" WHERE "id" > $1 ORDER BY "id" LIMIT $2`
	if q != want {
		t.Errorf("query = %q, want %q", q, want)
	}
	if len(args) != 2 || args[0] != 42 || args[1] != 50 {
		t.Errorf("args = %v", args)
	}
}
