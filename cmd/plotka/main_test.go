package main

import "testing"

func TestDispatchUnknownReturnsError(t *testing.T) {
	if err := dispatch([]string{"bogus"}); err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestDispatchVersion(t *testing.T) {
	if err := dispatch([]string{"version"}); err != nil {
		t.Fatalf("version should succeed, got %v", err)
	}
}
