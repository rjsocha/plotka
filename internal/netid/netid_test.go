package netid

import "testing"

func TestPickAdvertise(t *testing.T) {
	got, err := pick([]string{"10.0.0.2", "10.53.53.53"}, "10.53.53.53")
	if err != nil || got != "10.0.0.2" {
		t.Fatalf("got %q err %v", got, err)
	}
}

func TestPickAmbiguous(t *testing.T) {
	if _, err := pick([]string{"10.0.0.2", "10.0.0.3"}, "10.53.53.53"); err == nil {
		t.Fatal("expected ambiguity error for >1 non-VIP candidate")
	}
}

func TestPickNone(t *testing.T) {
	if _, err := pick([]string{"10.53.53.53"}, "10.53.53.53"); err == nil {
		t.Fatal("expected error when only the VIP is available")
	}
}
