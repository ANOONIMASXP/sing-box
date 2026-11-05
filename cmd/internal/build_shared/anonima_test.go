package build_shared

import (
	"testing"
	"time"
)

func TestAnonimaVersion(t *testing.T) {
	now := time.Date(2026, 11, 5, 23, 59, 0, 0, time.UTC)
	if got := AnonimaVersion("v1.15.0-alpha.13", now); got != "1.15.0-alpha.13.261105-anonima" {
		t.Fatalf("AnonimaVersion() = %q", got)
	}
	if got := AnonimaVersion("1.15.0-alpha.13", now); got != "1.15.0-alpha.13.261105-anonima" {
		t.Fatalf("AnonimaVersion() without v = %q", got)
	}
}

func TestCleanBaseVersion(t *testing.T) {
	for input, expected := range map[string]string{
		"v1.15.0-alpha.13":                     "1.15.0-alpha.13",
		"1.15.0-alpha.13\n":                    "1.15.0-alpha.13",
		"v1.15.0-alpha.13-xiaobaf14g.11":       "1.15.0-alpha.13",
		"v1.15.0-alpha.13-reF1nd":              "1.15.0-alpha.13",
		"v1.15.0-alpha.13.261105-anonima":      "1.15.0-alpha.13",
		"v1.15.0-alpha.13-anonima":             "1.15.0-alpha.13",
		"v1.15.0-alpha.13-3-g1234abc":          "1.15.0-alpha.13",
		"v1.15.0-alpha.13-xiaobaf14g.11-dirty": "1.15.0-alpha.13",
	} {
		if got := cleanBaseVersion(input); got != expected {
			t.Fatalf("cleanBaseVersion(%q) = %q, want %q", input, got, expected)
		}
	}
}
