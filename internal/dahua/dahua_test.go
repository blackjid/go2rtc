package dahua

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAPIDahuaRejectsTooManyChannels(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/dahua?serial=device&channels=65", nil)
	res := httptest.NewRecorder()

	apiDahua(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestDahuaHandlerRejectsInvalidP2PPort(t *testing.T) {
	if _, err := dahuaHandler("dahua://user:pass@device?p2p_port=70000"); err == nil {
		t.Fatal("expected invalid p2p_port error")
	}
}

func TestNegotiateSettleQueryOverridesDefault(t *testing.T) {
	defaultSettle = 500 * time.Millisecond
	t.Cleanup(func() { defaultSettle = 0 })

	tests := []struct {
		name  string
		query string
		want  time.Duration
		bad   bool
	}{
		{name: "absent uses the module default", query: "", want: 500 * time.Millisecond},
		{name: "override", query: "2s", want: 2 * time.Second},
		{name: "zero means the library default", query: "0s", want: 0},
		{name: "negative rejected", query: "-1s", bad: true},
		{name: "unparseable rejected", query: "soon", bad: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseNegotiateSettle(tt.query)
			if tt.bad {
				if err == nil {
					t.Fatalf("parseNegotiateSettle(%q) = %v, want error", tt.query, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseNegotiateSettle(%q): %v", tt.query, err)
			}
			if got != tt.want {
				t.Fatalf("parseNegotiateSettle(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}
