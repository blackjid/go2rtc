package dahua

import (
	"net/http"
	"net/http/httptest"
	"testing"
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
