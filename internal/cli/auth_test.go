package cli

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestReplayRequestUsesAdminTokenAndRejectsBadResponses(t *testing.T) {
	SetAdminToken("test-replay-token")
	t.Cleanup(func() { SetAdminToken("") })
	req, err := newReplayRequest("http://localhost/replay")
	if err != nil {
		t.Fatal(err)
	}
	if got := req.Header.Get("Authorization"); got != "Bearer test-replay-token" {
		t.Fatalf("Authorization = %q", got)
	}
	for _, resp := range []*http.Response{
		{StatusCode: http.StatusUnauthorized, Status: "401 Unauthorized", Body: io.NopCloser(strings.NewReader("unauthorized"))},
		{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader("not-json"))},
	} {
		var result map[string]any
		if err := decodeReplayResponse(resp, &result); err == nil {
			t.Fatalf("accepted replay error response: %+v", resp)
		}
	}
}
