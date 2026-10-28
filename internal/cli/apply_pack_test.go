package cli

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestApplyPackCLIDeferReplay(t *testing.T) {
	packYAML := []byte(`apiVersion: dino.io/v1
kind: LogSource
metadata:
  name: replay-test-pack
  version: 1.0.0
spec:
  sourceCategory: other
  format: text
  match:
    contains: replay-test
  fields:
    message:
      from: message
      type: string
  map:
    message: event.message
`)
	packPath := filepath.Join(t.TempDir(), "replay-test-pack.yaml")
	if err := os.WriteFile(packPath, packYAML, 0600); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name          string
		noReplay      bool
		wantReplayReq int
	}{
		{name: "default replays quarantine", wantReplayReq: 1},
		{name: "defer replay", noReplay: true, wantReplayReq: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replayRequests := 0
			client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Header.Get("Authorization") != "Bearer replay-test-token" {
					t.Errorf("missing management auth header: %q", r.Header.Get("Authorization"))
				}
				status, body := http.StatusOK, ""
				switch r.URL.Path {
				case "/api/v1/packs/activate":
					if _, err := io.Copy(io.Discard, r.Body); err != nil {
						t.Errorf("read activation body: %v", err)
					}
				case "/api/v1/quarantine/replay-all":
					replayRequests++
					body = `{"total":0,"replayed":0,"failed":0}`
				default:
					t.Errorf("unexpected request path %q", r.URL.Path)
					status = http.StatusNotFound
				}
				return &http.Response{
					StatusCode: status,
					Status:     http.StatusText(status),
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(body)),
					Request:    r,
				}, nil
			})}

			SetAdminToken("replay-test-token")
			t.Cleanup(func() { SetAdminToken("") })
			applyPackCLI(packPath, "http://management.test", "", tc.noReplay, client)

			if replayRequests != tc.wantReplayReq {
				t.Fatalf("replay requests = %d, want %d", replayRequests, tc.wantReplayReq)
			}
		})
	}
}
