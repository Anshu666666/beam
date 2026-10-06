package web

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestServer_StaticAssetServing(t *testing.T) {
	tempDir := t.TempDir()
	indexFile := filepath.Join(tempDir, "index.html")
	if err := os.WriteFile(indexFile, []byte("<h1>Beam Online</h1>"), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	server := NewServer(tempDir)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET / failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", resp.StatusCode)
	}
}

func TestServer_StreamEndpointMissingURL(t *testing.T) {
	server := NewServer(t.TempDir())
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	// Calling stream endpoint without url query parameter should return an SSE error payload
	resp, err := http.Get(ts.URL + "/api/download/stream")
	if err != nil {
		t.Fatalf("GET /api/download/stream failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for SSE stream, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Errorf("expected Content-Type text/event-stream, got %s", resp.Header.Get("Content-Type"))
	}
}
