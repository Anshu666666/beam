package downloader

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestProbeTarget_RangeSupported verifies that when a server provides
// Content-Length and "Accept-Ranges: bytes", ProbeTarget correctly parses them.
func TestProbeTarget_RangeSupported(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("expected HEAD request, got %s", r.Method)
		}
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", "5242880") // 5 MB
		w.Header().Set("Content-Disposition", `attachment; filename="archive.zip"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	info, err := ProbeTarget(server.URL + "/files/download")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if info.Size != 5242880 {
		t.Errorf("expected size 5242880, got %d", info.Size)
	}
	if !info.RangeSupported {
		t.Errorf("expected RangeSupported to be true")
	}
	if info.Filename != "archive.zip" {
		t.Errorf("expected filename 'archive.zip', got '%s'", info.Filename)
	}
}

// TestProbeTarget_NoRange verifies that if the server does not support byte ranges,
// RangeSupported is reported as false.
func TestProbeTarget_NoRange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1024")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	info, err := ProbeTarget(server.URL + "/data.json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if info.RangeSupported {
		t.Errorf("expected RangeSupported to be false")
	}
	if info.Filename != "data.json" {
		t.Errorf("expected filename 'data.json', got '%s'", info.Filename)
	}
}

// TestProbeTarget_NotFound verifies that non-2xx status codes return an error.
func TestProbeTarget_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	_, err := ProbeTarget(server.URL)
	if err == nil {
		t.Fatal("expected error for 404 status, got nil")
	}
}
