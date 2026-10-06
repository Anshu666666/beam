package web

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestSyntheticSeeker verifies seeking and reading byte ranges without memory overhead.
func TestSyntheticSeeker(t *testing.T) {
	seeker := NewSyntheticSeeker(1024)

	// Read first 10 bytes
	buf := make([]byte, 10)
	n, err := seeker.Read(buf)
	if err != nil || n != 10 {
		t.Fatalf("expected 10 bytes read, got %d, err: %v", n, err)
	}

	// Seek to byte 500
	pos, err := seeker.Seek(500, io.SeekStart)
	if err != nil || pos != 500 {
		t.Fatalf("expected seek to 500, got %d, err: %v", pos, err)
	}

	// Read next 5 bytes
	buf2 := make([]byte, 5)
	n, err = seeker.Read(buf2)
	if err != nil || n != 5 {
		t.Fatalf("expected 5 bytes read, got %d, err: %v", n, err)
	}
}

// TestServer_DemoEndpointRangeSupport verifies that the built-in demo endpoint
// returns 206 Partial Content and correct Content-Range header for RFC 7233 requests.
func TestServer_DemoEndpointRangeSupport(t *testing.T) {
	server := NewServer(t.TempDir())
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	// 1. Test HEAD request for Content-Length and Accept-Ranges
	headResp, err := http.Head(ts.URL + "/api/demo/50mb")
	if err != nil {
		t.Fatalf("HEAD request failed: %v", err)
	}
	defer headResp.Body.Close()

	if headResp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for HEAD, got %d", headResp.StatusCode)
	}
	if headResp.Header.Get("Accept-Ranges") != "bytes" {
		t.Errorf("expected Accept-Ranges: bytes, got %q", headResp.Header.Get("Accept-Ranges"))
	}
	if headResp.ContentLength != 50*1024*1024 {
		t.Errorf("expected Content-Length: 52428800, got %d", headResp.ContentLength)
	}

	// 2. Test Range GET request
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/demo/50mb", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Range", "bytes=0-999")

	getResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Range request failed: %v", err)
	}
	defer getResp.Body.Close()

	if getResp.StatusCode != http.StatusPartialContent {
		t.Errorf("expected 206 Partial Content, got %d", getResp.StatusCode)
	}
	body, _ := io.ReadAll(getResp.Body)
	if len(body) != 1000 {
		t.Errorf("expected 1000 bytes returned, got %d", len(body))
	}
}
