package downloader

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

// TestDownload_RangeSupported_Concurrent verifies the complete concurrent engine:
// 1. Probes server metadata.
// 2. Partitions into chunks.
// 3. Downloads concurrently across worker pool with progress tracking.
// 4. Stitches into final file.
// 5. Validates cryptographic SHA-256 integrity against the original payload.
func TestDownload_RangeSupported_Concurrent(t *testing.T) {
	// Generate 128 KB test payload
	payload := make([]byte, 128*1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	hasher := sha256.New()
	hasher.Write(payload)
	expectedHash := hex.EncodeToString(hasher.Sum(nil))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			w.WriteHeader(http.StatusOK)
			return
		}

		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			parts := strings.Split(strings.TrimPrefix(rangeHeader, "bytes="), "-")
			s, _ := strconv.Atoi(parts[0])
			e, _ := strconv.Atoi(parts[1])
			w.WriteHeader(http.StatusPartialContent)
			w.Write(payload[s : e+1])
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(payload)
	}))
	defer server.Close()

	tempFile, err := os.CreateTemp("", "e2e_concurrent_out_*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	outPath := tempFile.Name()
	tempFile.Close()
	defer os.Remove(outPath)

	var progressCallCount atomic.Int32

	opts := Options{
		URL:        server.URL + "/archive.bin",
		OutputFile: outPath,
		Workers:    4,
		Chunks:     8,
		OnProgress: func(tracker *ProgressTracker) {
			progressCallCount.Add(1)
		},
	}

	err = Download(opts)
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	// Verify cryptographic hash integrity of the stitched file
	f, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("failed to open output file: %v", err)
	}
	defer f.Close()

	h := sha256.New()
	io.Copy(h, f)
	actualHash := hex.EncodeToString(h.Sum(nil))

	if actualHash != expectedHash {
		t.Errorf("SHA-256 hash mismatch: expected %s, got %s", expectedHash, actualHash)
	}

	// Verify progress callback was invoked
	if progressCallCount.Load() == 0 {
		t.Errorf("expected OnProgress to be called at least once")
	}
}

// TestDownload_NonRange_Fallback verifies graceful fallback to single-stream download
// when the server does not support byte ranges.
func TestDownload_NonRange_Fallback(t *testing.T) {
	payload := []byte("Streaming non-range payload content from a dynamic HTTP endpoint.")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			// Deliberately omit Accept-Ranges header
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			w.WriteHeader(http.StatusOK)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(payload)
	}))
	defer server.Close()

	tempFile, _ := os.CreateTemp("", "e2e_fallback_out_*")
	outPath := tempFile.Name()
	tempFile.Close()
	defer os.Remove(outPath)

	opts := Options{
		URL:        server.URL + "/data.csv",
		OutputFile: outPath,
		Workers:    4,
		Chunks:     4,
	}

	err := Download(opts)
	if err != nil {
		t.Fatalf("Download fallback failed: %v", err)
	}

	downloaded, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed reading output: %v", err)
	}

	if !bytes.Equal(downloaded, payload) {
		t.Errorf("content mismatch: expected '%s', got '%s'", string(payload), string(downloaded))
	}
}

// TestDownload_AutoDeriveFilename verifies that if OutputFile is empty,
// the filename is auto-derived from the URL.
func TestDownload_AutoDeriveFilename(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", "12")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Write([]byte("HELLO_WORLD!"))
	}))
	defer server.Close()

	const targetFilename = "test_autoderived_file.txt"
	defer os.Remove(targetFilename)

	opts := Options{
		URL: server.URL + "/" + targetFilename,
		// OutputFile omitted
	}

	err := Download(opts)
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	if _, err := os.Stat(targetFilename); os.IsNotExist(err) {
		t.Errorf("expected auto-derived file %s to exist", targetFilename)
	}
}
