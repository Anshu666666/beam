package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestRun_MissingURL verifies that run returns an error when no -url is supplied.
func TestRun_MissingURL(t *testing.T) {
	err := run([]string{})
	if err == nil {
		t.Fatal("expected error when -url is missing, got nil")
	}
	if !strings.Contains(err.Error(), "-url is required") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

// TestRun_EndToEndSuccess verifies full CLI flow: flag parsing, HTTP Range downloading,
// and file output verification.
func TestRun_EndToEndSuccess(t *testing.T) {
	payload := []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Full end-to-end test payload!")

	// Spin up test server supporting HTTP HEAD and Range requests
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			w.WriteHeader(http.StatusOK)
			return
		}

		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			var start, end int
			_, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end)
			if err == nil && start <= end && end < len(payload) {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
				w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
				w.WriteHeader(http.StatusPartialContent)
				w.Write(payload[start : end+1])
				return
			}
		}

		w.WriteHeader(http.StatusOK)
		w.Write(payload)
	}))
	defer server.Close()

	destPath := filepath.Join(t.TempDir(), "downloaded_test.txt")
	args := []string{
		"-url", server.URL,
		"-o", destPath,
		"-w", "2",
		"-c", "3",
	}

	err := run(args)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}

	content, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}

	if !bytes.Equal(content, payload) {
		t.Fatalf("content mismatch.\nExpected: %q\nGot:      %q", string(payload), string(content))
	}
}
