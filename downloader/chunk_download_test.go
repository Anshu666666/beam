package downloader

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

// TestDownloadChunk_Success tests downloading a specific byte range (bytes=10-19)
// and verifying that exactly those 10 bytes are written to the chunk's TempFilePath on disk.
func TestDownloadChunk_Success(t *testing.T) {
	fullPayload := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "bytes=10-19" {
			t.Errorf("expected Range 'bytes=10-19', got '%s'", rangeHeader)
		}

		// RFC 7233: partial content status
		w.WriteHeader(http.StatusPartialContent)
		// Send slice [10:20] -> bytes 10 through 19 (10 bytes total)
		w.Write(fullPayload[10:20])
	}))
	defer server.Close()

	// Create a temporary file on disk for this test
	tempFile, err := os.CreateTemp("", "chunk_download_test_*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tempPath := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempPath)

	chunk := Chunk{
		Index:        0,
		Start:        10,
		End:          19,
		TempFilePath: tempPath,
	}

	err = DownloadChunk(server.URL, chunk, nil)
	if err != nil {
		t.Fatalf("DownloadChunk failed: %v", err)
	}

	// Verify file content on disk
	downloadedData, err := os.ReadFile(tempPath)
	if err != nil {
		t.Fatalf("failed to read downloaded chunk file: %v", err)
	}

	expectedData := fullPayload[10:20]
	if !bytes.Equal(downloadedData, expectedData) {
		t.Errorf("expected content '%s', got '%s'", string(expectedData), string(downloadedData))
	}
}

// TestDownloadChunk_ServerIgnoresRange verifies that if the server returns 200 OK
// instead of 206 Partial Content, DownloadChunk flags it as an error.
func TestDownloadChunk_ServerIgnoresRange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // Wrong status for range request
		w.Write([]byte("full file content"))
	}))
	defer server.Close()

	tempFile, _ := os.CreateTemp("", "chunk_test_*")
	tempPath := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempPath)

	chunk := Chunk{Index: 0, Start: 0, End: 4, TempFilePath: tempPath}

	err := DownloadChunk(server.URL, chunk, nil)
	if err == nil {
		t.Fatal("expected error when server returns 200 OK for range request, got nil")
	}
}

// TestDownloadChunk_EmptyPath verifies that an empty TempFilePath returns an error.
func TestDownloadChunk_EmptyPath(t *testing.T) {
	chunk := Chunk{Index: 0, Start: 0, End: 10, TempFilePath: ""}

	err := DownloadChunk("http://example.com", chunk, nil)
	if err == nil {
		t.Fatal("expected error for empty TempFilePath, got nil")
	}
}

// TestDownloadChunk_WithProgressCounter verifies that if an io.Writer is provided,
// bytes are copied to both the file on disk and the progress writer via io.TeeReader.
func TestDownloadChunk_WithProgressCounter(t *testing.T) {
	payload := []byte("STREAMING_BYTES_VERIFICATION")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPartialContent)
		w.Write(payload)
	}))
	defer server.Close()

	tempFile, _ := os.CreateTemp("", "chunk_tee_test_*")
	tempPath := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempPath)

	chunk := Chunk{
		Index:        0,
		Start:        0,
		End:          int64(len(payload) - 1),
		TempFilePath: tempPath,
	}

	var progressSpy bytes.Buffer
	err := DownloadChunk(server.URL, chunk, &progressSpy)
	if err != nil {
		t.Fatalf("DownloadChunk failed: %v", err)
	}

	if !bytes.Equal(progressSpy.Bytes(), payload) {
		t.Errorf("progress writer did not receive full payload: expected '%s', got '%s'",
			string(payload), progressSpy.String())
	}
}
