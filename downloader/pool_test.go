package downloader

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestWorkerPool_Success verifies that a pool of workers downloads all chunks concurrently
// into their respective temp files, correctly updating the progress tracker.
func TestWorkerPool_Success(t *testing.T) {
	payload := []byte("The quick brown fox jumps over the lazy dog. 1234567890! Concurrent Go streaming in action.")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if !strings.HasPrefix(rangeHeader, "bytes=") {
			t.Errorf("missing or invalid range header: %s", rangeHeader)
		}

		parts := strings.Split(strings.TrimPrefix(rangeHeader, "bytes="), "-")
		start, _ := strconv.Atoi(parts[0])
		end, _ := strconv.Atoi(parts[1])

		w.WriteHeader(http.StatusPartialContent)
		w.Write(payload[start : end+1])
	}))
	defer server.Close()

	totalSize := int64(len(payload))
	chunks := CalculateChunks(totalSize, 4)

	// Assign temp file paths to chunks
	for i := range chunks {
		f, err := os.CreateTemp("", fmt.Sprintf("pool_test_chunk_%d_*", i))
		if err != nil {
			t.Fatalf("failed to create temp file: %v", err)
		}
		chunks[i].TempFilePath = f.Name()
		f.Close()
		defer os.Remove(chunks[i].TempFilePath)
	}

	tracker := NewProgressTracker(totalSize)

	// Run with 2 concurrent workers handling 4 chunks
	err := RunWorkerPool(server.URL, chunks, 2, tracker)
	if err != nil {
		t.Fatalf("RunWorkerPool failed: %v", err)
	}

	// Verify all chunk files contain their expected slices
	for _, chunk := range chunks {
		data, err := os.ReadFile(chunk.TempFilePath)
		if err != nil {
			t.Fatalf("failed to read chunk %d: %v", chunk.Index, err)
		}
		expectedSlice := payload[chunk.Start : chunk.End+1]
		if !bytes.Equal(data, expectedSlice) {
			t.Errorf("chunk %d content mismatch: expected '%s', got '%s'",
				chunk.Index, string(expectedSlice), string(data))
		}
	}

	// Verify tracker totals
	if tracker.Downloaded() != totalSize {
		t.Errorf("tracker byte mismatch: expected %d, got %d", totalSize, tracker.Downloaded())
	}
	if tracker.Percent() != 100.0 {
		t.Errorf("tracker percent mismatch: expected 100%%, got %.1f%%", tracker.Percent())
	}
}

// TestWorkerPool_BoundedConcurrency proves that worker concurrency is strictly bounded
// to numWorkers and never exceeds the limit.
func TestWorkerPool_BoundedConcurrency(t *testing.T) {
	var currentActive atomic.Int32
	var maxObserved atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		active := currentActive.Add(1)
		// Update max observed concurrency
		for {
			curMax := maxObserved.Load()
			if active <= curMax || maxObserved.CompareAndSwap(curMax, active) {
				break
			}
		}

		// Artificial small delay to allow workers to overlap
		time.Sleep(20 * time.Millisecond)
		currentActive.Add(-1)

		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte("CHUNK_DATA"))
	}))
	defer server.Close()

	const numChunks = 8
	const maxWorkers = 3

	chunks := make([]Chunk, numChunks)
	for i := 0; i < numChunks; i++ {
		f, _ := os.CreateTemp("", "concurrency_test_*")
		chunks[i] = Chunk{
			Index:        i,
			Start:        int64(i * 10),
			End:          int64(i*10 + 9),
			TempFilePath: f.Name(),
		}
		f.Close()
		defer os.Remove(chunks[i].TempFilePath)
	}

	err := RunWorkerPool(server.URL, chunks, maxWorkers, nil)
	if err != nil {
		t.Fatalf("RunWorkerPool failed: %v", err)
	}

	if maxObserved.Load() > int32(maxWorkers) {
		t.Errorf("concurrency exceeded limit: max allowed %d, observed %d",
			maxWorkers, maxObserved.Load())
	}
}

// TestWorkerPool_ErrorHandling verifies that errors returned by any chunk download
// are returned by RunWorkerPool.
func TestWorkerPool_ErrorHandling(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	f, _ := os.CreateTemp("", "error_test_*")
	tempPath := f.Name()
	f.Close()
	defer os.Remove(tempPath)

	chunks := []Chunk{
		{Index: 0, Start: 0, End: 10, TempFilePath: tempPath},
	}

	err := RunWorkerPool(server.URL, chunks, 2, nil)
	if err == nil {
		t.Fatal("expected error from failed chunk download, got nil")
	}
}
