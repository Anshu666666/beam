package downloader

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"
)

// TestProgressTracker_SinglePassStreaming tests using ProgressTracker as an io.Writer
// with io.TeeReader to verify single-pass byte counting without altering the stream.
func TestProgressTracker_SinglePassStreaming(t *testing.T) {
	totalSize := int64(100)
	tracker := NewProgressTracker(totalSize)

	payload := []byte("0123456789") // 10 bytes
	src := bytes.NewReader(payload)
	var dst bytes.Buffer

	// TeeReader duplicates every byte read from src to tracker
	tee := io.TeeReader(src, tracker)

	n, err := io.Copy(&dst, tee)
	if err != nil {
		t.Fatalf("io.Copy failed: %v", err)
	}

	if n != 10 {
		t.Errorf("expected 10 bytes copied, got %d", n)
	}
	if !bytes.Equal(dst.Bytes(), payload) {
		t.Errorf("destination stream corrupted: expected '%s', got '%s'", string(payload), dst.String())
	}
	if tracker.Downloaded() != 10 {
		t.Errorf("expected 10 bytes tracked, got %d", tracker.Downloaded())
	}
	if tracker.Percent() != 10.0 {
		t.Errorf("expected 10.0%%, got %.1f%%", tracker.Percent())
	}
}

// TestProgressTracker_ConcurrentWrites verifies that multiple goroutines (simulating
// multiple parallel download workers) can write to the shared tracker simultaneously
// with zero race conditions and 100% accurate byte counting.
func TestProgressTracker_ConcurrentWrites(t *testing.T) {
	const numWorkers = 8
	const bytesPerWorker = 1000
	const totalExpected = int64(numWorkers * bytesPerWorker)

	tracker := NewProgressTracker(totalExpected)

	var wg sync.WaitGroup
	dataChunk := make([]byte, 100) // 100-byte write slice

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < bytesPerWorker/len(dataChunk); i++ {
				n, err := tracker.Write(dataChunk)
				if err != nil || n != len(dataChunk) {
					t.Errorf("unexpected write error or short write: %v", err)
				}
			}
		}()
	}

	wg.Wait()

	if tracker.Downloaded() != totalExpected {
		t.Errorf("concurrent tally mismatch: expected %d bytes, got %d", totalExpected, tracker.Downloaded())
	}
	if tracker.Percent() != 100.0 {
		t.Errorf("expected 100.0%%, got %.1f%%", tracker.Percent())
	}
}

// TestProgressTracker_SpeedAndETA verifies speed and ETA calculations.
func TestProgressTracker_SpeedAndETA(t *testing.T) {
	totalSize := int64(1000)
	tracker := NewProgressTracker(totalSize)

	// Simulate downloading 500 bytes
	tracker.Write(make([]byte, 500))

	if tracker.Downloaded() != 500 {
		t.Errorf("expected 500 bytes downloaded, got %d", tracker.Downloaded())
	}
	if tracker.Percent() != 50.0 {
		t.Errorf("expected 50.0%%, got %.1f%%", tracker.Percent())
	}

	// Speed should be positive
	time.Sleep(5 * time.Millisecond)
	speed := tracker.Speed()
	if speed <= 0 {
		t.Errorf("expected positive speed, got %f", speed)
	}

	eta := tracker.ETA()
	if eta < 0 {
		t.Errorf("expected non-negative ETA, got %v", eta)
	}
}

// TestFormatBytes verifies human-readable byte unit string formatting.
func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{500, "500 B"},
		{1024, "1.00 KB"},
		{1048576, "1.00 MB"},
		{1073741824, "1.00 GB"},
	}

	for _, tt := range tests {
		actual := FormatBytes(tt.bytes)
		if actual != tt.expected {
			t.Errorf("FormatBytes(%d): expected '%s', got '%s'", tt.bytes, tt.expected, actual)
		}
	}
}

// TestWorkerTracker_LifecycleAndDualWriting verifies that writes to an individual
// WorkerTracker simultaneously update both the worker's chunk progress and the master tracker.
func TestWorkerTracker_LifecycleAndDualWriting(t *testing.T) {
	tracker := NewProgressTracker(1000, 2)
	if len(tracker.Workers()) != 2 {
		t.Fatalf("expected 2 workers, got %d", len(tracker.Workers()))
	}

	w0 := tracker.Worker(0)
	w1 := tracker.Worker(1)

	// Verify initial idle state
	if w0.Status() != WorkerStatusIdle || w1.Status() != WorkerStatusIdle {
		t.Fatalf("expected initial idle status")
	}

	// Start chunks
	w0.StartChunk(0, 500)
	w1.StartChunk(1, 500)

	if w0.Status() != WorkerStatusDownloading || w0.CurrentChunk() != 0 {
		t.Errorf("expected w0 downloading chunk 0")
	}

	// Write 200 bytes to worker 0
	w0.Write(make([]byte, 200))
	if w0.ChunkDownloaded() != 200 {
		t.Errorf("w0: expected 200 bytes, got %d", w0.ChunkDownloaded())
	}
	if w0.Percent() != 40.0 {
		t.Errorf("w0: expected 40%%, got %.1f%%", w0.Percent())
	}
	if tracker.Downloaded() != 200 {
		t.Errorf("master: expected 200 bytes, got %d", tracker.Downloaded())
	}

	// Write 300 bytes to worker 1
	w1.Write(make([]byte, 300))
	if w1.ChunkDownloaded() != 300 {
		t.Errorf("w1: expected 300 bytes, got %d", w1.ChunkDownloaded())
	}
	if tracker.Downloaded() != 500 {
		t.Errorf("master: expected 500 total bytes, got %d", tracker.Downloaded())
	}

	// Complete chunks
	w0.FinishChunk()
	if w0.Status() != WorkerStatusDone {
		t.Errorf("expected w0 done status")
	}

	// Test dashboard rendering
	dashboard := tracker.RenderDashboard(20, 15)
	if len(dashboard) < 4 { // Master title, Master bar, Workers title, 2 worker lines
		t.Errorf("expected at least 4 dashboard lines, got %d", len(dashboard))
	}
}

// TestWorkerTracker_ConcurrentWorkers verifies thread safety when multiple workers
// write concurrently to their respective WorkerTrackers.
func TestWorkerTracker_ConcurrentWorkers(t *testing.T) {
	const numWorkers = 4
	const bytesPerWorker = 2000
	const totalExpected = int64(numWorkers * bytesPerWorker)

	tracker := NewProgressTracker(totalExpected, numWorkers)
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			wt := tracker.Worker(workerID)
			wt.StartChunk(workerID, bytesPerWorker)

			data := make([]byte, 100)
			for i := 0; i < bytesPerWorker/len(data); i++ {
				wt.Write(data)
			}
			wt.FinishChunk()
		}(w)
	}

	wg.Wait()

	if tracker.Downloaded() != totalExpected {
		t.Errorf("master: expected %d bytes, got %d", totalExpected, tracker.Downloaded())
	}

	for w := 0; w < numWorkers; w++ {
		wt := tracker.Worker(w)
		if wt.ChunkDownloaded() != bytesPerWorker {
			t.Errorf("worker %d: expected %d bytes, got %d", w, bytesPerWorker, wt.ChunkDownloaded())
		}
		if wt.Status() != WorkerStatusDone {
			t.Errorf("worker %d: expected done status, got %d", w, wt.Status())
		}
	}
}

