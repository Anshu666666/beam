package downloader

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// Worker status constants indicating lifecycle states
const (
	WorkerStatusIdle        int32 = 0
	WorkerStatusDownloading int32 = 1
	WorkerStatusDone        int32 = 2
)

// WorkerTracker provides thread-safe, lock-free progress tracking for an individual
// worker goroutine. It tracks its own chunk progress and concurrently forwards all
// streamed bytes to the master ProgressTracker.
type WorkerTracker struct {
	id              int
	currentChunk    atomic.Int64 // Current chunk index (-1 when idle/done)
	chunkTotal      atomic.Int64 // Total expected bytes for current chunk
	chunkDownloaded atomic.Int64 // Bytes downloaded for current chunk
	status          atomic.Int32 // WorkerStatusIdle, WorkerStatusDownloading, WorkerStatusDone
	master          *ProgressTracker
}

// NewWorkerTracker constructs an initialized WorkerTracker associated with a master tracker.
func NewWorkerTracker(id int, master *ProgressTracker) *WorkerTracker {
	w := &WorkerTracker{
		id:     id,
		master: master,
	}
	w.currentChunk.Store(-1)
	w.status.Store(WorkerStatusIdle)
	return w
}

// ID returns the worker identifier number.
func (w *WorkerTracker) ID() int {
	if w == nil {
		return -1
	}
	return w.id
}

// CurrentChunk returns the index of the chunk currently being downloaded (-1 if none).
func (w *WorkerTracker) CurrentChunk() int {
	if w == nil {
		return -1
	}
	return int(w.currentChunk.Load())
}

// ChunkTotal returns the total expected size of the current chunk in bytes.
func (w *WorkerTracker) ChunkTotal() int64 {
	if w == nil {
		return 0
	}
	return w.chunkTotal.Load()
}

// ChunkDownloaded returns the number of bytes downloaded for the current chunk.
func (w *WorkerTracker) ChunkDownloaded() int64 {
	if w == nil {
		return 0
	}
	return w.chunkDownloaded.Load()
}

// Status returns the current lifecycle status (Idle, Downloading, Done).
func (w *WorkerTracker) Status() int32 {
	if w == nil {
		return WorkerStatusIdle
	}
	return w.status.Load()
}

// StartChunk configures the worker tracker for a newly assigned chunk.
func (w *WorkerTracker) StartChunk(chunkIndex int, chunkSize int64) {
	if w == nil {
		return
	}
	w.chunkDownloaded.Store(0)
	w.chunkTotal.Store(chunkSize)
	w.currentChunk.Store(int64(chunkIndex))
	w.status.Store(WorkerStatusDownloading)
}

// FinishChunk marks the current chunk as completed.
func (w *WorkerTracker) FinishChunk() {
	if w == nil {
		return
	}
	w.status.Store(WorkerStatusDone)
}

// SetIdle marks the worker as idle (all jobs drained from queue).
func (w *WorkerTracker) SetIdle() {
	if w == nil {
		return
	}
	w.status.Store(WorkerStatusIdle)
}

// Percent calculates the percentage of the current chunk downloaded (0.0 to 100.0).
func (w *WorkerTracker) Percent() float64 {
	if w == nil {
		return 0.0
	}
	total := w.chunkTotal.Load()
	if total <= 0 {
		return 0.0
	}
	pct := (float64(w.chunkDownloaded.Load()) / float64(total)) * 100.0
	if pct > 100.0 {
		return 100.0
	}
	return pct
}

// Write implements the io.Writer interface.
// It simultaneously increments the worker's chunk byte count and the master tracker's
// global downloaded byte count using CPU hardware atomic instructions (lock-free).
func (w *WorkerTracker) Write(b []byte) (int, error) {
	if w == nil {
		return len(b), nil
	}
	n := int64(len(b))
	w.chunkDownloaded.Add(n)
	if w.master != nil {
		w.master.downloaded.Add(n)
	}
	return len(b), nil
}

// RenderBar produces a formatted terminal progress bar string for this specific worker.
func (w *WorkerTracker) RenderBar(barWidth int) string {
	if w == nil {
		return ""
	}

	st := w.Status()
	switch st {
	case WorkerStatusDownloading:
		pct := w.Percent()
		filled := int((pct / 100.0) * float64(barWidth))
		if filled > barWidth {
			filled = barWidth
		}
		if filled < 0 {
			filled = 0
		}
		empty := barWidth - filled
		// strings.Repeat(s, count):
		// - What it does: Returns a new string consisting of 'count' copies of the string 's'.
		// - Returns: string -> Repeated string.
		bar := strings.Repeat("=", filled) + strings.Repeat(" ", empty)
		// fmt.Sprintf(format, a...):
		// - What it does: Formats values into a string.
		// - Returns: string -> Formatted string.
		return fmt.Sprintf("Worker %d [Chunk #%d]: [%s] %5.1f%% (%s / %s)",
			w.id, w.CurrentChunk(), bar, pct, FormatBytes(w.ChunkDownloaded()), FormatBytes(w.ChunkTotal()))
	case WorkerStatusDone:
		bar := strings.Repeat("=", barWidth)
		return fmt.Sprintf("Worker %d [Chunk #%d]: [%s] 100.0%% (%s / %s) [DONE]",
			w.id, w.CurrentChunk(), bar, FormatBytes(w.ChunkTotal()), FormatBytes(w.ChunkTotal()))
	default:
		return fmt.Sprintf("Worker %d: [Idle / Complete]", w.id)
	}
}

// ProgressTracker provides thread-safe, lock-free progress and speed tracking
// for concurrent downloads. It implements the io.Writer interface so it can be
// plugged directly into io.TeeReader across multiple concurrent worker goroutines.
type ProgressTracker struct {
	totalSize  int64            // Expected total size of the file in bytes
	downloaded atomic.Int64     // Lock-free atomic accumulator for downloaded bytes
	startTime  time.Time        // Timestamp recorded when the tracker was created
	workers    []*WorkerTracker // Dedicated trackers for each worker goroutine
}

// NewProgressTracker constructs an initialized ProgressTracker with optional worker count.
// If numWorkers is provided and > 0, pre-allocates dedicated WorkerTrackers for each worker.
func NewProgressTracker(totalSize int64, numWorkers ...int) *ProgressTracker {
	p := &ProgressTracker{
		totalSize: totalSize,
		// time.Now():
		// - What it does: Returns the current local time with monotonic clock reading.
		// - Returns: time.Time -> Current timestamp.
		startTime: time.Now(),
	}

	count := 0
	if len(numWorkers) > 0 && numWorkers[0] > 0 {
		count = numWorkers[0]
	}

	if count > 0 {
		// make([]T, length):
		// - What it does: Allocates a slice with a specified length.
		// - Returns: []T.
		p.workers = make([]*WorkerTracker, count)
		for i := 0; i < count; i++ {
			p.workers[i] = NewWorkerTracker(i, p)
		}
	}

	return p
}

// Worker returns the WorkerTracker for the given worker ID, or nil if not found.
func (p *ProgressTracker) Worker(id int) *WorkerTracker {
	if p == nil || id < 0 || id >= len(p.workers) {
		return nil
	}
	return p.workers[id]
}

// Workers returns all worker trackers managed by this master tracker.
func (p *ProgressTracker) Workers() []*WorkerTracker {
	if p == nil {
		return nil
	}
	return p.workers
}

// Write implements the standard io.Writer interface.
// Multiple concurrent goroutines can call Write simultaneously because it uses
// atomic.Int64.Add, ensuring thread safety with zero mutex lock contention.
func (p *ProgressTracker) Write(b []byte) (int, error) {
	if p == nil {
		return len(b), nil
	}

	// atomic.Int64.Add(delta):
	// - What it does: Atomically adds delta to the counter using CPU hardware bus locking
	//   instructions (e.g. LOCK XADD on x86). Zero mutex lock overhead.
	// - Returns: int64 -> The new resulting value after addition.
	p.downloaded.Add(int64(len(b)))

	// As an io.Writer, we must return the number of bytes successfully accepted
	return len(b), nil
}

// Downloaded returns the total number of bytes read across all workers so far.
func (p *ProgressTracker) Downloaded() int64 {
	if p == nil {
		return 0
	}

	// atomic.Int64.Load():
	// - What it does: Atomically reads and returns the 64-bit integer value with memory barrier.
	// - Returns: int64 -> Current counter value.
	return p.downloaded.Load()
}

// TotalSize returns the total expected size of the download in bytes.
func (p *ProgressTracker) TotalSize() int64 {
	if p == nil {
		return 0
	}
	return p.totalSize
}

// Percent returns the download completion percentage from 0.0 to 100.0.
func (p *ProgressTracker) Percent() float64 {
	if p == nil || p.totalSize <= 0 {
		return 0.0
	}
	pct := (float64(p.Downloaded()) / float64(p.totalSize)) * 100.0
	if pct > 100.0 {
		return 100.0
	}
	return pct
}

// Speed returns the current average download speed in bytes per second.
func (p *ProgressTracker) Speed() float64 {
	if p == nil {
		return 0.0
	}
	// time.Since(t):
	// - What it does: Computes the elapsed duration from timestamp 't' until now.
	// - Returns: time.Duration -> Elapsed time interval.
	elapsed := time.Since(p.startTime).Seconds()
	if elapsed <= 0 {
		return 0.0
	}
	return float64(p.Downloaded()) / elapsed
}

// ETA calculates the estimated time remaining based on current transfer speed.
func (p *ProgressTracker) ETA() time.Duration {
	if p == nil {
		return 0
	}
	speed := p.Speed()
	if speed <= 0 {
		return 0
	}
	remainingBytes := p.totalSize - p.Downloaded()
	if remainingBytes <= 0 {
		return 0
	}

	secondsRemaining := float64(remainingBytes) / speed
	return time.Duration(secondsRemaining * float64(time.Second))
}

// FormatBytes converts a raw byte count into a human-readable string (B, KB, MB, GB).
func FormatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		// fmt.Sprintf(format, a...):
		// - What it does: Formats values into a string.
		// - Returns: string -> Formatted string.
		return fmt.Sprintf("%d B", b)
	}

	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	value := float64(b) / float64(div)
	units := []string{"KB", "MB", "GB", "TB"}
	return fmt.Sprintf("%.2f %s", value, units[exp])
}

// FormatSpeed formats bytes per second into human-readable throughput (e.g. "5.20 MB/s").
func FormatSpeed(bytesPerSec float64) string {
	return fmt.Sprintf("%s/s", FormatBytes(int64(bytesPerSec)))
}

// RenderBar produces a visual terminal progress bar string for the master tracker.
// Example: "[=========>          ] 45.2% | 45.20 MB / 100.00 MB | 5.20 MB/s | ETA: 10s"
func (p *ProgressTracker) RenderBar(barWidth int) string {
	if p == nil {
		return ""
	}
	pct := p.Percent()
	filled := int((pct / 100.0) * float64(barWidth))
	if filled > barWidth {
		filled = barWidth
	}
	if filled < 0 {
		filled = 0
	}

	empty := barWidth - filled
	bar := strings.Repeat("=", filled) + strings.Repeat(" ", empty)

	downloadedStr := FormatBytes(p.Downloaded())
	totalStr := FormatBytes(p.totalSize)
	speedStr := FormatSpeed(p.Speed())

	// Round(m):
	// - What it does: Returns duration rounded to nearest multiple of m.
	// - Returns: time.Duration.
	etaDuration := p.ETA().Round(time.Second)

	return fmt.Sprintf("[%s] %5.1f%% | %s / %s | %s | ETA: %v",
		bar, pct, downloadedStr, totalStr, speedStr, etaDuration)
}

// RenderDashboard produces a multi-line visual dashboard containing both the master
// progress bar and per-worker progress bars.
func (p *ProgressTracker) RenderDashboard(masterWidth int, workerWidth int) []string {
	if p == nil {
		return nil
	}

	var lines []string
	lines = append(lines, "[+] Master Progress:")
	lines = append(lines, "  "+p.RenderBar(masterWidth))

	if len(p.workers) > 0 {
		lines = append(lines, fmt.Sprintf("[+] Worker Status (%d workers):", len(p.workers)))
		for _, w := range p.workers {
			lines = append(lines, "  "+w.RenderBar(workerWidth))
		}
	}

	return lines
}
