package downloader

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// Options specifies configuration parameters for the download process.
type Options struct {
	URL        string                         // Target URL to download (required)
	OutputFile string                         // Local destination path (optional: auto-derived if omitted)
	Workers    int                            // Maximum concurrent worker goroutines (default 4)
	Chunks     int                            // Total chunk slices to partition file into (default 4)
	RateLimit  int64                          // Maximum download speed in bytes/sec (0 = unlimited)
	OnProgress func(tracker *ProgressTracker) // Optional progress callback invoked during download
}

// Download coordinates the entire download workflow:
// 1. Probes the server for Content-Length and Range capability via HTTP HEAD.
// 2. Determines the local output filename.
// 3. If the server supports byte ranges, downloads concurrently across a worker pool
//    and stitches temporary parts with zero whole-file RAM usage.
// 4. If the server does NOT support byte ranges, gracefully falls back to a standard
//    single-stream download with progress tracking.
func Download(opts Options) error {
	// Guard clause: ensure valid URL
	if opts.URL == "" {
		return fmt.Errorf("URL is required")
	}

	// Apply default concurrency and chunk options
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.Chunks <= 0 {
		opts.Chunks = 4
	}

	// Step 1: Probe remote server metadata
	info, err := ProbeTarget(opts.URL)
	if err != nil {
		return fmt.Errorf("probe failed: %w", err)
	}

	// Step 2: Resolve final output filename
	outputPath := opts.OutputFile
	if outputPath == "" {
		outputPath = info.Filename
	}
	if outputPath == "" {
		outputPath = "downloaded_file"
	}

	// Step 3: Branch between Concurrent Chunking vs Single-Stream Fallback
	if info.RangeSupported && info.Size > 0 {
		return downloadConcurrent(opts, info, outputPath)
	}

	// Graceful fallback when the server does not support byte ranges
	fmt.Printf("[Downloader]: Target does not support Range requests. Falling back to single-stream download...\n")
	return downloadSingleStream(opts, info, outputPath)
}

// downloadConcurrent executes multi-threaded chunk downloading and stream stitching.
func downloadConcurrent(opts Options, info *TargetInfo, outputPath string) error {
	// Partition total file into chunks
	chunks := CalculateChunks(info.Size, opts.Chunks)

	// Assign deterministic temporary file paths for each chunk
	for i := range chunks {
		chunks[i].TempFilePath = fmt.Sprintf("%s.part%d", outputPath, chunks[i].Index)
	}

	tracker := NewProgressTracker(info.Size, opts.Workers)

	// Launch background progress reporter if callback provided
	stopReporter := startProgressReporter(tracker, opts.OnProgress)
	defer stopReporter()

	// Download all chunks concurrently across bounded worker pool
	limiter := NewRateLimiter(opts.RateLimit)
	err := RunWorkerPoolWithLimiter(opts.URL, chunks, opts.Workers, tracker, limiter)
	if err != nil {
		// Clean up any partially downloaded chunk files on failure
		for _, c := range chunks {
			os.Remove(c.TempFilePath)
		}
		return fmt.Errorf("concurrent download failed: %w", err)
	}

	// Assemble all chunk files into final output file and remove part files
	err = StitchChunks(chunks, outputPath)
	if err != nil {
		return fmt.Errorf("stitching failed: %w", err)
	}

	return nil
}

// downloadSingleStream downloads the entire payload over a single standard HTTP stream.
func downloadSingleStream(opts Options, info *TargetInfo, outputPath string) error {
	// http.Get(url):
	// - What it does: Issues an HTTP GET request using Go's default HTTP client.
	// - Returns: (*http.Response, error) -> Pointer to http.Response or error.
	resp, err := http.Get(opts.URL)
	if err != nil {
		return fmt.Errorf("GET request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("server responded with status: %s (%d)", resp.Status, resp.StatusCode)
	}

	// os.Create(name):
	// - What it does: Creates the local output file on disk.
	// - Returns: (*os.File, error) -> Open file descriptor or error.
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file %q: %w", outputPath, err)
	}
	defer outFile.Close()

	tracker := NewProgressTracker(info.Size, 1)

	stopReporter := startProgressReporter(tracker, opts.OnProgress)
	defer stopReporter()

	limiter := NewRateLimiter(opts.RateLimit)
	var src io.Reader = resp.Body
	if limiter != nil {
		src = NewThrottledReader(src, limiter)
	}
	if tracker != nil {
		var counter io.Writer = tracker
		if tracker.Worker(0) != nil {
			tracker.Worker(0).StartChunk(0, info.Size)
			counter = tracker.Worker(0)
		}
		// io.TeeReader(r, w):
		// - What it does: Duplicates network stream bytes to tracker on the fly.
		// - Returns: io.Reader -> Wrapped composite reader.
		src = io.TeeReader(resp.Body, counter)
	}

	// io.Copy(dst, src):
	// - What it does: Streams 32 KB at a time from network socket to disk.
	// - Returns: (int64, error) -> Bytes copied and error.
	_, err = io.Copy(outFile, src)
	if err != nil {
		return fmt.Errorf("streaming download failed: %w", err)
	}

	if tracker != nil {
		tracker.MarkChunkDone(0)
		if tracker.Worker(0) != nil {
			tracker.Worker(0).FinishChunk()
		}
	}

	return nil
}

// startProgressReporter starts a background goroutine ticking every 100ms
// to trigger the user's progress callback, returning a cancellation function.
func startProgressReporter(tracker *ProgressTracker, onProgress func(*ProgressTracker)) func() {
	if onProgress == nil {
		return func() {}
	}

	done := make(chan struct{})
	finished := make(chan struct{})

	go func() {
		// time.NewTicker(d):
		// - What it does: Creates a Ticker that delivers clock ticks on channel C every duration d.
		// - Returns: *time.Ticker.
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		defer close(finished)

		for {
			select {
			case <-ticker.C:
				onProgress(tracker)
			case <-done:
				// Final progress update upon completion
				onProgress(tracker)
				return
			}
		}
	}()

	return func() {
		// close(done):
		// - What it does: Signals the background ticker goroutine to exit.
		close(done)
		// Await clean exit of background goroutine to avoid terminal display interleaving
		<-finished
	}
}
