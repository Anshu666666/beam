package downloader

import (
	"fmt"
	"io"
	"sync"
)

// RunWorkerPool dispatches chunk download jobs across a bounded pool of worker goroutines.
//
// Concurrency Architecture:
// 1. A buffered channel 'jobs' is loaded with all chunks and closed.
// 2. Exactly 'numWorkers' goroutines are spawned, all pulling from the shared 'jobs' channel.
// 3. Each worker downloads its chunk via DownloadChunk and reports completion or error
//    to a buffered 'results' channel.
// 4. A background goroutine waits on sync.WaitGroup to close 'results' once all workers finish.
// 5. If any worker reports an error, RunWorkerPool captures and returns the first error encountered.
func RunWorkerPool(targetURL string, chunks []Chunk, numWorkers int, tracker *ProgressTracker) error {
	// Guard clause: if no chunks exist, there is nothing to download
	if len(chunks) == 0 {
		return nil
	}

	// Guard clause: ensure at least 1 worker
	if numWorkers <= 0 {
		numWorkers = 1
	}

	// Guard clause: do not spawn more workers than total chunks available
	if numWorkers > len(chunks) {
		numWorkers = len(chunks)
	}

	// make(chan T, capacity):
	// - What it does: Allocates a buffered channel on the heap with capacity len(chunks).
	//   Using a buffered channel ensures that sending tasks or results will NEVER block
	//   or cause goroutine deadlocks, even if workers finish out of order.
	// - Returns: chan Chunk -> An initialized buffered channel.
	jobs := make(chan Chunk, len(chunks))
	results := make(chan error, len(chunks))

	// sync.WaitGroup:
	// - What it does: A synchronization primitive that counts down active goroutines.
	//   - wg.Add(delta): increments the counter by delta.
	//   - wg.Done(): decrements the counter by 1.
	//   - wg.Wait(): blocks until the counter drops to zero.
	var wg sync.WaitGroup

	// Launch bounded worker goroutines:
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)

		// Spawns worker goroutine
		go func(workerID int) {
			// defer wg.Done():
			// - What it does: Guarantees the waitgroup counter decrements when the worker exits.
			defer wg.Done()

			var workerTracker *WorkerTracker
			var workerWriter io.Writer
			if tracker != nil {
				workerTracker = tracker.Worker(workerID)
				if workerTracker != nil {
					workerWriter = workerTracker
				} else {
					workerWriter = tracker
				}
			}

			// 'for chunk := range jobs' continuously pulls jobs until the 'jobs' channel
			// is both closed AND drained of all items, then terminates naturally.
			for chunk := range jobs {
				if workerTracker != nil {
					workerTracker.StartChunk(chunk.Index, chunk.Size())
				}

				err := DownloadChunk(targetURL, chunk, workerWriter)
				if err != nil {
					results <- fmt.Errorf("worker %d failed chunk %d [%d-%d]: %w",
						workerID, chunk.Index, chunk.Start, chunk.End, err)
					continue
				}

				if workerTracker != nil {
					workerTracker.FinishChunk()
				}
				results <- nil
			}

			if workerTracker != nil {
				workerTracker.SetIdle()
			}
		}(w)
	}

	// Load all chunk jobs into the buffered channel:
	for _, chunk := range chunks {
		jobs <- chunk
	}

	// close(ch):
	// - What it does: Marks the 'jobs' channel as closed. Workers can still drain all
	//   existing items in the buffer, but range loops over 'jobs' will automatically
	//   terminate once the buffer is empty.
	close(jobs)

	// Launch background monitor to close 'results' when all workers exit:
	go func() {
		wg.Wait()
		close(results)
	}()

	// Drain all results from workers:
	var firstErr error
	for err := range results {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}
