# Project 1: Concurrent Chunk Downloader — Learning Guide

This document captures every architectural decision, networking mechanic, Go standard library detail, and runtime behavior learned while building the **Concurrent Chunk Downloader (`aria2` / `curl` Lite)**.

---

## Table of Contents
1. [Core Architecture & Mental Model](#1-core-architecture--mental-model)
2. [Task 1: The HTTP Metadata Probe](#2-task-1-the-http-metadata-probe)
   - [Why HEAD instead of GET?](#why-head-instead-of-get)
   - [Does Every Remote File Have Range Headers?](#does-every-remote-file-have-range-headers)
   - [RFC 7233: The Byte-Range Standard](#rfc-7233-the-byte-range-standard)
   - [Filename Resolution & Path Traversal Security](#filename-resolution--path-traversal-security)
   - [Testing HTTP Without the Internet (`net/http/httptest`)](#testing-http-without-the-internet-nethttphttptest)
3. [Task 2: Chunk Calculation & Byte Range Partitioning](#3-task-2-chunk-calculation--byte-range-partitioning)
   - [Inclusive HTTP Range Math: End - Start + 1](#inclusive-http-range-math-end---start--1)
   - [Handling Remainder Bytes (Zero Leakage)](#handling-remainder-bytes-zero-leakage)
   - [Memory Efficiency: Pre-allocating vs Dynamic Slices](#memory-efficiency-pre-allocating-vs-dynamic-slices)
   - [Defensive Clamping for Small Files](#defensive-clamping-for-small-files)
4. [Task 3: Single Chunk Downloader with HTTP Range](#4-task-3-single-chunk-downloader-with-http-range)
   - [Where Does TempFilePath Come From?](#where-does-tempfilepath-come-from)
   - [The Range Header Syntax & 206 Partial Content Contract](#the-range-header-syntax--206-partial-content-contract)
   - [Why We Must Reject 200 OK on Range Requests](#why-we-must-reject-200-ok-on-range-requests)
   - [Constant 32 KB RAM Streaming with io.Copy](#constant-32-kb-ram-streaming-with-iocopy)
5. [Task 4: Dynamic Progress Tracking with io.TeeReader](#5-task-4-dynamic-progress-tracking-with-ioteereader)
   - [The Mechanics of http.Client.Do: Headers vs Streaming Body](#the-mechanics-of-httpclientdo-headers-vs-streaming-body)
   - [Why Workers Share a Unified Atomic Counter](#why-workers-share-a-unified-atomic-counter)
   - [Lock-Free Concurrency with sync/atomic.Int64](#lock-free-concurrency-with-syncatomicint64)
   - [Dynamic Speed & ETA Calculations](#dynamic-speed--eta-calculations)
6. [Task 5: Bounded Worker Pool for Concurrent Chunk Downloading](#6-task-5-bounded-worker-pool-for-concurrent-chunk-downloading)
   - [Atomics vs Mutexes: Why Do We Still Need Mutexes?](#atomics-vs-mutexes-why-do-we-still-need-mutexes)
   - [Bounded Worker Pool Mechanics](#bounded-worker-pool-mechanics)
   - [Graceful Channel Termination: The Range Loop & Closer Pattern](#graceful-channel-termination-the-range-loop--closer-pattern)
   - [The Infamous Go Gotcha: Typed Nil Pointer in an Interface](#the-infamous-go-gotcha-typed-nil-pointer-in-an-interface)
7. [Task 6: Zero-RAM Stream Stitcher](#7-task-6-zero-ram-stream-stitcher)
   - [How io.MultiReader Works Under the Hood](#how-iomultireader-works-under-the-hood)
   - [Zero-RAM File Assembly with io.Copy](#zero-ram-file-assembly-with-iocopy)
   - [OS File Handles & The Windows File Locking Gotcha](#os-file-handles--the-windows-file-locking-gotcha)
8. [Task 7: Engine Orchestrator & Graceful Fallback](#8-task-7-engine-orchestrator--graceful-fallback)
   - [The Facade Pattern in Go](#the-facade-pattern-in-go)
   - [Graceful Fallback Strategy](#graceful-fallback-strategy)
   - [Asynchronous Progress Reporting with time.Ticker & Cancellation Channels](#asynchronous-progress-reporting-with-timeticker--cancellation-channels)
   - [Cryptographic Verification with crypto/sha256](#cryptographic-verification-with-cryptosha256)
9. [Task 8: CLI Entry Point & User Experience](#9-task-8-cli-entry-point--user-experience)
   - [Decoupling run() from main() for Unit Testability](#decoupling-run-from-main-for-unit-testability)
   - [How flag.FlagSet Isolates Command Parsing](#how-flagflagset-isolates-command-parsing)
   - [Terminal UI: In-Place Refresh with Carriage Return (\r)](#terminal-ui-in-place-refresh-with-carriage-return-r)
   - [Hierarchical Lock-Free Worker Tracking & Multi-Line ANSI Terminal Redrawing](#hierarchical-lock-free-worker-tracking--multi-line-ansi-terminal-redrawing)
   - [Bidirectional Goroutine Teardown Synchronization](#bidirectional-goroutine-teardown-synchronization)
10. [Task 9: Real-Time Web Telemetry with Server-Sent Events (SSE) & Virtual Byte Ranges](#10-task-9-real-time-web-telemetry-with-server-sent-events-sse--virtual-byte-ranges)
    - [SSE vs WebSockets vs gRPC: Why SSE Wins for Telemetry](#sse-vs-websockets-vs-grpc-why-sse-wins-for-telemetry)
    - [Synthetic RFC 7233 Byte-Range Serving (Zero-RAM Virtual File Seeker)](#synthetic-rfc-7233-byte-range-serving-zero-ram-virtual-file-seeker)
    - [Server-Sent Events Protocol & http.Flusher Mechanics](#server-sent-events-protocol--httpflusher-mechanics)
    - [Consuming SSE in Modern JavaScript with Native EventSource](#consuming-sse-in-modern-javascript-with-native-eventsource)
    - [Aesthetic & Design Philosophy (Linear/OLED Luxury UI)](#aesthetic--design-philosophy-linearoled-luxury-ui)
11. [Go Standard Library Function Dictionary](#11-go-standard-library-function-dictionary)
12. [Gotchas & Best Practices Log](#12-gotchas--best-practices-log)


---

## 1. Core Architecture & Mental Model

A standard downloader (`curl`, web browsers) establishes a single TCP socket connection. Bandwidth is bottlenecked by per-connection throughput limits, latency, and single-stream TCP window scaling.

A concurrent chunk downloader overcomes this by:
1. **Probing metadata**: Inquires total file size and byte-range slicing capabilities via HTTP `HEAD`.
2. **Partitioning**: Slices the total byte span $[0, \text{Size}-1]$ into $N$ non-overlapping intervals.
3. **Bounded Concurrency**: Spawns $W$ worker goroutines (Topic 2 Worker Pool) reading from a `chan Chunk`.
4. **Stream Splitting**: Workers pipe `206 Partial Content` streams through `io.TeeReader` (Topic 5) to tally live progress and speed without double memory/disk passes.
5. **Zero-RAM Stitching**: Chains chunk files on disk via `io.MultiReader` into an `io.Copy` stream with a fixed 32 KB buffer.

```
                    [ Remote Server (HTTP) ]
                               │
            ┌──────────────────┼──────────────────┐
     Range: 0-24MB      Range: 25-49MB     Range: 50-74MB   ...
            │                  │                  │
       [Worker 1]         [Worker 2]         [Worker 3]
            │                  │                  │
        TeeReader          TeeReader          TeeReader
       ┌────┴────┐        ┌────┴────┐        ┌────┴────┐
       ▼         ▼        ▼         ▼        ▼         ▼
  [Progress] [part_0] [Progress] [part_1] [Progress] [part_2]
       │                  │                  │
       └──────────────────┴──────────────────┘
                          │
               io.MultiReader(part_0, part_1, part_2...)
                          │
                   io.Copy (32 KB buffer)
                          ▼
                  [ Final Output File ]
```

---

## 2. Task 1: The HTTP Metadata Probe

### Why HEAD instead of GET?
* `GET https://example.com/bigfile.iso`: Instructs the server to transmit the entire file body immediately over the socket.
* `HEAD https://example.com/bigfile.iso`: Instructs the server to return **only the HTTP response headers** and immediately close/end the response without sending a single byte of body payload.
* **Result**: We discover file size, range support, and filename in under 1 KB of network transfer.

---

### Does Every Remote File Have Range Headers?
**No!** Whether a server returns `Content-Length` and `Accept-Ranges: bytes` depends on how the remote server serves the content:

| Scenario | Server Type | `Content-Length` | `Accept-Ranges: bytes` | Can We Chunk? |
| :--- | :--- | :--- | :--- | :--- |
| **Static Files** (S3, Cloudflare, Nginx, Apache) | `.iso`, `.zip`, `.mp4` on disk | ✅ Yes (Exact bytes) | ✅ Yes (`bytes`) | **Yes (Full Concurrency)** |
| **Dynamic Generation** (API exports, live reports) | `Content-Type: text/csv` generated on the fly | ❌ No (`Transfer-Encoding: chunked`) | ❌ No | **No (Fallback to 1 stream)** |
| **Media Streams** (Webcam, Live Audio, HLS) | Continuous socket stream | ❌ No | ❌ No | **No (Infinite stream)** |
| **Legacy / Minimal Microservices** | Simple custom HTTP handlers | May have size | Omitted / `none` | **No (Fallback to 1 stream)** |

> [!NOTE]
> This is why our downloader includes a **Probe Phase** first! If `Accept-Ranges: bytes` is missing or `Size <= 0`, our engine gracefully falls back to single-stream download instead of crashing.

---

### RFC 7233: The Byte-Range Standard
RFC 7233 is the official HTTP standard for range requests:
1. Server advertises capability: `Accept-Ranges: bytes`
2. Client requests range: `Range: bytes=0-1048575` (inclusive byte indices)
3. Server responds with:
   - Status code: `206 Partial Content` (NOT `200 OK`)
   - Header: `Content-Range: bytes 0-1048575/5242880` (start-end/total)

---

### Filename Resolution & Path Traversal Security
When downloading a file, where does the filename come from?

1. **`Content-Disposition` Header**:
   - Format: `attachment; filename="setup.exe"`
   - Parsed with `mime.ParseMediaType(cd)`.
2. **URL Path Base**:
   - `https://example.com/releases/v1.0/app.tar.gz` $\rightarrow$ `app.tar.gz`
   - Parsed with `path.Base(path.Clean(u.Path))`.
3. **Path Traversal Security Defense**:
   - A malicious server could return: `attachment; filename="../../Windows/System32/evil.dll"`
   - `path.Base()` strips all parent directory sequences (`../../`), leaving only `"evil.dll"` saved in the current directory.

---

### Testing HTTP Without the Internet (`net/http/httptest`)
In Go, you never need to mock HTTP by monkey-patching sockets or relying on third-party libraries.
* `httptest.NewServer(handler)` starts a real, in-process HTTP server running on a local loopback IP (`127.0.0.1`) with a free ephemeral port.
* Fast (executes in 5 milliseconds), deterministic, and runs in isolated offline CI/CD pipelines.

---

## 3. Task 2: Chunk Calculation & Byte Range Partitioning

### Inclusive HTTP Range Math: End - Start + 1
In the HTTP Range standard, byte positions are **0-indexed and inclusive on both sides**:
* `Range: bytes=0-0` $\rightarrow$ Requests 1 byte (byte 0). Length = $0 - 0 + 1 = 1$.
* `Range: bytes=0-9` $\rightarrow$ Requests 10 bytes (bytes 0, 1, 2, 3, 4, 5, 6, 7, 8, 9). Length = $9 - 0 + 1 = 10$.
* Therefore, the size formula on `Chunk` is:
  $$\text{Size} = \text{End} - \text{Start} + 1$$

---

### Handling Remainder Bytes (Zero Leakage)
When dividing a file size by the chunk count using integer division:
$$\text{chunkSize} = \frac{\text{totalSize}}{\text{numChunks}}$$
There is often a non-zero remainder ($\text{totalSize} \pmod{\text{numChunks}}$).
For example, a **100-byte file** split into **3 chunks**:
* $100 / 3 = 33$ (integer division drops the fraction).
* If we made all chunks 33 bytes:
  - Chunk 0: $[0, 32]$ ($33$ bytes)
  - Chunk 1: $[33, 65]$ ($33$ bytes)
  - Chunk 2: $[66, 98]$ ($33$ bytes)
  - **Result: Total bytes covered = 99. Byte 99 is MISSING! The file would be corrupted.**
* **The Solution**: On the final chunk ($i = \text{numChunks} - 1$), we set:
  $$\text{end} = \text{totalSize} - 1$$
  - Chunk 2 becomes: $[66, 99]$ ($34$ bytes).
  - $33 + 33 + 34 = 100$ bytes. Zero gaps, zero overlaps, 100% byte integrity!

---

### Memory Efficiency: Pre-allocating vs Dynamic Slices
In Go (from Topic 3 & Topic 5), dynamically appending to a slice (`append()`) causes reallocation when `len == cap`. The Go runtime allocates a new array (often double the capacity) and copies the elements over.

Because we know `numChunks` upfront, we use:
```go
chunks := make([]Chunk, numChunks)
```
* **Heap allocations**: Exactly 1.
* **Array copies**: 0.
* Direct indexing `chunks[i] = ...` directly populates the pre-allocated contiguous memory.

---

### Defensive Clamping for Small Files
What if a user asks for 10 chunks, but the file is only 4 bytes long?
If we blindly created 10 chunks, chunks 4 through 9 would have inverted or zero ranges ($start > end$).
We clamp:
```go
if totalSize < int64(numChunks) {
    numChunks = int(totalSize)
}
```
If the file is 4 bytes, we create 4 chunks of 1 byte each ($[0,0], [1,1], [2,2], [3,3]$), guaranteeing clean execution.

---

## 4. Task 3: Single Chunk Downloader with HTTP Range

### Where Does `TempFilePath` Come From?
In `CalculateChunks`, we only calculate the mathematical byte boundaries (`Index`, `Start`, `End`). `TempFilePath` is initialized to its empty string zero-value `""`.

`TempFilePath` is assigned **right before downloading begins** by the orchestration layer. There are two standard strategies:
1. **Deterministic naming (Production/CLI approach)**:
   ```go
   chunk.TempFilePath = fmt.Sprintf("%s.part%d", outputPath, chunk.Index)
   ```
   * Example: Downloading `ubuntu.iso` creates `ubuntu.iso.part0`, `ubuntu.iso.part1`, `ubuntu.iso.part2`.
   * **Advantage**: If a download is paused or interrupted, you know exactly which chunks finished on disk, enabling resume functionality!
2. **OS Temp Files (Ephemeral approach)**:
   ```go
   f, _ := os.CreateTemp("", "chunk_*")
   chunk.TempFilePath = f.Name()
   ```
   * Used in unit tests so temporary files never collide or pollute the project folder.

---

### The Range Header Syntax & 206 Partial Content Contract
To fetch a partial slice of a file, we set the HTTP `Range` request header:
```http
GET /large-video.mp4 HTTP/1.1
Host: cdn.example.com
Range: bytes=1048576-2097151
```
The server MUST return:
* **HTTP Status 206 Partial Content** (RFC 7233).
* **`Content-Range: bytes 1048576-2097151/52428800`** header.

---

### Why We Must Reject 200 OK on Range Requests
If a server returns `200 OK` in response to a `Range: bytes=...` request, it means:
> *"I do NOT support range slicing or I ignored your header, so here is the entire 10 GB file from byte 0!"*

If our chunk worker accepted `200 OK`, every single one of our 4 parallel workers would download the entire 10 GB file into its own chunk file, corrupting the final output and wasting bandwidth.
Therefore, `DownloadChunk` strictly enforces:
```go
if resp.StatusCode != http.StatusPartialContent {
    return fmt.Errorf("server did not honor range (expected 206, got %s)", resp.Status)
}
```

---

### Constant 32 KB RAM Streaming with `io.Copy`
Instead of reading the entire chunk into a `[]byte` in memory, we stream directly from the network socket (`resp.Body`) to the disk file descriptor (`os.File`):
```go
_, err = io.Copy(outFile, src)
```
* Under the hood, `io.Copy` allocates a fixed **32 KB buffer**.
* It reads 32 KB from the network socket $\rightarrow$ writes 32 KB to disk $\rightarrow$ repeats until EOF.
* **RAM footprint:** Capped at ~32 KB per active worker, whether downloading a 10 MB audio file or a 50 GB database dump.

---

## 5. Task 4: Dynamic Progress Tracking with `io.TeeReader`

### The Mechanics of `http.Client.Do`: Headers vs Streaming Body
A common beginner misconception is assuming `resp, err := client.Do(req)` downloads the entire response payload into `resp`.
**It does NOT!**
* `client.Do(req)` connects the TCP socket, performs the TLS handshake, sends the HTTP request headers, and waits **only until the server sends the HTTP response headers** (`Status`, `Content-Length`, `Content-Type`).
* As soon as headers arrive, `client.Do()` returns immediately!
* The body bytes are **still sitting on the remote server across the TCP socket**.
* `resp.Body` is an open live network stream (`io.ReadCloser`).
* Not a single byte of body payload is stored in RAM until **you** call `.Read()` (or `io.Copy`).
* This is why `io.Reader` is a superpower: whether the file is 5 MB or 50 GB, `client.Do` uses almost zero RAM and completes in milliseconds!

---

### Why Workers Share a Unified Atomic Counter
When 4 workers download chunks concurrently:
* Worker 0 streams Chunk 0 (`ubuntu.iso.part0`)
* Worker 1 streams Chunk 1 (`ubuntu.iso.part1`)
* Worker 2 streams Chunk 2 (`ubuntu.iso.part2`)
* Worker 3 streams Chunk 3 (`ubuntu.iso.part3`)

In the terminal CLI UI, the user wants a single unified progress bar:
`[=======>    ] 58.3% | 58.30 MB / 100.00 MB | 12.40 MB/s | ETA: 3s`

If each worker had an isolated counter, the main program would have to poll each worker, lock multiple state structs, and sum them up every render tick.
Instead, all workers share **one `*ProgressTracker`**:
1. When Worker 0 reads a 32 KB chunk from the socket, `io.TeeReader` calls `tracker.Write(chunk)`.
2. `tracker.downloaded.Add(32768)` increments the global byte counter.
3. Simultaneously, Worker 1 reads 32 KB and calls `tracker.downloaded.Add(32768)`.
4. At any microsecond, `tracker.Downloaded()` reflects the exact total bytes fetched across all connections!

---

### Lock-Free Concurrency with `sync/atomic.Int64`
Why use `atomic.Int64` instead of a `sync.Mutex`?
* A `sync.Mutex` puts the operating system thread to sleep (context switch) if another goroutine is currently holding the lock.
* On high-speed networks transferring hundreds of megabytes per second, thousands of 32 KB writes happen every second. Lock contention would degrade CPU throughput.
* `atomic.Int64.Add()` compiles directly into a single hardware CPU instruction:
  - On x86/x64: `LOCK XADD`
  - On ARM64: `LDADD`
* **Zero OS context switches, zero thread sleep, lock-free performance.**

---

### Dynamic Speed & ETA Calculations
1. **Transfer Speed (Throughput)**:
   $$\text{Speed} = \frac{\text{Bytes Downloaded}}{\text{Elapsed Seconds since start}}$$
2. **Estimated Time of Arrival (ETA)**:
   $$\text{Remaining Bytes} = \text{Total Size} - \text{Bytes Downloaded}$$
   $$\text{ETA (seconds)} = \frac{\text{Remaining Bytes}}{\text{Speed}}$$

---

## 6. Task 5: Bounded Worker Pool for Concurrent Chunk Downloading

### Atomics vs Mutexes: Why Do We Still Need Mutexes?
If atomic operations are lock-free and super fast, why do we need `sync.Mutex` or `sync.RWMutex` at all?

| Capability | `sync/atomic` (Lock-Free) | `sync.Mutex` / `sync.RWMutex` (Locks) |
| :--- | :--- | :--- |
| **Protected Data** | Single 64-bit primitive word (`int64`, pointer, `bool`) | Complex data structures (`map`, `slice`, tree, struct) |
| **Operation Type** | Single-step hardware instruction (`Add`, `Load`, `Store`, `CAS`) | Multi-step blocks of arbitrary Go code |
| **"Check-Then-Act"** | ❌ Cannot group multiple lines atomically | ✅ Protects multi-step invariants (`if len(q) > 0 { pop() }`) |
| **Map Protection** | ❌ Impossible to protect a Go `map` | ✅ Essential for thread-safe maps (`Topic 3`) |
| **Overhead** | Nanosecond-level CPU register operations | Microsecond-level OS thread descheduling on contention |

**Rule of Thumb:**
* Use **`sync/atomic`** for: simple independent counters, metrics, status flags, and rates.
* Use **`sync.Mutex`** for: complex state, shared maps/slices, and multi-variable business invariants (like bank account transfers).

---

### Bounded Worker Pool Mechanics
The Bounded Worker Pool pattern decouples the **number of jobs** from the **number of concurrent goroutines**:
* If a file is sliced into **100 chunks**, spawning 100 simultaneous network connections can exhaust local socket descriptors, trigger CDN rate-limits, or choke disk I/O.
* Instead, we configure a fixed worker count (e.g. `numWorkers = 4`).
* All 4 workers pull from a single shared incoming channel: `jobs := make(chan Chunk, len(chunks))`.
* As soon as Worker 1 finishes Chunk 0, it does not die—it immediately grabs the next available chunk from `jobs`.

---

### Graceful Channel Termination: The Range Loop & Closer Pattern
How do worker goroutines know when all work is finished?
1. Before workers run, we load all chunks into the buffered `jobs` channel:
   ```go
   for _, c := range chunks { jobs <- c }
   close(jobs)
   ```
2. In Go, closing a channel does **NOT** discard buffered data. Workers can still read every remaining item in the buffer.
3. The worker loop:
   ```go
   for chunk := range jobs { ... }
   ```
   automatically reads until the buffer is empty, and then **exits the loop cleanly**.
4. To safely collect results without deadlocking:
   ```go
   go func() {
       wg.Wait()      // Waits for all workers to finish
       close(results) // Tells main thread all results are gathered
   }()
   ```

---

### The Infamous Go Gotcha: Typed Nil Pointer in an Interface ⚠️
In Go, an interface value is a two-word pair under the hood:
```text
Interface Header: [ (Type Pointer), (Value Pointer) ]
```
An interface is only `nil` if **BOTH** the type pointer and value pointer are `nil`:
```go
var p *ProgressTracker = nil // Concrete type pointer is *ProgressTracker, value is nil
var w io.Writer = p          // Interface is: [ Type: *ProgressTracker, Value: nil ]

if w == nil { // FALSE! The interface has a non-nil Type pointer!
}
```
If you pass a `nil` pointer into an `io.Writer` parameter, `if counter != nil` evaluates to `true`! When `counter.Write()` is called, it invokes the method on a `nil` receiver, leading to a panic:
`runtime error: invalid memory address or nil pointer dereference`.

**The Defenses:**
1. In caller code, pass untyped `nil`:
   ```go
   var counter io.Writer
   if tracker != nil {
       counter = tracker
   }
   ```
2. In receiver methods, write defensive guards:
   ```go
   func (p *ProgressTracker) Write(b []byte) (int, error) {
       if p == nil {
           return len(b), nil
       }
       ...
   }
   ```

---

## 7. Task 6: Zero-RAM Stream Stitcher

### How `io.MultiReader` Works Under the Hood
In Topic 5, you learned about `io.MultiReader`. How does it combine files without allocating memory?
```go
stream := io.MultiReader(r1, r2, r3)
```
Inside Go's standard library `io` package:
```go
type multiReader struct {
    readers []Reader
}

func (mr *multiReader) Read(p []byte) (n int, err error) {
    for len(mr.readers) > 0 {
        n, err = mr.readers[0].Read(p)
        if err == EOF {
            mr.readers = mr.readers[1:] // Shift forward to the next reader!
        }
        if n > 0 || err != EOF {
            return
        }
    }
    return 0, EOF
}
```
* It maintains a slice of `Reader` contracts.
* When `r1` reaches `io.EOF`, it drops `r1` from the list and immediately begins reading `r2` into the caller's buffer `p`.
* **Zero byte copying in memory! No concatenation of byte slices!**

---

### Zero-RAM File Assembly with `io.Copy`
By combining `io.MultiReader` with `io.Copy`:
```go
_, err := io.Copy(destFile, combinedMultiReader)
```
1. `io.Copy` allocates a single **32 KB buffer**.
2. Reads 32 KB from `ubuntu.iso.part0` $\rightarrow$ writes to `ubuntu.iso`.
3. Reads 32 KB from `ubuntu.iso.part1` $\rightarrow$ writes to `ubuntu.iso`.
4. Repeats smoothly until the entire multi-gigabyte file is stitched together on disk.
5. **Memory overhead:** Fixed at ~32 KB!

---

### OS File Handles & The Windows File Locking Gotcha ⚠️
On Unix systems (Linux, macOS), you can delete a file from the filesystem even if a process currently holds an open file descriptor (`unlink()` removes the directory entry while the file remains alive until descriptors close).

**On Windows, file locking is strict!**
If an `*os.File` is still open, calling `os.Remove(filePath)` fails immediately with:
`os.PathError: Access is denied` (or `The process cannot access the file because it is being used by another process`).

**The Rule:**
In `StitchChunks`, we must explicitly call `f.Close()` on all chunk file handles **before** attempting to delete them with `os.Remove(chunk.TempFilePath)`.

---

## 8. Task 7: Engine Orchestrator & Graceful Fallback

### The Facade Pattern in Go
Rather than requiring callers to manually coordinate:
1. `ProbeTarget`
2. `CalculateChunks`
3. Temp file naming
4. `RunWorkerPool`
5. `StitchChunks`
We expose a clean, idiomatic **Facade**:
```go
err := downloader.Download(downloader.Options{
    URL:     "https://example.com/file.zip",
    Workers: 4,
})
```
All internal state machines and complexity are cleanly encapsulated.

---

### Graceful Fallback Strategy
Production tools can never assume every endpoint supports HTTP Range slicing:
* If `info.RangeSupported && info.Size > 0` $\rightarrow$ High-throughput concurrent worker pool download.
* If `!info.RangeSupported || info.Size <= 0` $\rightarrow$ Graceful fallback to single-stream download via `http.Get`.
The end user gets their file regardless of server configuration without crashes or corrupted output.

---

### Asynchronous Progress Reporting with `time.Ticker` & Cancellation Channels
To update the terminal UI smoothly without blocking worker goroutines:
1. `time.NewTicker(100 * time.Millisecond)` fires 10 times per second.
2. A background goroutine multiplexes between the timer and a `done` channel:
   ```go
   select {
   case <-ticker.C:
       onProgress(tracker)
   case <-done:
       onProgress(tracker) // Final 100% update
       return
   }
   ```
3. Calling `close(done)` cancels the ticker loop gracefully when download completes.

---

### Cryptographic Verification with `crypto/sha256`
In concurrent chunk downloading, the biggest engineering risk is silent file corruption:
* An off-by-one error in byte ranges.
* Missing remainder bytes.
* Chunks assembled in the wrong order.
To definitively verify zero corruption, we generate a known random byte payload, compute its SHA-256 hash before downloading, and verify that the reassembled file on disk computes the exact identical SHA-256 digest.

---

## 9. Task 8: CLI Entry Point & User Experience

### Decoupling `run()` from `main()` for Unit Testability
A common pitfall in Go CLI applications is putting all argument parsing, printing, and `os.Exit(1)` calls directly inside `func main()`. Because `os.Exit` immediately terminates the entire OS process without running deferred functions, `func main()` cannot be tested from `go test` without killing the test runner!

By decoupling `main()` into a thin wrapper:
```go
func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "\n[Error]: %v\n", err)
		os.Exit(1)
	}
}
```
`run(args []string) error` becomes a pure, 100% unit-testable function. Tests can pass arbitrary command-line flag slices, verify flag validation errors, check required flags, and run end-to-end downloads against local `httptest.Server` instances safely.

---

### How `flag.FlagSet` Isolates Command Parsing
The standard library `flag` package offers package-level functions like `flag.StringVar` and `flag.Parse()`. However, these operate on a global shared instance (`flag.CommandLine`). In tests, if multiple tests invoke `flag.Parse()`, the second test will panic or inherit dirty flags from the first.

Using `flag.NewFlagSet("chunkdown", flag.ContinueOnError)` creates an isolated, local flag registry per invocation:
* Flags belong strictly to that scope.
* Setting `flag.ContinueOnError` ensures that when an invalid flag or `-h` is supplied, `fs.Parse()` returns an error value instead of terminating the process with `os.Exit(2)`.

---

### Terminal UI: In-Place Refresh with Carriage Return (`\r`)
When printing real-time progress updates in terminal consoles:
* Newline (`\n`): Advances the terminal cursor down to the next row, causing long scrolling tails.
* Carriage Return (`\r`): Moves the cursor back to the **first column (column 0) of the current line** without moving down.

By writing `\r` before the formatted progress bar string:
```go
fmt.Printf("\r%s", tracker.RenderBar(20))
```
the terminal continually redraws the progress bar over itself in-place:
```
[==========>         ]  50.0% | 50.00 MB / 100.00 MB | 5.00 MB/s | ETA: 10s
```
Once the download finishes, printing a single newline (`fmt.Println()`) parks the cursor on the next line so summary statistics do not overwrite the completed progress bar.

---

### Hierarchical Lock-Free Worker Tracking & Multi-Line ANSI Terminal Redrawing

When downloading across multiple concurrent workers, a single aggregated progress bar hides which workers are actively pulling, which chunks are finishing, and whether specific workers are stalling.

#### 1. The Dual-Update WorkerTracker
We introduced a dedicated `WorkerTracker` for each worker goroutine. It tracks:
* `currentChunk atomic.Int64`: The chunk index actively being fetched.
* `chunkTotal atomic.Int64`: Total bytes in that chunk (`chunk.Size()`).
* `chunkDownloaded atomic.Int64`: Bytes streamed for this specific chunk.
* `status atomic.Int32`: `WorkerStatusIdle`, `WorkerStatusDownloading`, or `WorkerStatusDone`.

When a worker's `io.TeeReader` intercepts response stream bytes, `workerTracker.Write(b)` executes a lock-free dual increment:
```go
func (w *WorkerTracker) Write(b []byte) (int, error) {
    n := int64(len(b))
    w.chunkDownloaded.Add(n)   // 1. Worker chunk counter
    if w.master != nil {
        w.master.downloaded.Add(n) // 2. Global aggregate counter
    }
    return len(b), nil
}
```
Both operations execute CPU hardware atomic instructions (`sync/atomic.Int64.Add`). There is **zero mutex contention**, allowing all $N$ workers to write simultaneously at wire speed.

#### 2. Multi-Line ANSI Terminal Redrawing
A single `\r` carriage return can only overwrite the current line. To redraw $M$ lines (Master bar + headers + $N$ worker bars):
1. **Cursor Up**: `\033[%dA` jumps the cursor up by $M$ rows back to line 0.
2. **Clear & Print**: For each row, `\r\033[2K` clears the entire terminal row and prints the updated status line followed by `\n`.
3. **Windows Virtual Terminal Processing**: On Windows consoles, ANSI escape codes are activated via `kernel32.dll`'s `SetConsoleMode` with `ENABLE_VIRTUAL_TERMINAL_PROCESSING` (0x0004), ensuring flicker-free multi-line rendering on Windows PowerShell and CMD.

---

### Bidirectional Goroutine Teardown Synchronization
When running asynchronous background tasks (like our 100ms progress reporting ticker), a single unbuffered signaling channel `close(done)` is **unidirectional**.
If `main` initiates teardown by closing `done` and immediately prints the completion banner, the background goroutine may wake up on `<-done` a few milliseconds *afterwards* and write a final progress bar to `stdout`, corrupting the terminal output display!

To eliminate this race condition, we implement a **bidirectional handshaking pattern**:
```go
done := make(chan struct{})
finished := make(chan struct{})

go func() {
	defer close(finished) // Signal caller that goroutine has cleanly terminated
	for {
		select {
		case <-ticker.C:
			onProgress(tracker)
		case <-done:
			onProgress(tracker) // Guaranteed final 100% update
			return
		}
	}
}()

return func() {
	close(done) // 1. Signal background goroutine to stop
	<-finished  // 2. Block until background goroutine has rendered its final frame and exited!
}
```
This guarantees zero terminal interleaving and clean deterministic shutdown.

## 10. Task 9: Real-Time Web Telemetry with Server-Sent Events (SSE) & Virtual Byte Ranges

Building a production-ready concurrent engine is incomplete without real-time observability. In Task 9, we built an Awwwards / Linear-tier dark luxury web dashboard (`web/`) that connects directly to the concurrent download engine via **Server-Sent Events (SSE)**.

---

### SSE vs WebSockets vs gRPC: Why SSE Wins for Telemetry

When designing real-time progress streaming from server to browser, three paradigms exist:

| Feature / Metric | Server-Sent Events (SSE) | WebSockets (WS) | gRPC / HTTP/2 |
|---|---|---|---|
| **Communication Direction** | **Unidirectional** (Server $\rightarrow$ Client) | **Bidirectional** (Full Duplex) | **Bidirectional** (RPC Streams) |
| **Transport Protocol** | Standard HTTP/1.1 or HTTP/2 | Upgraded TCP Socket (`ws://`, `wss://`) | HTTP/2 Multiplexed Framing |
| **Browser Compatibility** | **Native** (`EventSource` API, zero dependencies) | Native (`WebSocket` API) | **Requires Envoy / `grpc-web` proxy** |
| **Corporate Firewall / Proxy Traversal** | **Trivial** (standard port 80/443 HTTP) | Prone to proxy/firewall dropping | Requires specialized proxy routing |
| **Automatic Reconnection** | **Built-in** by browser specification | Manual backoff logic required in JS | Complex client connection recovery |
| **Server Implementation in Go** | Zero external libraries (`net/http` + `http.Flusher`) | Requires external library (`gorilla/websocket` or `nhooyr/websocket`) | Heavy Protobuf codegen & toolchain |
| **Suitability for Telemetry** | 🌟 **Ideal** (Telemetry is strictly server $\rightarrow$ client) | Overkill (Client rarely sends data back during download) | Over-engineered for a web browser UI |

**Architectural Rationale**:
Because file download progress is strictly a server-to-client telemetry broadcast:
1. **Zero third-party dependencies** in Go: Uses standard library `http.ResponseWriter` and type assertion to `http.Flusher`.
2. **Native browser support**: `const sse = new EventSource(url)` provides automatic exponential backoff reconnection.
3. **Transparent caching and security**: Standard HTTP headers, CORS policies, and TLS termination work out of the box without special WebSocket upgrade proxies.

---

### Synthetic RFC 7233 Byte-Range Serving (Zero-RAM Virtual File Seeker)

To demo concurrent downloads of 50 MB or 100 MB files without requiring internet connectivity or allocating massive files on disk, we engineered `SyntheticSeeker`.

Go's standard library `http.ServeContent` already contains a complete RFC 7233 byte-range state machine! However, it requires an `io.ReadSeeker`:
```go
type ReadSeeker interface {
    io.Reader
    io.Seeker
}
```

Instead of keeping 100 MB in memory (`[]byte`) or writing 100 MB to disk, `SyntheticSeeker` calculates virtual byte offsets mathematically on the fly:

```go
type SyntheticSeeker struct {
    size   int64
    offset int64
}

func (s *SyntheticSeeker) Read(p []byte) (n int, err error) {
    if s.offset >= s.size {
        return 0, io.EOF
    }
    toRead := int64(len(p))
    if s.offset+toRead > s.size {
        toRead = s.size - s.offset
    }
    // Deterministic pseudo-random bytes based on byte offset
    for i := int64(0); i < toRead; i++ {
        p[i] = byte((s.offset + i) % 256)
    }
    s.offset += toRead
    return int(toRead), nil
}

func (s *SyntheticSeeker) Seek(offset int64, whence int) (int64, error) {
    var newOffset int64
    switch whence {
    case io.SeekStart:
        newOffset = offset
    case io.SeekCurrent:
        newOffset = s.offset + offset
    case io.SeekEnd:
        newOffset = s.size + offset
    default:
        return 0, errors.New("invalid whence")
    }
    if newOffset < 0 {
        return 0, errors.New("negative offset")
    }
    s.offset = newOffset
    return s.offset, nil
}
```

**Memory & Performance Impact**:
- Serving a 10 GB file uses **16 bytes of RAM** (two `int64` fields).
- Any standard HTTP client sending `Range: bytes=1000000-2000000` receives exact bytes with cryptographic repeatability and zero disk I/O.

---

### Server-Sent Events Protocol & http.Flusher Mechanics

An SSE endpoint writes text data adhering to the W3C EventSource standard:
```http
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
Access-Control-Allow-Origin: *

data: {"type":"progress","total":52428800,"transferred":10485760}

data: {"type":"stitched","sha256":"a1b2c3d4..."}

```

In Go's `net/http` package, HTTP response bodies are buffered by default for TCP efficiency. For SSE, this buffering must be bypassed so the browser sees updates instantly. This is done via type assertion to `http.Flusher`:

```go
w.Header().Set("Content-Type", "text/event-stream")
w.Header().Set("Cache-Control", "no-cache")
w.Header().Set("Connection", "keep-alive")

flusher, ok := w.(http.Flusher)
if !ok {
    http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
    return
}

sendEvent := func(eventType string, data any) {
    payload, _ := json.Marshal(data)
    fmt.Fprintf(w, "data: %s\n\n", payload)
    flusher.Flush() // Force immediate TCP packet transmission!
}
```

#### Context Cancellation & Goroutine Leaks
When a user closes their browser tab or clicks "Cancel", the HTTP connection terminates. If the server does not monitor this, worker goroutines keep running, leaking sockets and CPU cycles. We bind the download to the HTTP request context:

```go
ctx, cancel := context.WithCancel(r.Context())
defer cancel()

// Listen for client disconnect
go func() {
    <-r.Context().Done()
    cancel() // Abort background download immediately!
}()
```

---

### Consuming SSE in Modern JavaScript with Native EventSource

On the frontend, zero third-party libraries (no Socket.io, no Axios) are needed:

```javascript
const sse = new EventSource(`/api/download/stream?url=${encodeURIComponent(url)}&workers=${workers}&chunks=${chunks}`);

sse.onmessage = (event) => {
    const data = JSON.parse(event.data);
    switch (data.type) {
        case 'start':
            renderWorkers(data.workers);
            break;
        case 'progress':
            updateMasterProgress(data.transferred, data.total, data.speed_mbps, data.eta_seconds);
            updateWorkerCards(data.workers);
            break;
        case 'stitched':
            showIntegrityBadge(data.sha256);
            break;
        case 'done':
            sse.close();
            break;
    }
};

sse.onerror = (err) => {
    console.error("SSE stream disconnected", err);
    sse.close();
};
```

---

### Aesthetic & Design Philosophy (Linear/OLED Luxury UI)

Following modern design principles:
1. **OLED Background (`#050505`)**: Infinite contrast that makes glowing UI accents pop.
2. **Concentric Double-Bezel Architecture**: Cards have subtle borders (`rgba(255, 255, 255, 0.08)`) and nested inner containers with slightly deeper shadows (`0 8px 32px rgba(0, 0, 0, 0.5)`).
3. **Custom Spring Easing**:
   `transition: all 300ms cubic-bezier(0.32, 0.72, 0, 1);` delivers snappy, tactile feedback reminiscent of native macOS/iOS animations.
4. **Per-Worker Gauge Cards**: Each worker thread has its own progress bar, byte counter, and state indicator (`idle`, `active`, `completed`).
5. **Protocol Trace Drawer**: Collapsible diagnostic log providing transparency into RFC 7233 range negotiation, 206 responses, and cryptographic verification.

---

## 11. Go Standard Library Function Dictionary

This section catalogs every standard library function used, its purpose, and its return values.

### Built-in (`builtin`)
* **`make([]T, length, capacity) []T`**
  * *What it does*: Allocates a slice with a specified length and capacity on the heap, initializing elements to their type's zero value.
  * *Returns*: Initialized slice of type `[]T`.
* **`make(chan T, capacity) chan T`**
  * *What it does*: Allocates a buffered channel on the heap with specified buffer capacity.
  * *Returns*: Initialized channel of type `chan T`.
* **`close(c chan<- T)`**
  * *What it does*: Closes a channel so no further values can be sent. Existing buffered items can still be received.
  * *Returns*: None (`void`).

### `path/filepath`
* **`filepath.Dir(path string) string`**
  * *What it does*: Returns all but the last element of path, typically the path's enclosing directory.
  * *Returns*: Directory path `string`.

### `sync`
* **`sync.WaitGroup`**
  * *What it does*: Concurrency counter used to wait for a collection of goroutines to finish.
  * *Methods*:
    - `Add(delta int)`: Increments waitgroup counter.
    - `Done()`: Decrements waitgroup counter by 1.
    - `Wait()`: Blocks calling thread until counter reaches 0.

### `sync/atomic`
* **`atomic.Int64`**
  * *What it does*: Struct providing atomic 64-bit integer operations using hardware-level CPU instructions without mutex locks.
  * *Methods*:
    - `Add(delta int64) int64`: Atomically adds delta and returns the new value.
    - `Load() int64`: Atomically reads and returns the current value.
    - `Store(val int64)`: Atomically stores a new value.

### `time`
* **`time.Now() time.Time`**
  * *What it does*: Returns current local timestamp with monotonic clock reading for high-precision duration measurement.
  * *Returns*: `time.Time`.
* **`time.Since(t time.Time) time.Duration`**
  * *What it does*: Computes elapsed duration from timestamp `t` to current time.
  * *Returns*: `time.Duration`.
* **`time.NewTicker(d time.Duration) *time.Ticker`**
  * *What it does*: Produces a Ticker that delivers clock ticks on channel C every interval d.
  * *Methods*: `Stop()`: Releases associated system timer resources.
  * *Returns*: `*time.Ticker`.
* **`time.Duration`**
  * *What it does*: Integer type representing nanosecond elapsed time intervals.
  * *Methods*: `.Seconds() float64`, `.Round(m time.Duration) time.Duration`.

### `strings`
* **`strings.Repeat(s string, count int) string`**
  * *What it does*: Produces a new string consisting of `count` copies of string `s`.
  * *Returns*: Repeated `string`.

### `fmt`
* **`fmt.Sprintf(format string, a ...any) string`**
  * *What it does*: Formats values according to a format specifier without printing to stdout, returning the resulting formatted string.
  * *Returns*: Formatted `string`.

### `net/url`
* **`url.ParseRequestURI(rawURL string) (*url.URL, error)`**
  * *What it does*: Parses a raw string into a structured URL. Validates that the input is a valid absolute URI (with scheme and host) or an absolute path.
  * *Returns*: `*url.URL` (struct with `Scheme`, `Host`, `Path`, `Query`, etc.) and `error`.

### `net/http`
* **`http.Get(url string) (*http.Response, error)`**
  * *What it does*: Issues a standard HTTP GET request using default client settings.
  * *Returns*: `*http.Response` and error.
* **`http.NewRequest(method string, url string, body io.Reader) (*http.Request, error)`**
  * *What it does*: Constructs an HTTP request object. Supports `http.MethodHead`, `http.MethodGet`, etc.
  * *Returns*: `*http.Request` pointer or an error.
* **`req.Header.Set(key string, value string)`**
  * *What it does*: Stores a header key-value pair, canonicalizing the key (e.g. `"user-agent"` $\rightarrow$ `"User-Agent"`).
  * *Returns*: None (`void`).
* **`http.DefaultClient.Do(req *http.Request) (*http.Response, error)`**
  * *What it does*: Sends the request through Go's default connection-pooled HTTP client. Automatically manages TLS handshakes and follows up to 10 HTTP redirects.
  * *Returns*: `*http.Response` (with `StatusCode`, `Header`, `Body`, etc.) or network error.
* **`resp.Body.Close() error`**
  * *What it does*: Closes the response body stream. Mandatory to release socket resources back to the Go HTTP connection pool for TCP reuse.

### `os`
* **`os.Open(name string) (*os.File, error)`**
  * *What it does*: Opens a named file for reading (read-only mode).
  * *Returns*: `*os.File` pointer or filesystem error.
* **`os.Create(name string) (*os.File, error)`**
  * *What it does*: Creates or truncates the named file with mode `0666`.
  * *Returns*: `*os.File` pointer or filesystem error.
* **`os.OpenFile(name string, flag int, perm FileMode) (*os.File, error)`**
  * *What it does*: Opens a file on disk with custom POSIX flags (`O_CREATE`, `O_WRONLY`, `O_TRUNC`, `O_APPEND`) and octal permissions (e.g. `0644`).
  * *Returns*: `*os.File` pointer or filesystem error.
* **`os.CreateTemp(dir string, pattern string) (*os.File, error)`**
  * *What it does*: Creates a new temporary file in directory `dir` (or system default if `""`) with a unique randomized name matching `pattern`.
  * *Returns*: Open `*os.File` pointer or error.
* **`os.Remove(name string) error`**
  * *What it does*: Deletes the named file or empty directory from disk.
  * *Returns*: Error if removal fails.
* **`os.MkdirAll(path string, perm FileMode) error`**
  * *What it does*: Recursively creates directory tree along with any necessary parents (`mkdir -p`).
  * *Returns*: Error if creation fails.
* **`f.Close() error`**
  * *What it does*: Flushes cached data and releases the operating system file descriptor.
  * *Returns*: Error if closing fails.
* **`os.Exit(code int)`**
  * *What it does*: Causes the current program to exit immediately with the given status code (0 for success, non-zero for error). Deferred functions are not run.
  * *Returns*: Does not return.

### `io`
* **`io.MultiReader(readers ...io.Reader) io.Reader`**
  * *What it does*: Chains multiple readers into a single logical continuous stream. Once reader $N$ hits EOF, MultiReader shifts to reader $N+1$.
  * *Returns*: Composite `io.Reader`.
* **`io.TeeReader(r io.Reader, w io.Writer) io.Reader`**
  * *What it does*: Returns a reader that writes to `w` what it reads from `r`. All reads from `r` are matched by corresponding writes to `w`.
  * *Returns*: Composite `io.Reader`.
* **`io.Copy(dst io.Writer, src io.Reader) (written int64, err error)`**
  * *What it does*: Copies from `src` to `dst` using an internal 32 KB buffer until EOF or error. Zero full-file memory consumption.
  * *Returns*: Total bytes written (`int64`) and any error.

### `strconv`
* **`strconv.ParseInt(s string, base int, bitSize int) (int64, error)`**
  * *What it does*: Converts ASCII decimal string into a signed 64-bit integer (`bitSize: 64`). Prevents integer overflow for files larger than 2 GB on 32-bit platforms.
  * *Returns*: `int64` and `error`.

### `strings`
* **`strings.TrimSpace(s string) string`**
  * *What it does*: Strips leading and trailing whitespace, newlines, and tabs.
  * *Returns*: Cleaned `string`.
* **`strings.EqualFold(s1 string, s2 string) bool`**
  * *What it does*: Compares two strings case-insensitively using Unicode case-folding (e.g. `"bytes" == "BYTES"`). More efficient than allocating new strings with `strings.ToLower()`.
  * *Returns*: `bool`.

### `flag`
* **`flag.NewFlagSet(name string, errorHandling ErrorHandling) *flag.FlagSet`**
  * *What it does*: Creates a new isolated flag parser with its own name and error policy (`ContinueOnError`, `ExitOnError`, `PanicOnError`).
  * *Returns*: `*flag.FlagSet`.
* **`fs.StringVar(p *string, name string, value string, usage string)`**
  * *What it does*: Binds a string flag to the pointer `p`.
  * *Returns*: None (`void`).
* **`fs.IntVar(p *int, name string, value int, usage string)`**
  * *What it does*: Binds an integer flag to the pointer `p`.
  * *Returns*: None (`void`).
* **`fs.Parse(arguments []string) error`**
  * *What it does*: Parses the given command-line argument slice according to defined flags.
  * *Returns*: `error` if parsing fails (under `ContinueOnError`).
* **`fs.PrintDefaults()`**
  * *What it does*: Writes the default values and usage docs of all defined flags to standard error.
  * *Returns*: None (`void`).

### `mime` & `path`
* **`mime.ParseMediaType(v string) (mediatype string, params map[string]string, err error)`**
  * *What it does*: Parses MIME parameters from headers like `Content-Disposition: attachment; filename="data.zip"`.
  * *Returns*: Base type (`string`), key-value parameter map (`map[string]string`), and `error`.
* **`path.Clean(path string) string`**
  * *What it does*: Resolves relative path tokens like `.` and `..` and redundant slashes.
  * *Returns*: Clean canonical path `string`.
* **`path.Base(path string) string`**
  * *What it does*: Extracts the final element from a slash-delimited path, stripping all leading folders.
  * *Returns*: Base filename `string`.

### `context`
* **`context.WithCancel(parent context.Context) (ctx context.Context, cancel context.CancelFunc)`**
  * *What it does*: Returns a copy of parent with a new Done channel. The returned context's Done channel is closed when the returned cancel function is called or when the parent context's Done channel is closed.
  * *Returns*: `(context.Context, context.CancelFunc)`.
* **`ctx.Done() <-chan struct{}`**
  * *What it does*: Returns a channel that's closed when work done on behalf of this context should be cancelled.
  * *Returns*: `<-chan struct{}` receive-only channel.

### `net/http` (Streaming & Range Extensions)
* **`http.Flusher` (Interface)**
  * *What it does*: Interface implemented by `http.ResponseWriter` allowing an HTTP handler to flush buffered data directly to the client socket.
  * *Methods*: `Flush()`
* **`http.ServeContent(w http.ResponseWriter, req *http.Request, name string, modtime time.Time, content io.ReadSeeker)`**
  * *What it does*: Replies to the request using the content in the provided `ReadSeeker`. Automatically evaluates `Range`, `If-Range`, and `If-Modified-Since` headers, setting `206 Partial Content` or `200 OK` accordingly.
  * *Returns*: None (`void`).

### `io` (Seeker Extensions)
* **`io.Seeker` (Interface)**
  * *What it does*: Interface that wraps the basic `Seek` method. Sets the offset for the next Read or Write.
  * *Methods*: `Seek(offset int64, whence int) (int64, error)`
* **`io.ReadSeeker` (Interface)**
  * *What it does*: Interface grouping the basic `Reader` and `Seeker` interfaces.
  * *Methods*: `Read(p []byte) (n int, err error)`, `Seek(offset int64, whence int) (int64, error)`

---

## 12. Gotchas & Best Practices Log

### 1. Always Close `resp.Body`, Even on `HEAD` Requests
In Go, `resp.Body` must be read and closed. Even though a `HEAD` request has no body bytes, calling `defer resp.Body.Close()` is required so the Go runtime can safely recycle the underlying TCP socket in its HTTP keep-alive connection pool.

### 2. Wrap Errors with `%w`
Always use `fmt.Errorf("...: %w", err)` rather than `%v`. `%w` preserves the error chain, allowing callers to use `errors.Is(err, ...)` or `errors.As(err, ...)`.

### 3. Check for Status 2xx Explicitly
`http.Client.Do` returns `err != nil` only for transport/network failures (e.g. DNS failure, connection refused, TLS handshake failure). An HTTP `404 Not Found` or `500 Internal Server Error` is considered a **successful HTTP round-trip** with `err == nil`. You must manually verify:
```go
if resp.StatusCode < 200 || resp.StatusCode >= 300 {
    return nil, fmt.Errorf("server returned: %s", resp.Status)
}
```

### 4. HTTP Range Offsets are Inclusive
Unlike slice slicing in Go (`s[start:end]` where `end` is exclusive), HTTP Range header offsets (`bytes=start-end`) are **inclusive on both ends**. Always calculate byte counts as `(end - start + 1)`.

### 5. Always Check for Status 206 on Range Requests
If a server returns `200 OK` when you sent a `Range` header, it ignored your range request. Never stream a `200 OK` response into a chunk file, as it will contain the whole file, not just the slice!

### 6. The Race Detector (`go test -race`) is Mandatory for Concurrent Code
When multiple goroutines access shared variables, race conditions cause silent memory corruption. Always validate concurrent packages with `go test -race`. Atomic variables (`sync/atomic`) pass cleanly because their operations are serialized by CPU memory barriers.

### 7. Never Pass a Typed Nil Pointer into an Interface Parameter
In Go, `var p *T = nil` assigned to `var i Interface = p` produces an interface where `i != nil` is **true** because its dynamic type is `*T`. If receiving methods do not check `if p == nil`, calling `i.Method()` will trigger a runtime nil pointer panic! Always pass untyped `nil` or check `if p == nil` inside the receiver method.

### 8. Windows Requires Closing File Descriptors Before Deleting Files
On Windows, attempting to delete an open file with `os.Remove()` triggers an OS file-sharing violation (`Access is denied`). Always close all open `*os.File` handles before deleting temporary chunk files.

### 9. Await Background Worker Clean Teardown
Never assume that sending a cancellation signal (`close(done)`) to a background goroutine finishes synchronously. If the main thread continues immediately, race conditions between teardown and output rendering will corrupt terminal output. Always use a completion channel (`<-finished`) or a `sync.WaitGroup` to await full exit before continuing.

### 10. Mock HTTP Servers Must Match `Content-Length` on 206 Responses
When writing unit tests with `httptest.Server`, setting `Content-Length: len(payload)` globally will cause Go's `http.Client` to fail with `unexpected EOF` on 206 Partial Content responses if only a slice of the payload is returned. For 206 Partial Content, `Content-Length` must be set to `(end - start + 1)`.

### 11. Assert `http.Flusher` Before Streaming SSE
Not all `http.ResponseWriter` implementations support flushing (for instance, certain HTTP middleware, recording test writers, or reverse proxies might buffer whole responses). Always verify with type assertion:
```go
flusher, ok := w.(http.Flusher)
if !ok {
    http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
    return
}
```

### 12. Always Bind Long-Running Background Streams to `r.Context()`
If a user closes their browser window or navigates away mid-download, the HTTP TCP socket closes, but a background goroutine downloading files or executing work will keep running silently forever unless tied to `r.Context().Done()`. Always bind worker pools or downloads to the request context to avoid goroutine leaks.

### 13. SSE Double-Newline Framing Contract (`\n\n`)
The W3C Server-Sent Events standard dictates that an event block is only dispatched to the client when terminated by **two consecutive newline characters** (`\n\n`). If you emit `fmt.Fprintf(w, "data: %s\n", msg)`, the browser's `EventSource` will buffer it endlessly waiting for the terminating newline. Always terminate each SSE message block with `\n\n`.
