package downloader

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

// DownloadChunk downloads a single byte-range slice defined by 'chunk' from 'targetURL'
// and streams the payload directly to 'chunk.TempFilePath' on disk.
//
// If 'counter' is non-nil, an io.TeeReader is attached to resp.Body so that every byte
// read from the HTTP response socket is duplicated to 'counter' (e.g. for progress tracking)
// in a single pass without buffering or double disk writes.
func DownloadChunk(targetURL string, chunk Chunk, counter io.Writer) error {
	// Guard clause: ensure destination file path is specified
	if chunk.TempFilePath == "" {
		return fmt.Errorf("chunk %d has empty TempFilePath", chunk.Index)
	}

	// http.NewRequest(method, url, body):
	// - What it does: Constructs an *http.Request. We use GET with a nil body.
	// - Returns: (*http.Request, error) -> Prepared request or creation error.
	req, err := http.NewRequest(http.MethodGet, targetURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create GET request for chunk %d: %w", chunk.Index, err)
	}

	// fmt.Sprintf(format, a...):
	// - What it does: Formats string according to format specifier.
	//   HTTP Range syntax (RFC 7233) format is: "bytes=START-END".
	// - Returns: string -> The formatted range header value.
	rangeHeaderValue := fmt.Sprintf("bytes=%d-%d", chunk.Start, chunk.End)

	// req.Header.Set(key, value):
	// - What it does: Sets the HTTP 'Range' request header.
	// - Returns: void.
	req.Header.Set("Range", rangeHeaderValue)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Beam/1.0")

	// http.DefaultClient.Do(req):
	// - What it does: Dispatches HTTP request over network socket using connection pooling.
	// - Returns: (*http.Response, error) -> HTTP response or network/transport error.
	resp, err := HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("network request failed for chunk %d [%s]: %w", chunk.Index, rangeHeaderValue, err)
	}

	// resp.Body.Close():
	// - What it does: Closes the HTTP response body stream so the underlying TCP socket
	//   connection can be safely returned to Go's HTTP client connection pool for reuse.
	// - Returns: error -> Error if closing fails.
	defer resp.Body.Close()

	// RFC 7233 Specification:
	// A server serving a valid range request MUST reply with status code 206 Partial Content.
	// If it replies with 200 OK, the server ignored the Range header and sent the entire file!
	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("server did not honor range for chunk %d (expected 206 Partial Content, got %s [%d])",
			chunk.Index, resp.Status, resp.StatusCode)
	}

	// os.OpenFile(name, flag, perm):
	// - What it does: Opens or creates a file on disk with specified POSIX flags:
	//   - os.O_CREATE: create the file if it does not already exist
	//   - os.O_WRONLY: open for writing only
	//   - os.O_TRUNC: truncate (empty) the file if it already exists
	//   - 0644: permissions (read/write for owner, read-only for others)
	// - Returns: (*os.File, error) -> Pointer to os.File or filesystem error.
	outFile, err := os.OpenFile(chunk.TempFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open temp chunk file %q: %w", chunk.TempFilePath, err)
	}

	// outFile.Close():
	// - What it does: Flushes any dirty buffers and closes the OS file descriptor.
	// - Returns: error -> Error if closing fails.
	defer outFile.Close()

	// Stream Reader setup:
	var src io.Reader = resp.Body

	if counter != nil {
		// io.TeeReader(r, w):
		// - What it does: Wraps reader 'r' and writer 'w'. Every byte read from 'r'
		//   is immediately written to 'w'. This enables single-pass stream monitoring
		//   without buffering bytes in RAM or performing two separate disk passes.
		// - Returns: io.Reader -> A new composite reader.
		src = io.TeeReader(resp.Body, counter)
	}

	// io.Copy(dst, src):
	// - What it does: Continuously reads chunks from 'src' and writes to 'dst'
	//   using a fixed 32 KB internal buffer until EOF or an error is encountered.
	//   Guarantees constant, low-RAM streaming regardless of chunk size (e.g. 10 GB).
	// - Returns: (int64, error) -> Number of bytes transferred and any error encountered.
	_, err = io.Copy(outFile, src)
	if err != nil {
		return fmt.Errorf("failed streaming chunk %d to disk: %w", chunk.Index, err)
	}

	return nil
}
