package downloader

import (
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
)

// TargetInfo stores metadata discovered during the HTTP probe phase.
type TargetInfo struct {
	URL            string // The original or resolved target URL
	Size           int64  // File size in bytes from Content-Length (-1 if unknown)
	RangeSupported bool   // True if the server advertises Accept-Ranges: bytes
	Filename       string // Suggested filename derived from headers or URL path
}

// ProbeTarget sends an HTTP HEAD request to determine the remote file size,
// range capability, and suggested filename without downloading the actual payload.
func ProbeTarget(rawURL string) (*TargetInfo, error) {
	// url.ParseRequestURI(rawURL):
	// - What it does: Parses a raw string into a URL structure. It validates that
	//   the input is a valid absolute URI (with scheme & host) or an absolute path.
	// - Returns: (*url.URL, error) -> A pointer to a url.URL struct containing parsed
	//   fields (Scheme, Host, Path, Query, etc.), or an error if invalid.
	parsedURL, err := url.ParseRequestURI(rawURL)
	if err != nil {
		// fmt.Errorf(format, args...):
		// - What it does: Formats a string using printf-style specifiers.
		//   The '%w' verb wraps the original error so callers can unwrap it
		//   or inspect it using errors.Is() / errors.As().
		// - Returns: error -> A formatted error value.
		return nil, fmt.Errorf("invalid URL %q: %w", rawURL, err)
	}

	// http.NewRequest(method, url, body):
	// - What it does: Constructs an *http.Request ready to be sent. We specify
	//   http.MethodHead ("HEAD") to request only response headers without the body.
	//   The body parameter is nil because HEAD requests carry no request payload.
	// - Returns: (*http.Request, error) -> Pointer to http.Request or an error.
	req, err := http.NewRequest(http.MethodHead, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HEAD request: %w", err)
	}

	// req.Header.Set(key, value):
	// - What it does: Sets an HTTP header key-value pair, overwriting any previous
	//   values for that key. Key casing is automatically canonicalized (e.g. "User-Agent").
	// - Returns: void (no return value).
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Beam/1.0")

	// http.DefaultClient.Do(req):
	// - What it does: Sends the HTTP request using Go's default HTTP client.
	//   It automatically handles redirects (up to 10 hops) and TLS handshakes.
	// - Returns: (*http.Response, error) -> Pointer to http.Response (containing
	//   StatusCode, Header, Body, etc.) or a network/transport error.
	resp, err := HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HEAD request failed: %w", err)
	}

	// resp.Body.Close():
	// - What it does: Closes the response body stream. In Go, you MUST close
	//   resp.Body even for HEAD requests so the underlying TCP connection can be
	//   reused by the client's connection pool (HTTP Keep-Alive).
	// - 'defer' guarantees this executes when the surrounding function exits.
	// - Returns: error -> Error if closing fails (usually safely ignored in defer).
	defer resp.Body.Close()

	// resp.StatusCode:
	// - Field on http.Response holding the integer HTTP status (e.g. 200, 404).
	// - resp.Status is the human-readable string (e.g. "200 OK").
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("server responded with status: %s (%d)", resp.Status, resp.StatusCode)
	}

	// 1. Detect File Size via Content-Length
	var size int64 = -1

	// resp.Header.Get(key):
	// - What it does: Performs a case-insensitive lookup for the specified HTTP header
	//   key and returns its first value. If the header does not exist, returns "".
	// - Returns: string -> Header value or "" if absent.
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		// strconv.ParseInt(s, base, bitSize):
		// - What it does: Parses the string 'cl' into a signed integer.
		//   base 10 = decimal representation ('0'-'9').
		//   bitSize 64 = fits into a 64-bit integer (int64), supporting files > 2GB.
		// - Returns: (int64, error) -> Parsed 64-bit integer and an error if non-numeric/overflow.
		if parsedSize, err := strconv.ParseInt(cl, 10, 64); err == nil && parsedSize >= 0 {
			size = parsedSize
		}
	}

	// 2. Detect Range Support via Accept-Ranges
	// RFC 7233 defines "bytes" as the standard range unit
	acceptRanges := resp.Header.Get("Accept-Ranges")

	// strings.TrimSpace(s):
	// - What it does: Strips leading and trailing whitespace (spaces, tabs, newlines).
	// - Returns: string -> Trimmed string.
	//
	// strings.EqualFold(s1, s2):
	// - What it does: Compares two strings case-insensitively using Unicode case-folding
	//   (e.g. "bytes" == "Bytes" == "BYTES"). Faster than converting both to lowercase.
	// - Returns: bool -> true if equal ignoring case, false otherwise.
	rangeSupported := strings.EqualFold(strings.TrimSpace(acceptRanges), "bytes")

	// 3. Extract Suggested Filename
	filename := extractFilename(resp, parsedURL)

	return &TargetInfo{
		URL:            rawURL,
		Size:           size,
		RangeSupported: rangeSupported,
		Filename:       filename,
	}, nil
}

// extractFilename attempts to determine the file name first from the
// Content-Disposition header, falling back to the URL path.
func extractFilename(resp *http.Response, u *url.URL) string {
	// Strategy A: Check Content-Disposition header (e.g. attachment; filename="foo.zip")
	cd := resp.Header.Get("Content-Disposition")
	if cd != "" {
		// mime.ParseMediaType(v):
		// - What it does: Parses a MIME media type header and its optional parameters
		//   (e.g., 'attachment; filename="data.zip"').
		// - Returns:
		//     1. mediatype (string): The base type (e.g., "attachment").
		//     2. params (map[string]string): Key-value parameters (e.g., {"filename": "data.zip"}).
		//     3. err (error): Error if parsing fails.
		if _, params, err := mime.ParseMediaType(cd); err == nil {
			if name, ok := params["filename"]; ok && strings.TrimSpace(name) != "" {
				// path.Base(path):
				// - What it does: Returns the last element of a slash-separated path.
				//   Stripping directory prefixes (e.g. "../../secret.txt" -> "secret.txt")
				//   helps prevent path traversal vulnerabilities.
				// - Returns: string -> The base filename.
				return path.Base(strings.TrimSpace(name))
			}
		}
	}

	// Strategy B: Extract base from URL path (e.g. https://example.com/files/archive.tar.gz -> archive.tar.gz)
	//
	// path.Clean(path):
	// - What it does: Lexically cleans a path by resolving '.', '..', and redundant slashes.
	// - Returns: string -> Cleaned path.
	cleanPath := path.Clean(u.Path)

	// path.Base(cleanPath):
	// - What it does: Returns the final path component (e.g. "/a/b/file.zip" -> "file.zip").
	//   If empty, returns ".". If "/", returns "/".
	// - Returns: string -> The base filename or "."/"/".
	base := path.Base(cleanPath)
	if base != "" && base != "." && base != "/" {
		return base
	}

	// Strategy C: Safe fallback if URL has no file extension or name
	return "downloaded_file"
}
