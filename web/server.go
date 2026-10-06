package web

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Anshu666666/beam/downloader"
)

// SyntheticSeeker implements io.ReadSeeker without holding the file in RAM.
// It generates deterministic byte sequences on the fly, allowing zero-memory
// HTTP byte-range serving for 50MB and 100MB demonstration files.
type SyntheticSeeker struct {
	size int64
	pos  int64
}

// NewSyntheticSeeker creates a synthetic seeker of the given byte size.
func NewSyntheticSeeker(size int64) *SyntheticSeeker {
	return &SyntheticSeeker{size: size}
}

func (s *SyntheticSeeker) Read(p []byte) (n int, err error) {
	if s.pos >= s.size {
		return 0, io.EOF
	}
	remaining := s.size - s.pos
	toRead := int64(len(p))
	if toRead > remaining {
		toRead = remaining
	}
	for i := int64(0); i < toRead; i++ {
		p[i] = byte((s.pos + i) % 251)
	}
	s.pos += toRead
	return int(toRead), nil
}

func (s *SyntheticSeeker) Seek(offset int64, whence int) (int64, error) {
	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = s.pos + offset
	case io.SeekEnd:
		newPos = s.size + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}
	if newPos < 0 {
		return 0, fmt.Errorf("negative position: %d", newPos)
	}
	s.pos = newPos
	return newPos, nil
}

// MasterProgressDTO represents JSON serializable master progress.
type MasterProgressDTO struct {
	Percent             float64 `json:"percent"`
	Downloaded          int64   `json:"downloaded"`
	Total               int64   `json:"total"`
	DownloadedFormatted string  `json:"downloadedFormatted"`
	TotalFormatted      string  `json:"totalFormatted"`
	Speed               float64 `json:"speed"`
	SpeedFormatted      string  `json:"speedFormatted"`
	ETASeconds          int     `json:"etaSeconds"`
}

// WorkerProgressDTO represents JSON serializable worker status.
type WorkerProgressDTO struct {
	ID                  int     `json:"id"`
	CurrentChunk        int     `json:"currentChunk"`
	ChunkSize           int64   `json:"chunkSize"`
	ChunkDownloaded     int64   `json:"chunkDownloaded"`
	Percent             float64 `json:"percent"`
	DownloadedFormatted string  `json:"downloadedFormatted"`
	TotalFormatted      string  `json:"totalFormatted"`
	Status              string  `json:"status"` // "idle", "downloading", "done"
}

// SSEMessage represents the payload streamed to the frontend over SSE.
type SSEMessage struct {
	Type           string              `json:"type"` // "start", "progress", "complete", "error"
	Filename       string              `json:"filename,omitempty"`
	RangeSupported bool                `json:"rangeSupported,omitempty"`
	Duration       string              `json:"duration,omitempty"`
	AverageSpeed   string              `json:"averageSpeed,omitempty"`
	Error          string              `json:"error,omitempty"`
	Master         *MasterProgressDTO  `json:"master,omitempty"`
	Workers        []WorkerProgressDTO `json:"workers,omitempty"`
}

// Server handles HTTP static serving and SSE download orchestration.
type Server struct {
	mux       *http.ServeMux
	staticDir string
}

// NewServer constructs and routes the web application server.
func NewServer(staticDir string) *Server {
	s := &Server{
		mux:       http.NewServeMux(),
		staticDir: staticDir,
	}
	s.routes()
	return s
}

// Handler returns the underlying http.Handler.
func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) routes() {
	// 1. Static asset server (HTML, CSS, JS)
	fs := http.FileServer(http.Dir(s.staticDir))
	s.mux.Handle("/", fs)

	// 2. Synthetic RFC 7233 Demo Endpoints (50MB & 100MB)
	s.mux.HandleFunc("/api/demo/50mb", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		seeker := NewSyntheticSeeker(50 * 1024 * 1024)
		http.ServeContent(w, r, "demo_50mb.dat", time.Time{}, seeker)
	})

	s.mux.HandleFunc("/api/demo/100mb", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		seeker := NewSyntheticSeeker(100 * 1024 * 1024)
		http.ServeContent(w, r, "demo_100mb.dat", time.Time{}, seeker)
	})

	// 3. Server-Sent Events (SSE) Download Endpoint
	s.mux.HandleFunc("/api/download/stream", s.handleDownloadStream)
}

func (s *Server) handleDownloadStream(w http.ResponseWriter, r *http.Request) {
	// Enable SSE response headers
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported by client", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	flusher.Flush()

	query := r.URL.Query()
	targetURL := query.Get("url")
	if targetURL == "" {
		sendSSE(w, flusher, SSEMessage{
			Type:  "error",
			Error: "Query parameter 'url' is required",
		})
		return
	}

	// Auto-resolve relative paths (e.g. /api/demo/50mb) against the incoming Host header
	if strings.HasPrefix(targetURL, "/") {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		host := r.Host
		if host == "" {
			host = "localhost:8080"
		}
		targetURL = fmt.Sprintf("%s://%s%s", scheme, host, targetURL)
	}

	workers, _ := strconv.Atoi(query.Get("workers"))
	if workers <= 0 {
		workers = 4
	}

	chunks, _ := strconv.Atoi(query.Get("chunks"))
	if chunks <= 0 {
		chunks = 4
	}

	// Create temporary destination file
	tmpFile, err := os.CreateTemp("", "web_download_*")
	if err != nil {
		sendSSE(w, flusher, SSEMessage{Type: "error", Error: err.Error()})
		return
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	// Step 1: Probe target info
	info, err := downloader.ProbeTarget(targetURL)
	if err != nil {
		sendSSE(w, flusher, SSEMessage{
			Type:  "error",
			Error: fmt.Sprintf("Probe failed: %v", err),
		})
		return
	}

	sendSSE(w, flusher, SSEMessage{
		Type:           "start",
		Filename:       info.Filename,
		RangeSupported: info.RangeSupported,
		Master: &MasterProgressDTO{
			Total:          info.Size,
			TotalFormatted: downloader.FormatBytes(info.Size),
		},
	})

	startTime := time.Now()
	var mu sync.Mutex

	opts := downloader.Options{
		URL:        targetURL,
		OutputFile: tmpPath,
		Workers:    workers,
		Chunks:     chunks,
		OnProgress: func(tracker *downloader.ProgressTracker) {
			mu.Lock()
			defer mu.Unlock()

			msg := SSEMessage{
				Type: "progress",
				Master: &MasterProgressDTO{
					Percent:             tracker.Percent(),
					Downloaded:          tracker.Downloaded(),
					Total:               tracker.TotalSize(),
					DownloadedFormatted: downloader.FormatBytes(tracker.Downloaded()),
					TotalFormatted:      downloader.FormatBytes(tracker.TotalSize()),
					Speed:               tracker.Speed(),
					SpeedFormatted:      downloader.FormatSpeed(tracker.Speed()),
					ETASeconds:          int(tracker.ETA().Seconds()),
				},
			}

			for _, wt := range tracker.Workers() {
				statusStr := "idle"
				switch wt.Status() {
				case downloader.WorkerStatusDownloading:
					statusStr = "downloading"
				case downloader.WorkerStatusDone:
					statusStr = "done"
				}

				msg.Workers = append(msg.Workers, WorkerProgressDTO{
					ID:                  wt.ID(),
					CurrentChunk:        wt.CurrentChunk(),
					ChunkSize:           wt.ChunkTotal(),
					ChunkDownloaded:     wt.ChunkDownloaded(),
					Percent:             wt.Percent(),
					DownloadedFormatted: downloader.FormatBytes(wt.ChunkDownloaded()),
					TotalFormatted:      downloader.FormatBytes(wt.ChunkTotal()),
					Status:              statusStr,
				})
			}

			sendSSE(w, flusher, msg)
		},
	}

	err = downloader.Download(opts)
	if err != nil {
		sendSSE(w, flusher, SSEMessage{
			Type:  "error",
			Error: fmt.Sprintf("Download failed: %v", err),
		})
		return
	}

	elapsed := time.Since(startTime)
	sendSSE(w, flusher, SSEMessage{
		Type:         "complete",
		Duration:     elapsed.Round(time.Millisecond).String(),
		AverageSpeed: downloader.FormatSpeed(float64(info.Size) / elapsed.Seconds()),
	})
}

func sendSSE(w http.ResponseWriter, flusher http.Flusher, msg SSEMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	fmt.Fprintf(w, "data: %s\n\n", data)
	flusher.Flush()
}
