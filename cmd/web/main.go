package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/Anshu666666/beam/web"
)

func main() {
	var port int
	var staticDir string

	flag.IntVar(&port, "port", 8080, "Port for web dashboard")
	flag.StringVar(&staticDir, "static", "projects/01_chunk_downloader/web/static", "Directory containing static frontend assets")
	flag.Parse()

	// Fallback check if run from different working directory
	if _, err := os.Stat(staticDir); os.IsNotExist(err) {
		altDir := filepath.Join("web", "static")
		if _, err2 := os.Stat(altDir); err2 == nil {
			staticDir = altDir
		}
	}

	server := web.NewServer(staticDir)

	fmt.Println("============================================================")
	fmt.Println("     Beam — Concurrent Byte-Range Streaming Engine         ")
	fmt.Println("============================================================")
	fmt.Printf("🚀 Server live on:   http://localhost:%d\n", port)
	fmt.Printf("📦 Static assets:    %s\n", staticDir)
	fmt.Println("------------------------------------------------------------")
	fmt.Println("Press Ctrl+C to terminate.")

	err := http.ListenAndServe(fmt.Sprintf(":%d", port), server.Handler())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Server exited with error: %v\n", err)
		os.Exit(1)
	}
}
