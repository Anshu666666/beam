package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Anshu666666/beam/downloader"
)

func main() {
	// Delegate execution to run() and handle OS process exit codes
	if err := run(os.Args[1:]); err != nil {
		// fmt.Fprintf(w, format, a...):
		// - What it does: Formats according to a format specifier and writes to the provided io.Writer stream.
		// - Returns: (int, error) -> Number of bytes written and any write error encountered.
		fmt.Fprintf(os.Stderr, "\n[Error]: %v\n", err)
		// os.Exit(code):
		// - What it does: Causes the current program to exit immediately with the given status code (0 for success, non-zero for error).
		// - Returns: Does not return.
		os.Exit(1)
	}
}

// run parses command-line arguments and coordinates the download.
// Decoupling run() from main() enables isolated unit and integration testing
// without calling os.Exit(1).
func run(args []string) error {
	// flag.NewFlagSet(name, errorHandling):
	// - What it does: Creates a new, isolated set of command-line flags with its own name and error policy.
	// - Returns: *flag.FlagSet -> Configured flag set instance.
	fs := flag.NewFlagSet("beam", flag.ContinueOnError)

	var url string
	var outputFile string
	var workers int
	var chunks int

	// fs.StringVar(p, name, value, usage):
	// - What it does: Binds a string flag with specified name, default value, and usage description to target pointer.
	// - Returns: None (modifies target pointer in-place).
	fs.StringVar(&url, "url", "", "Target URL of the remote file to download (required)")
	fs.StringVar(&outputFile, "o", "", "Destination file path on disk (optional: auto-derived from URL if omitted)")

	// fs.IntVar(p, name, value, usage):
	// - What it does: Binds an integer flag with specified name, default value, and usage description to target pointer.
	// - Returns: None (modifies target pointer in-place).
	fs.IntVar(&workers, "w", 4, "Number of concurrent worker goroutines in the pool")
	fs.IntVar(&chunks, "c", 4, "Number of byte range chunks to divide the file into")

	// Custom usage message when help is requested or arguments are malformed
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of beam:\n")
		fmt.Fprintf(os.Stderr, "  beam -url <URL> [-o <destination>] [-w <workers>] [-c <chunks>]\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		// fs.PrintDefaults():
		// - What it does: Prints the default values and usage documentation for all defined flags to standard error.
		// - Returns: None.
		fs.PrintDefaults()
	}

	// fs.Parse(arguments):
	// - What it does: Parses flag definitions from the provided argument slice.
	// - Returns: error -> nil on success, or flag parsing error.
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	// Validate required argument
	if url == "" {
		fs.Usage()
		return fmt.Errorf("-url is required")
	}

	// Print startup header banner
	// fmt.Println(a...):
	// - What it does: Formats using default formats for its operands and writes to standard output with a trailing newline.
	// - Returns: (int, error) -> Number of bytes written and any write error encountered.
	fmt.Println("┌────────────────────────────────────────────────────────────┐")
	fmt.Println("│  ⚡ BEAM · Concurrent RFC 7233 Streaming Engine (Go CLI)   │")
	fmt.Println("└────────────────────────────────────────────────────────────┘")

	fmt.Printf("URL:        \033[36m%s\033[0m\n", url)
	if outputFile != "" {
		fmt.Printf("Output:     \033[33m%s\033[0m\n", outputFile)
	} else {
		fmt.Printf("Output:     \033[90m(auto-derived)\033[0m\n")
	}
	fmt.Printf("Workers:    \033[1m%d\033[0m concurrent streams\n", workers)
	fmt.Printf("Chunks:     \033[1m%d\033[0m byte partitions\n", chunks)
	fmt.Println("────────────────────────────────────────────────────────────")
	fmt.Println("\033[33m[⚡] Probing target server & byte-range capabilities...\033[0m")

	// time.Now():
	// - What it does: Returns the current local time with monotonic clock reading.
	// - Returns: time.Time -> Current timestamp.
	startTime := time.Now()

	initTerminal()

	var lastLines int
	opts := downloader.Options{
		URL:        url,
		OutputFile: outputFile,
		Workers:    workers,
		Chunks:     chunks,
		OnProgress: func(tracker *downloader.ProgressTracker) {
			lines := tracker.RenderDashboard(25, 20)
			if lastLines > 0 {
				// \033[%dA moves cursor UP by lastLines count
				fmt.Printf("\033[%dA", lastLines)
			}
			for _, line := range lines {
				// \r moves to col 0, \033[2K clears entire line, \n advances to next row
				fmt.Printf("\r\033[2K%s\n", line)
			}
			lastLines = len(lines)
		},
	}

	err := downloader.Download(opts)
	if err != nil {
		fmt.Println()
		return fmt.Errorf("download failed: %w", err)
	}

	// time.Since(t):
	// - What it does: Computes the elapsed duration from timestamp 't' until now.
	// - Returns: time.Duration -> Elapsed time interval.
	elapsed := time.Since(startTime)

	fmt.Println("────────────────────────────────────────────────────────────")
	fmt.Printf("\033[1;32m✔ Download completed in %s!\033[0m\n", elapsed.Round(time.Millisecond))
	fmt.Println("============================================================")

	return nil
}
