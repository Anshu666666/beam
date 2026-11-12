package main

import (
	"errors"
	"flag"
	"fmt"
	neturl "net/url"
	"os"
	"path"
	"path/filepath"
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
	var demo bool
	var version bool

	// fs.StringVar(p, name, value, usage):
	// - What it does: Binds a string flag with specified name, default value, and usage description to target pointer.
	// - Returns: None (modifies target pointer in-place).
	fs.StringVar(&url, "url", "", "Target URL of the remote file to download (required unless positional URL or -demo is used)")
	fs.StringVar(&outputFile, "o", "", "Destination file path on disk (optional: auto-derived from URL if omitted)")

	// fs.IntVar(p, name, value, usage):
	// - What it does: Binds an integer flag with specified name, default value, and usage description to target pointer.
	// - Returns: None (modifies target pointer in-place).
	fs.IntVar(&workers, "w", 4, "Number of concurrent worker goroutines in the pool")
	fs.IntVar(&chunks, "c", 4, "Number of byte range chunks to divide the file into")

	fs.BoolVar(&demo, "demo", false, "Run an instant demonstration download (50 MB sample file, 8 workers, 16 chunks)")
	fs.BoolVar(&version, "version", false, "Print version information and exit")
	fs.BoolVar(&version, "v", false, "Print version information and exit (shorthand)")

	// Custom usage message when help is requested or arguments are malformed
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "┌────────────────────────────────────────────────────────────┐\n")
		fmt.Fprintf(os.Stderr, "│  ⚡ BEAM · Concurrent RFC 7233 Streaming Engine (Go CLI)   │\n")
		fmt.Fprintf(os.Stderr, "└────────────────────────────────────────────────────────────┘\n\n")
		fmt.Fprintf(os.Stderr, "High-throughput concurrent byte-range downloader with live visual cockpit.\n\n")
		fmt.Fprintf(os.Stderr, "USAGE:\n")
		fmt.Fprintf(os.Stderr, "  beam -url <URL> [flags]\n")
		fmt.Fprintf(os.Stderr, "  beam <URL> [flags]\n")
		fmt.Fprintf(os.Stderr, "  beam -demo\n\n")
		fmt.Fprintf(os.Stderr, "FLAGS:\n")
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nEXAMPLES:\n")
		fmt.Fprintf(os.Stderr, "  # Instant test download (50 MB sample asset):\n")
		fmt.Fprintf(os.Stderr, "  beam -demo\n\n")
		fmt.Fprintf(os.Stderr, "  # Download multi-GB file with 8 workers and 16 chunks:\n")
		fmt.Fprintf(os.Stderr, "  beam -url \"https://releases.ubuntu.com/24.04/ubuntu-24.04.5.1-desktop-amd64.iso\" -w 8 -c 16\n\n")
		fmt.Fprintf(os.Stderr, "  # Direct URL positional syntax:\n")
		fmt.Fprintf(os.Stderr, "  beam https://releases.ubuntu.com/24.04/ubuntu-24.04.5.1-desktop-amd64.iso -w 8 -c 16\n\n")
		fmt.Fprintf(os.Stderr, "  # Custom destination file:\n")
		fmt.Fprintf(os.Stderr, "  beam -url \"https://example.com/data.tar.gz\" -o archive.tar.gz\n\n")
	}

	// Quick subcommand handling: beam demo / beam sample / beam help / beam version
	if len(args) > 0 {
		switch args[0] {
		case "help", "-h", "--help":
			fs.Usage()
			return nil
		case "demo", "sample":
			args = []string{"-demo"}
		case "version", "--version":
			fmt.Println("Beam v1.0.0 (Go 1.22+ RFC 7233 engine)")
			return nil
		}
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

	if version {
		fmt.Println("Beam v1.0.0 (Go 1.22+ RFC 7233 engine)")
		return nil
	}

	if demo {
		url = "https://github.com/Anshu666666/beam/releases/download/v1.0.0/beam-sample-50mb.bin"
		if outputFile == "" {
			outputFile = "beam-sample-50mb.bin"
		}
		if workers == 4 {
			workers = 8
		}
		if chunks == 4 {
			chunks = 16
		}
	} else if url == "" && len(fs.Args()) > 0 {
		// Positional URL support: beam https://...
		url = fs.Args()[0]
	}

	// Validate required argument
	if url == "" {
		fs.Usage()
		return fmt.Errorf("-url is required (or run 'beam -demo' for an instant test)")
	}

	// Print startup header banner
	// fmt.Println(a...):
	// - What it does: Formats using default formats for its operands and writes to standard output with a trailing newline.
	// - Returns: (int, error) -> Number of bytes written and any write error encountered.
	fmt.Println("┌────────────────────────────────────────────────────────────┐")
	fmt.Println("│  ⚡ BEAM · Concurrent RFC 7233 Streaming Engine (Go CLI)   │")
	fmt.Println("└────────────────────────────────────────────────────────────┘")

	resolvedOutput := outputFile
	if resolvedOutput == "" {
		if parsed, err := neturl.ParseRequestURI(url); err == nil {
			cleanPath := path.Clean(parsed.Path)
			base := path.Base(cleanPath)
			if base != "" && base != "." && base != "/" {
				resolvedOutput = base
			}
		}
		if resolvedOutput == "" {
			resolvedOutput = "downloaded_file"
		}
	}

	fmt.Printf("URL:        \033[36m%s\033[0m\n", url)
	if outputFile != "" {
		fmt.Printf("Output:     \033[33m%s\033[0m\n", outputFile)
	} else {
		fmt.Printf("Output:     \033[33m%s\033[0m \033[90m(auto-derived)\033[0m\n", resolvedOutput)
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

	absPath, err := filepath.Abs(resolvedOutput)
	if err != nil {
		absPath = resolvedOutput
	}

	fmt.Println("────────────────────────────────────────────────────────────")
	fmt.Printf("\033[1;32m✔ Download completed in %s!\033[0m\n", elapsed.Round(time.Millisecond))
	fmt.Printf("\033[1;33mSaved to:   \033[1;36m%s\033[0m\n", absPath)
	fmt.Println("============================================================")

	return nil
}
