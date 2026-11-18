package main

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// artLines is the wordmark "BEAM" rendered with the figlet "smshadow" font
// (via github.com/common-nighthawk/go-figure, see the repo README for the
// technique) in the butter yellow from the Beam frontend palette
// (--color-butter #FCD34D). Each line is exactly 27 visible cells wide.
var artLines = []string{
	"\033[1;38;2;252;211;77m  _ )   __|     \\      \\  |\033[0m",
	"\033[1;38;2;252;211;77m  _ \\   _|     _ \\    |\\/ |\033[0m",
	"\033[1;38;2;252;211;77m ___/  ___|  _/  _\\  _|  _|\033[0m",
}

// artLineWidths[i] is the visible (cell) width of artLines[i], used to keep
// the side info column aligned.
var artLineWidths = []int{27, 27, 27}

const artWidth = 27
const artOffset = 3 // first info row where the art block begins (vertically centered)
const bannerGap = 3 // spaces between the art and the info column

// printBanner prints the neofetch-style Beam banner: neon wordmark on the
// left, spec sheet on the right. Falls back to a single plain line when
// output is piped or NO_COLOR is set (neofetch behaves the same way).
func printBanner() {
	printBannerTo(os.Stdout)
}

// printBannerTo is printBanner for an arbitrary destination (e.g. stderr when
// rendering command usage).
func printBannerTo(w io.Writer) {
	if !writerIsTerminal(w) || os.Getenv("NO_COLOR") != "" {
		fmt.Fprintln(w, "BEAM v1.0.0 — Concurrent RFC 7233 Streaming Engine (Go CLI)")
		return
	}

	label := func(s string) string { return "\033[90m" + s + "\033[0m" }
	value := func(s string) string { return "\033[1m" + s + "\033[0m" }

	info := []string{
		"\033[1;33mBEAM\033[0m \033[90m· Concurrent RFC 7233 Streaming Engine\033[0m",
		"\033[90m─────────────────────────────────────────\033[0m",
		label("Version   ") + value("v1.0.0"),
		label("Runtime   ") + value("Go 1.22+"),
		label("Protocol  ") + value("HTTP byte-range · RFC 7233"),
		label("Engine    ") + value("Worker pool · chunked ranges"),
		label("License   ") + value("MIT"),
		label("Repo      ") + value("github.com/Anshu666666/beam"),
	}

	for i, side := range info {
		left := strings.Repeat(" ", artWidth)
		if idx := i - artOffset; idx >= 0 && idx < len(artLines) {
			left = artLines[idx] + strings.Repeat(" ", artWidth-artLineWidths[idx])
		}
		fmt.Fprintln(w, left+strings.Repeat(" ", bannerGap)+side)
	}
}

// writerIsTerminal reports whether w is a character device (a real console),
// so piped/redirected output stays free of ANSI escape sequences.
func writerIsTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
