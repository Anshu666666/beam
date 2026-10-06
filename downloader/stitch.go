package downloader

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// StitchChunks combines sequentially ordered temporary chunk files into one final
// destination file using io.MultiReader and io.Copy.
//
// Key Engineering Properties:
// 1. Zero-RAM File Assembly: io.MultiReader chains multiple readers into a single virtual
//    continuous stream. io.Copy streams 32 KB at a time directly from chunk file descriptors
//    into the destination file descriptor without ever holding the full file in memory.
// 2. Safe OS File Handles: All open file descriptors are explicitly closed before calling
//    os.Remove. (On Windows, attempting to delete an open file triggers "Access is denied").
// 3. Automated Cleanup: Temporary chunk files are safely deleted once stitching succeeds.
func StitchChunks(chunks []Chunk, outputPath string) error {
	// Guard clause: ensure at least one chunk exists
	if len(chunks) == 0 {
		return fmt.Errorf("no chunks provided for stitching")
	}

	// Guard clause: ensure output path is not empty
	if outputPath == "" {
		return fmt.Errorf("output path cannot be empty")
	}

	// filepath.Dir(path):
	// - What it does: Returns all but the last element of path, typically the path's directory.
	// - Returns: string -> Directory portion of path.
	dir := filepath.Dir(outputPath)
	if dir != "" && dir != "." {
		// os.MkdirAll(path, perm):
		// - What it does: Creates directory path along with any necessary parents (mkdir -p).
		// - Returns: error -> Error if directory creation fails.
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create destination directory %q: %w", dir, err)
		}
	}

	// os.Create(name):
	// - What it does: Creates or truncates the named file. If the file exists, it is truncated.
	//   If it does not exist, it is created with mode 0666 (before umask).
	// - Returns: (*os.File, error) -> Pointer to open destination file or error.
	destFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create destination output file %q: %w", outputPath, err)
	}

	// Track open chunk files so we can guarantee all file descriptors are closed
	openFiles := make([]*os.File, len(chunks))
	readers := make([]io.Reader, len(chunks))

	// Open each chunk file sequentially:
	for i, chunk := range chunks {
		if chunk.TempFilePath == "" {
			destFile.Close()
			closeAllFiles(openFiles)
			return fmt.Errorf("chunk %d has empty TempFilePath", chunk.Index)
		}

		// os.Open(name):
		// - What it does: Opens named file for reading (read-only mode).
		// - Returns: (*os.File, error) -> Open file handle or filesystem error.
		f, err := os.Open(chunk.TempFilePath)
		if err != nil {
			destFile.Close()
			closeAllFiles(openFiles)
			return fmt.Errorf("failed to open chunk file %d (%q): %w", chunk.Index, chunk.TempFilePath, err)
		}

		openFiles[i] = f
		readers[i] = f
	}

	// io.MultiReader(readers...):
	// - What it does: Returns an io.Reader that is the logical concatenation of the provided
	//   input readers. They're read sequentially. Once the first reader reaches io.EOF,
	//   MultiReader moves to the second reader, and so on, until all readers reach EOF.
	// - Returns: io.Reader -> A composite sequential virtual reader.
	combinedReader := io.MultiReader(readers...)

	// io.Copy(dst, src):
	// - What it does: Copies 32 KB at a time from combinedReader to destFile until EOF.
	//   The whole file is NEVER buffered in RAM.
	// - Returns: (int64, error) -> Total bytes written and any error.
	_, copyErr := io.Copy(destFile, combinedReader)

	// Close the destination file:
	closeDestErr := destFile.Close()

	// Explicitly close all chunk file handles BEFORE deleting them:
	closeAllFiles(openFiles)

	if copyErr != nil {
		return fmt.Errorf("failed during chunk stitching copy: %w", copyErr)
	}
	if closeDestErr != nil {
		return fmt.Errorf("failed to finalize destination file: %w", closeDestErr)
	}

	// Clean up temporary chunk files from disk:
	for _, chunk := range chunks {
		// os.Remove(name):
		// - What it does: Deletes the named file or empty directory from the filesystem.
		// - Returns: error -> Error if removal fails.
		if err := os.Remove(chunk.TempFilePath); err != nil && !os.IsNotExist(err) {
			// Non-fatal warning if temp cleanup fails (file is already stitched)
			fmt.Printf("warning: failed to remove temporary chunk file %q: %v\n", chunk.TempFilePath, err)
		}
	}

	return nil
}

// closeAllFiles closes every non-nil *os.File in the slice.
func closeAllFiles(files []*os.File) {
	for _, f := range files {
		if f != nil {
			f.Close()
		}
	}
}
