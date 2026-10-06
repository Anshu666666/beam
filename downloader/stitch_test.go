package downloader

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestStitchChunks_Success verifies that multiple chunk files are assembled
// into one continuous output file in exact index order, and that all temporary
// part files are cleanly deleted from disk afterwards.
func TestStitchChunks_Success(t *testing.T) {
	// Create 3 temporary part files simulating downloaded chunks
	part1, err := os.CreateTemp("", "part_0_*")
	if err != nil {
		t.Fatalf("failed to create part 0: %v", err)
	}
	part1.Write([]byte("Chunk 0: [Header] "))
	part1Path := part1.Name()
	part1.Close()

	part2, err := os.CreateTemp("", "part_1_*")
	if err != nil {
		t.Fatalf("failed to create part 1: %v", err)
	}
	part2.Write([]byte("Chunk 1: [Payload] "))
	part2Path := part2.Name()
	part2.Close()

	part3, err := os.CreateTemp("", "part_2_*")
	if err != nil {
		t.Fatalf("failed to create part 2: %v", err)
	}
	part3.Write([]byte("Chunk 2: [Footer]"))
	part3Path := part3.Name()
	part3.Close()

	chunks := []Chunk{
		{Index: 0, TempFilePath: part1Path},
		{Index: 1, TempFilePath: part2Path},
		{Index: 2, TempFilePath: part3Path},
	}

	outFile, err := os.CreateTemp("", "stitched_output_*")
	if err != nil {
		t.Fatalf("failed to create output file: %v", err)
	}
	outPath := outFile.Name()
	outFile.Close()
	defer os.Remove(outPath)

	err = StitchChunks(chunks, outPath)
	if err != nil {
		t.Fatalf("StitchChunks failed: %v", err)
	}

	// Verify assembled file content on disk
	actualContent, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read stitched output: %v", err)
	}

	expectedContent := "Chunk 0: [Header] Chunk 1: [Payload] Chunk 2: [Footer]"
	if !bytes.Equal(actualContent, []byte(expectedContent)) {
		t.Errorf("stitched content mismatch: expected '%s', got '%s'",
			expectedContent, string(actualContent))
	}

	// Verify all temporary chunk part files were cleanly deleted
	for _, chunk := range chunks {
		_, err := os.Stat(chunk.TempFilePath)
		if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("temp file %s was not deleted after stitching (err=%v)", chunk.TempFilePath, err)
		}
	}
}

// TestStitchChunks_EmptyChunks verifies error handling when no chunks are passed.
func TestStitchChunks_EmptyChunks(t *testing.T) {
	err := StitchChunks([]Chunk{}, "some_file.txt")
	if err == nil {
		t.Fatal("expected error for empty chunks slice, got nil")
	}
}

// TestStitchChunks_MissingFile verifies error handling when one of the chunk files is missing on disk.
func TestStitchChunks_MissingFile(t *testing.T) {
	chunks := []Chunk{
		{Index: 0, TempFilePath: "non_existent_part_file.tmp"},
	}

	err := StitchChunks(chunks, filepath.Join(t.TempDir(), "out.tmp"))
	if err == nil {
		t.Fatal("expected error for missing chunk file, got nil")
	}
}
