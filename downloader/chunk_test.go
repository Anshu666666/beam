package downloader

import (
	"testing"
)

// TestCalculateChunks_ExactDivision tests partitioning when totalSize is cleanly divisible by numChunks.
func TestCalculateChunks_ExactDivision(t *testing.T) {
	totalSize := int64(100)
	numChunks := 4

	chunks := CalculateChunks(totalSize, numChunks)

	if len(chunks) != 4 {
		t.Fatalf("expected 4 chunks, got %d", len(chunks))
	}

	expected := []struct {
		start int64
		end   int64
	}{
		{0, 24},
		{25, 49},
		{50, 74},
		{75, 99},
	}

	for i, exp := range expected {
		if chunks[i].Index != i {
			t.Errorf("chunk %d: expected Index %d, got %d", i, i, chunks[i].Index)
		}
		if chunks[i].Start != exp.start || chunks[i].End != exp.end {
			t.Errorf("chunk %d: expected [%d, %d], got [%d, %d]", i, exp.start, exp.end, chunks[i].Start, chunks[i].End)
		}
		if chunks[i].Size() != (exp.end - exp.start + 1) {
			t.Errorf("chunk %d: expected size %d, got %d", i, exp.end-exp.start+1, chunks[i].Size())
		}
	}
}

// TestCalculateChunks_WithRemainder tests partitioning when there is a non-zero remainder.
// 100 / 3 = 33 with remainder 1. The final chunk must absorb the remainder.
func TestCalculateChunks_WithRemainder(t *testing.T) {
	totalSize := int64(100)
	numChunks := 3

	chunks := CalculateChunks(totalSize, numChunks)

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}

	expected := []struct {
		start int64
		end   int64
	}{
		{0, 32},  // 33 bytes
		{33, 65}, // 33 bytes
		{66, 99}, // 34 bytes (absorbs remainder)
	}

	for i, exp := range expected {
		if chunks[i].Start != exp.start || chunks[i].End != exp.end {
			t.Errorf("chunk %d: expected [%d, %d], got [%d, %d]", i, exp.start, exp.end, chunks[i].Start, chunks[i].End)
		}
	}
}

// TestCalculateChunks_SingleChunk verifies handling when only 1 chunk is requested.
func TestCalculateChunks_SingleChunk(t *testing.T) {
	totalSize := int64(500)
	numChunks := 1

	chunks := CalculateChunks(totalSize, numChunks)

	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	if chunks[0].Start != 0 || chunks[0].End != 499 {
		t.Errorf("expected [0, 499], got [%d, %d]", chunks[0].Start, chunks[0].End)
	}
}

// TestCalculateChunks_SizeSmallerThanChunks verifies that if totalSize is smaller than numChunks,
// the number of chunks is safely clamped to totalSize so no empty chunks are generated.
func TestCalculateChunks_SizeSmallerThanChunks(t *testing.T) {
	totalSize := int64(2)
	numChunks := 5

	chunks := CalculateChunks(totalSize, numChunks)

	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}
	if chunks[0].Start != 0 || chunks[0].End != 0 {
		t.Errorf("chunk 0 expected [0, 0], got [%d, %d]", chunks[0].Start, chunks[0].End)
	}
	if chunks[1].Start != 1 || chunks[1].End != 1 {
		t.Errorf("chunk 1 expected [1, 1], got [%d, %d]", chunks[1].Start, chunks[1].End)
	}
}

// TestCalculateChunks_ZeroOrNegativeSize verifies that zero or negative sizes return an empty slice.
func TestCalculateChunks_ZeroOrNegativeSize(t *testing.T) {
	if chunks := CalculateChunks(0, 4); len(chunks) != 0 {
		t.Errorf("expected 0 chunks for size 0, got %d", len(chunks))
	}
	if chunks := CalculateChunks(-50, 4); len(chunks) != 0 {
		t.Errorf("expected 0 chunks for negative size, got %d", len(chunks))
	}
}

// TestCalculateChunks_ContinuityAndCoverage tests a prime number size with an arbitrary chunk count
// to mathematically prove zero gaps, zero overlaps, and 100% byte coverage.
func TestCalculateChunks_ContinuityAndCoverage(t *testing.T) {
	totalSize := int64(1048577) // 1 MB + 1 byte
	numChunks := 7

	chunks := CalculateChunks(totalSize, numChunks)

	if len(chunks) != numChunks {
		t.Fatalf("expected %d chunks, got %d", numChunks, len(chunks))
	}

	var totalAccumulatedBytes int64

	for i := range chunks {
		totalAccumulatedBytes += chunks[i].Size()

		// Verify continuity with previous chunk
		if i > 0 {
			if chunks[i].Start != chunks[i-1].End+1 {
				t.Fatalf("gap or overlap between chunk %d and %d: prev end=%d, current start=%d",
					i-1, i, chunks[i-1].End, chunks[i].Start)
			}
		}
	}

	// Verify first byte is 0
	if chunks[0].Start != 0 {
		t.Errorf("expected start at byte 0, got %d", chunks[0].Start)
	}

	// Verify last byte is totalSize - 1
	if chunks[len(chunks)-1].End != totalSize-1 {
		t.Errorf("expected final end at %d, got %d", totalSize-1, chunks[len(chunks)-1].End)
	}

	// Verify total byte count matches
	if totalAccumulatedBytes != totalSize {
		t.Errorf("total byte coverage mismatch: expected %d, got %d", totalSize, totalAccumulatedBytes)
	}
}
