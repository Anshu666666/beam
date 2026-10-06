package downloader

// Chunk represents an isolated byte-range slice of the remote target file.
type Chunk struct {
	Index        int    // 0-based sequential sequence index of the chunk
	Start        int64  // Starting byte offset (inclusive, e.g. 0)
	End          int64  // Ending byte offset (inclusive, e.g. 1048575)
	TempFilePath string // Absolute or relative path to the temporary file on disk storing this chunk
}

// Size returns the total byte length of this chunk.
// In HTTP Range headers, byte ranges are inclusive on both ends,
// so a range of bytes=0-9 contains (9 - 0 + 1) = 10 bytes.
func (c Chunk) Size() int64 {
	// If the chunk bounds are invalid, return 0
	if c.End < c.Start {
		return 0
	}
	return c.End - c.Start + 1
}

// CalculateChunks partitions a total file byte size into numChunks contiguous,
// non-overlapping byte intervals.
//
// Rules enforced:
// 1. If totalSize <= 0, returns an empty slice.
// 2. If numChunks <= 0, defaults to 1 chunk.
// 3. If totalSize < numChunks, clamps numChunks to totalSize (avoids 0-byte chunks).
// 4. Any division remainder is automatically absorbed into the final chunk, ensuring
//    the sum of chunk lengths equals totalSize with 100% precision.
func CalculateChunks(totalSize int64, numChunks int) []Chunk {
	// Guard clause: files with zero or negative size have no downloadable ranges
	if totalSize <= 0 {
		return []Chunk{}
	}

	// Guard clause: at least 1 chunk is required
	if numChunks <= 0 {
		numChunks = 1
	}

	// Guard clause: if the file has fewer bytes than the requested chunk count
	// (e.g. 2 bytes requested across 5 chunks), clamp chunks to totalSize
	if totalSize < int64(numChunks) {
		numChunks = int(totalSize)
	}

	// make([]T, length, capacity):
	// - What it does: Allocates a new slice of type []Chunk backed by an array
	//   of size numChunks on the heap. Pre-allocating the exact size avoids
	//   costly slice reallocations and array copying during appending.
	// - Returns: []Chunk -> A pre-sized slice initialized with zero-value structs.
	chunks := make([]Chunk, numChunks)

	// Calculate base chunk size via integer division
	chunkSize := totalSize / int64(numChunks)

	for i := 0; i < numChunks; i++ {
		start := int64(i) * chunkSize
		end := start + chunkSize - 1

		// The final chunk absorbs any leftover remainder bytes so that
		// end is guaranteed to reach the last byte of the file (totalSize - 1).
		if i == numChunks-1 {
			end = totalSize - 1
		}

		chunks[i] = Chunk{
			Index: i,
			Start: start,
			End:   end,
		}
	}

	return chunks
}
