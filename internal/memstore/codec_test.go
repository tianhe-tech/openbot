package memstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"unicode/utf8"
)

// compressibleText builds a payload that is large enough to be compressed and
// repetitive enough that compression actually wins. Real conversation text is
// not this repetitive, but the point of these tests is the codec contract, not
// the ratio, and a payload that does not compress would silently exercise the
// raw path instead.
func compressibleText(repeats int) string {
	var builder strings.Builder
	for index := 0; index < repeats; index++ {
		builder.WriteString("the quick brown fox jumps over the lazy dog; ")
	}
	return builder.String()
}

// TestCompressBlockChoosesCodecBySizeAndRatio pins the decision rule itself,
// without a database in the way.
func TestCompressBlockChoosesCodecBySizeAndRatio(t *testing.T) {
	t.Run("small payload stays raw", func(t *testing.T) {
		text := []byte("short message")
		codec, payload, rawBytes, err := compressBlock(text)
		if err != nil {
			t.Fatal(err)
		}
		if codec != blockCodecRaw {
			t.Fatalf("codec=%q, want %q", codec, blockCodecRaw)
		}
		if rawBytes != len(text) {
			t.Fatalf("rawBytes=%d, want %d", rawBytes, len(text))
		}
		if !bytes.Equal(payload, text) {
			t.Fatal("raw payload was altered")
		}
	})

	t.Run("large compressible payload is compressed", func(t *testing.T) {
		text := []byte(compressibleText(400))
		if len(text) < blockCompressMinBytes {
			t.Fatalf("fixture is %d bytes, below the %d threshold", len(text), blockCompressMinBytes)
		}
		codec, payload, rawBytes, err := compressBlock(text)
		if err != nil {
			t.Fatal(err)
		}
		if codec != blockCodecZstd {
			t.Fatalf("codec=%q, want %q", codec, blockCodecZstd)
		}
		if rawBytes != len(text) {
			t.Fatalf("rawBytes=%d, want %d", rawBytes, len(text))
		}
		if len(payload) >= len(text) {
			t.Fatalf("compressed to %d bytes from %d", len(payload), len(text))
		}
	})

	t.Run("large incompressible payload stays raw", func(t *testing.T) {
		// A deterministic pseudo-random byte stream: no repeats for the encoder
		// to exploit, so the frame is larger than the input.
		text := make([]byte, 16*1024)
		state := uint32(12345)
		for index := range text {
			state = state*1664525 + 1013904223
			text[index] = byte(state >> 24)
		}
		codec, payload, rawBytes, err := compressBlock(text)
		if err != nil {
			t.Fatal(err)
		}
		if codec != blockCodecRaw {
			t.Fatalf("codec=%q for incompressible input, want %q", codec, blockCodecRaw)
		}
		if rawBytes != len(text) {
			t.Fatalf("rawBytes=%d, want %d", rawBytes, len(text))
		}
		if !bytes.Equal(payload, text) {
			t.Fatal("raw payload was altered")
		}
	})

	t.Run("payload above the decode ceiling stays raw", func(t *testing.T) {
		// The ceiling is enforced on write as well as read, so a payload that
		// could not be decoded is never written compressed in the first place.
		text := make([]byte, maxBlockDecodeBytes+1)
		codec, _, rawBytes, err := compressBlock(text)
		if err != nil {
			t.Fatal(err)
		}
		if codec != blockCodecRaw {
			t.Fatalf("codec=%q above the decode ceiling, want %q", codec, blockCodecRaw)
		}
		if rawBytes != len(text) {
			t.Fatalf("rawBytes=%d, want %d", rawBytes, len(text))
		}
	})
}

// TestDecodeBlockRoundTripsExactly is the round-trip gate. It covers the raw
// path, the compressed path, and the empty payload, and it compares bytes
// rather than strings so a difference in encoding cannot hide.
func TestDecodeBlockRoundTripsExactly(t *testing.T) {
	cases := map[string][]byte{
		"empty":        {},
		"ascii":        []byte("plain ascii text"),
		"cjk":          []byte(strings.Repeat("\u914d\u7f6e\u6587\u4ef6\u5939", 2000)),
		"emoji":        []byte(strings.Repeat("\U0001F600\U0001F680", 3000)),
		"mixed":        []byte(strings.Repeat("a\u4e2d\U0001F600b", 4000)),
		"large ascii":  []byte(compressibleText(500)),
		"nul bytes":    []byte("before\x00after"),
		"invalid utf8": {0xff, 0xfe, 0xfd, 0x00, 0x41},
	}
	for name, text := range cases {
		t.Run(name, func(t *testing.T) {
			codec, payload, rawBytes, err := compressBlock(text)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decodeBlock(codec, blockCodecVersion, int64(rawBytes), payload)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decoded, text) {
				t.Fatalf("round trip changed the payload: %d bytes in, %d bytes out", len(text), len(decoded))
			}
		})
	}
}

// TestDecodeBlockRejectsCorruption is the corruption-handling gate. Each case
// is a way a stored block can be wrong, and each must produce an error rather
// than plausible text.
func TestDecodeBlockRejectsCorruption(t *testing.T) {
	text := []byte(compressibleText(400))
	codec, payload, rawBytes, err := compressBlock(text)
	if err != nil {
		t.Fatal(err)
	}
	if codec != blockCodecZstd {
		t.Fatalf("fixture did not compress: codec=%q", codec)
	}

	t.Run("unknown codec", func(t *testing.T) {
		if _, err := decodeBlock("brotli", blockCodecVersion, int64(rawBytes), payload); err == nil {
			t.Fatal("unknown codec was accepted")
		}
	})

	t.Run("unsupported codec version", func(t *testing.T) {
		if _, err := decodeBlock(codec, blockCodecVersion+1, int64(rawBytes), payload); err == nil {
			t.Fatal("future codec version was accepted")
		}
	})

	t.Run("truncated frame", func(t *testing.T) {
		if _, err := decodeBlock(codec, blockCodecVersion, int64(rawBytes), payload[:len(payload)/2]); err == nil {
			t.Fatal("truncated frame decoded without error")
		}
	})

	t.Run("flipped payload byte", func(t *testing.T) {
		corrupt := append([]byte(nil), payload...)
		// Flip a byte in the middle of the frame, past the header, so the
		// frame checksum is what catches it.
		corrupt[len(corrupt)/2] ^= 0xff
		if _, err := decodeBlock(codec, blockCodecVersion, int64(rawBytes), corrupt); err == nil {
			t.Fatal("corrupted frame decoded without error")
		}
	})

	t.Run("declared length disagrees with frame", func(t *testing.T) {
		if _, err := decodeBlock(codec, blockCodecVersion, int64(rawBytes)+1, payload); err == nil {
			t.Fatal("length mismatch was accepted")
		}
	})

	t.Run("declared length above the ceiling", func(t *testing.T) {
		if _, err := decodeBlock(codec, blockCodecVersion, maxBlockDecodeBytes+1, payload); err == nil {
			t.Fatal("declared length above the ceiling was accepted")
		}
	})

	t.Run("negative declared length", func(t *testing.T) {
		if _, err := decodeBlock(codec, blockCodecVersion, -1, payload); err == nil {
			t.Fatal("negative declared length was accepted")
		}
	})

	t.Run("empty payload for a compressed block", func(t *testing.T) {
		if _, err := decodeBlock(codec, blockCodecVersion, int64(rawBytes), nil); err == nil {
			t.Fatal("empty compressed payload decoded without error")
		}
	})
}

// TestArchiveStoresLargeMessageCompressedAndReadsItBack is the end-to-end gate.
// It writes a message large enough to compress, proves the row really is
// compressed, and then reads the text back through every public read path.
func TestArchiveStoresLargeMessageCompressedAndReadsItBack(t *testing.T) {
	path, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()

	text := compressibleText(600)
	if len(text) < blockCompressMinBytes {
		t.Fatalf("fixture is %d bytes, below the %d threshold", len(text), blockCompressMinBytes)
	}
	message := archiveMessageForTest("large")
	message.Text = text
	if err := writer.RecordMessage(ctx, message); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}

	reader, err := openDatabaseMode(path, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// The row must actually be compressed, or the rest of this test proves
	// nothing about the compressed read path.
	var codec string
	var codecVersion int
	var rawBytes, storedBytes int64
	var checksum string
	if err := reader.db.QueryRow(`SELECT codec,codec_version,raw_bytes,stored_bytes,checksum
		FROM mem_v2_blocks`).Scan(&codec, &codecVersion, &rawBytes, &storedBytes, &checksum); err != nil {
		t.Fatal(err)
	}
	if codec != blockCodecZstd {
		t.Fatalf("codec=%q, want %q", codec, blockCodecZstd)
	}
	if codecVersion != blockCodecVersion {
		t.Fatalf("codec_version=%d, want %d", codecVersion, blockCodecVersion)
	}
	if rawBytes != int64(len(text)) {
		t.Fatalf("raw_bytes=%d, want %d", rawBytes, len(text))
	}
	if storedBytes >= rawBytes {
		t.Fatalf("stored_bytes=%d is not smaller than raw_bytes=%d", storedBytes, rawBytes)
	}
	// The checksum is over the original text, not the stored bytes, so it stays
	// meaningful after compression and can be verified on read.
	sum := sha256.Sum256([]byte(text))
	if checksum != hex.EncodeToString(sum[:]) {
		t.Fatal("checksum is not over the original text")
	}

	// GetMessages is the detail path.
	scope := SearchScope{Adapter: "test", UserID: "owner"}
	detail, err := reader.GetMessages(ctx, scope, []string{"large"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(detail.Messages) != 1 {
		t.Fatalf("got %d messages, want 1", len(detail.Messages))
	}
	if detail.Messages[0].Text != text {
		t.Fatal("detail text did not round trip")
	}
	if detail.Messages[0].ByteLength != len(text) {
		t.Fatalf("byte_length=%d, want %d", detail.Messages[0].ByteLength, len(text))
	}

	// Scroll is the session path.
	scrolled, err := reader.Scroll(ctx, scope, "session", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(scrolled.Messages) != 1 || scrolled.Messages[0].Text != text {
		t.Fatal("scroll text did not round trip")
	}
}

// TestArchiveStoresSmallMessageRaw proves the threshold is real: a small
// message must not pay compression, and its row must say so.
func TestArchiveStoresSmallMessageRaw(t *testing.T) {
	path, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()

	message := archiveMessageForTest("small")
	message.Text = "a short reply"
	if err := writer.RecordMessage(ctx, message); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}

	reader, err := openDatabaseMode(path, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var codec string
	var rawBytes, storedBytes int64
	if err := reader.db.QueryRow(`SELECT codec,raw_bytes,stored_bytes FROM mem_v2_blocks`).
		Scan(&codec, &rawBytes, &storedBytes); err != nil {
		t.Fatal(err)
	}
	if codec != blockCodecRaw {
		t.Fatalf("codec=%q for a small message, want %q", codec, blockCodecRaw)
	}
	if rawBytes != storedBytes {
		t.Fatalf("raw_bytes=%d stored_bytes=%d, want equal", rawBytes, storedBytes)
	}
}

// TestCompressedMessageIsSearchable proves the index path decodes before
// chunking. If it did not, a compressed message would be indexed as binary
// noise and become unfindable, which is the failure mode that would make
// compression a data-loss bug rather than a space saving.
func TestCompressedMessageIsSearchable(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()

	// A distinctive token buried in a large compressible body.
	marker := "zebracrossing"
	text := compressibleText(600) + " " + marker + " " + compressibleText(600)
	message := archiveMessageForTest("searchable")
	message.Text = text
	if err := writer.RecordMessage(ctx, message); err != nil {
		t.Fatal(err)
	}
	// Index before closing: IndexPending is a writer method, and the writer is
	// the only handle that can drain the job queue.
	if _, err := writer.IndexPending(ctx, maxIndexBatch); err != nil {
		t.Fatal(err)
	}

	result, err := writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner"},
		Query: marker,
		Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 {
		t.Fatalf("got %d hits for a token inside a compressed message, want 1", len(result.Hits))
	}
	if result.Hits[0].MessageID != "searchable" {
		t.Fatalf("hit=%q, want %q", result.Hits[0].MessageID, "searchable")
	}
	// The excerpt is cut from the decoded text, so it must contain the marker
	// and must be valid UTF-8.
	if !strings.Contains(result.Hits[0].Excerpt, marker) {
		t.Fatalf("excerpt %q does not contain the marker", result.Hits[0].Excerpt)
	}
	if !utf8.ValidString(result.Hits[0].Excerpt) {
		t.Fatal("excerpt is not valid UTF-8")
	}
}

// TestCompressedMessageOffsetsAreUTF8Safe is the UTF-8 offset gate. Chunk
// offsets are byte offsets into the decoded text, so they must still address
// the same bytes after a compress/decompress cycle.
func TestCompressedMessageOffsetsAreUTF8Safe(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()

	// Multi-byte runes throughout, so a byte offset that ignored rune
	// boundaries would land mid-character.
	text := strings.Repeat("\u914d\u7f6e\u6587\u4ef6\u5939\u4e2d\u6587\u5185\u5bb9 ", 800)
	message := archiveMessageForTest("offsets")
	message.Text = text
	if err := writer.RecordMessage(ctx, message); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.IndexPending(ctx, maxIndexBatch); err != nil {
		t.Fatal(err)
	}

	rows, err := writer.store.db.Query(`SELECT start_byte,end_byte FROM mem_v2_chunks
		WHERE message_id='offsets' ORDER BY ordinal`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	checked := 0
	for rows.Next() {
		var start, end int
		if err := rows.Scan(&start, &end); err != nil {
			t.Fatal(err)
		}
		if start < 0 || end > len(text) || start >= end {
			t.Fatalf("chunk offsets [%d,%d) are outside the %d-byte text", start, end, len(text))
		}
		if !utf8.ValidString(text[start:end]) {
			t.Fatalf("chunk [%d,%d) is not valid UTF-8", start, end)
		}
		checked++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if checked == 0 {
		t.Fatal("no chunks were written, so no offsets were checked")
	}
}

// TestBlockCodecSummaryReportsSavings proves the health report's numbers are
// derived from the rows rather than assumed.
func TestBlockCodecSummaryReportsSavings(t *testing.T) {
	path, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()

	small := archiveMessageForTest("small")
	small.Text = "tiny"
	small.Sequence = 0
	if err := writer.RecordMessage(ctx, small); err != nil {
		t.Fatal(err)
	}
	large := archiveMessageForTest("large")
	large.Text = compressibleText(600)
	// Both messages share one session, so they need distinct sequences.
	large.Sequence = 1
	if err := writer.RecordMessage(ctx, large); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}

	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stats, err := store.BlockCodecStats(ctx, SearchScope{Adapter: "test", UserID: "owner"})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Blocks != 2 {
		t.Fatalf("blocks=%d, want 2", stats.Blocks)
	}
	if stats.RawBlocks != 1 || stats.ZstdBlocks != 1 {
		t.Fatalf("raw=%d zstd=%d, want 1 and 1", stats.RawBlocks, stats.ZstdBlocks)
	}
	if stats.SavedBytes() <= 0 {
		t.Fatalf("saved=%d bytes, want a positive saving", stats.SavedBytes())
	}
	if stats.CompressedRaw != int64(len(large.Text)) {
		t.Fatalf("compressed_raw=%d, want %d", stats.CompressedRaw, len(large.Text))
	}
}
