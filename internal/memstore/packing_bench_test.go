package memstore

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

// This file answers the question the plan asks before block packing is built:
// "benchmark per-message compression against session-local 32-128 KiB blocks".
//
// It is a measurement, not an implementation. The reason to measure first is
// that packing is not free in either direction. It can only save space on
// payloads that are currently stored raw because they are below the 4 KiB
// compression threshold, and it costs read amplification on every fetch,
// because reading one 200-byte message out of a 64 KiB packed block means
// decompressing all 64 KiB. If the space saving is small, the trade is bad and
// the correct outcome is to not build it.
//
// The corpus is deliberately shaped like a real conversation rather than like a
// benchmark: many short turns, a few long answers, and the occasional large tool
// output. A corpus of uniformly large messages would make packing look useless
// (they are already compressed individually) and a corpus of uniformly tiny
// messages would make it look free (they compress well together). Neither is a
// conversation.

// conversationMessage is one archived message in the synthetic corpus.
type conversationMessage struct {
	role string
	text string
}

// conversationCorpusForBench builds a conversation of roughly totalBytes.
//
// The size mix is the point. Real chat is dominated by short turns: a question
// of one or two sentences, an answer of a paragraph, an occasional long
// explanation, and a tool output that dwarfs everything else. The proportions
// below are chosen so the corpus is dominated by messages that the current
// write path stores raw, because those are the only messages packing can help.
func conversationCorpusForBench(totalBytes int, seed int) []conversationMessage {
	var messages []conversationMessage
	index := 0
	for size := 0; size < totalBytes; index++ {
		var message conversationMessage
		switch index % 10 {
		case 0, 1, 2, 3:
			// Short user turns: a question, a follow-up, a correction.
			message = conversationMessage{"user", variedTextForBench(120+index%80, seed+index)}
		case 4, 5, 6:
			// Typical assistant answers, one to three paragraphs.
			message = conversationMessage{"assistant", variedTextForBench(600+index%900, seed+index)}
		case 7, 8:
			// A long explanation or a code block.
			message = conversationMessage{"assistant", variedTextForBench(3000+index%2000, seed+index)}
		default:
			// A tool output. These are already above the compression threshold
			// and are already compressed individually, so they are the part of
			// the corpus packing cannot improve.
			message = conversationMessage{"tool", variedTextForBench(20*1024+index%4096, seed+index)}
		}
		messages = append(messages, message)
		size += len(message.text)
	}
	return messages
}

// storedBytesPerMessage reports what the current write path stores for one
// message: the compressed payload when it is large enough to compress, and the
// raw bytes when it is not.
func storedBytesPerMessage(text string) int {
	_, stored, _, err := compressBlock([]byte(text))
	if err != nil {
		panic(err)
	}
	return len(stored)
}

// packMessages concatenates messages into blocks of at most blockBytes and
// returns the compressed size of each block.
//
// The separator is a single newline rather than a length prefix, because the
// real format would carry offsets in mem_v2_messages.byte_offset and
// byte_length, which the schema already has. The separator only has to keep two
// adjacent messages from being read as one token, which is what a compressor
// would otherwise exploit to claim a saving that the format does not actually
// deliver.
func packMessages(messages []conversationMessage, blockBytes int) []int {
	var blocks []int
	var builder strings.Builder
	flush := func() {
		if builder.Len() == 0 {
			return
		}
		_, stored, _, err := compressBlock([]byte(builder.String()))
		if err != nil {
			panic(err)
		}
		blocks = append(blocks, len(stored))
		builder.Reset()
	}
	for _, message := range messages {
		// A message larger than the block target gets its own block rather than
		// being split, because splitting would put one message's bytes in two
		// blocks and make the offset arithmetic in the read path conditional.
		if builder.Len() > 0 && builder.Len()+len(message.text) > blockBytes {
			flush()
		}
		builder.WriteString(message.text)
		builder.WriteString("\n")
	}
	flush()
	return blocks
}

// TestPackingSpaceSavingIsMeasured is the gate for whether block packing is
// worth building at all.
//
// It is a test rather than only a benchmark because the answer is a decision,
// and a decision recorded in a benchmark's output is a decision nobody re-runs.
// The assertion is deliberately weak: it requires only that the measurement be
// internally consistent, not that packing win. If packing does not win, the
// correct response is to record the number and not build it, and a test that
// failed in that case would be asserting a conclusion instead of measuring one.
func TestPackingSpaceSavingIsMeasured(t *testing.T) {
	const corpusBytes = 4 * 1024 * 1024
	messages := conversationCorpusForBench(corpusBytes, 7)

	rawTotal := 0
	perMessageTotal := 0
	rawCount := 0
	for _, message := range messages {
		rawTotal += len(message.text)
		perMessageTotal += storedBytesPerMessage(message.text)
		if len(message.text) < blockCompressMinBytes {
			rawCount++
		}
	}
	if rawCount == 0 {
		t.Fatal("corpus has no messages below the compression threshold; packing cannot help it and the measurement is meaningless")
	}

	type packingResult struct {
		blockBytes int
		stored     int
		blocks     int
	}
	var results []packingResult
	for _, blockBytes := range []int{32 * 1024, 64 * 1024, 128 * 1024} {
		blocks := packMessages(messages, blockBytes)
		stored := 0
		for _, size := range blocks {
			stored += size
		}
		results = append(results, packingResult{blockBytes, stored, len(blocks)})
	}

	t.Logf("corpus: %d messages, %d bytes raw, %d below the %d-byte threshold",
		len(messages), rawTotal, rawCount, blockCompressMinBytes)
	t.Logf("current per-message storage: %d bytes (%.1f%% of raw)",
		perMessageTotal, 100*float64(perMessageTotal)/float64(rawTotal))
	for _, result := range results {
		saving := perMessageTotal - result.stored
		t.Logf("packed into %6d-byte blocks: %d blocks, %d bytes (%.1f%% of raw), %+d bytes vs per-message (%+.1f%%)",
			result.blockBytes, result.blocks, result.stored,
			100*float64(result.stored)/float64(rawTotal),
			-saving, -100*float64(saving)/float64(perMessageTotal))
	}

	// The measurement has to be self-consistent before its conclusion can be
	// trusted: a packed total larger than the raw corpus would mean the
	// compressor expanded the data, which is possible for incompressible input
	// but not for this fixture, and would mean the fixture is wrong.
	for _, result := range results {
		if result.stored > rawTotal {
			t.Fatalf("packing into %d-byte blocks stored %d bytes, more than the %d-byte raw corpus; the fixture is not compressible text",
				result.blockBytes, result.stored, rawTotal)
		}
		if result.blocks == 0 {
			t.Fatalf("packing into %d-byte blocks produced no blocks", result.blockBytes)
		}
	}
}

// BenchmarkPackedBlockReadAmplification measures what packing costs on the read
// path, which is the half of the trade the space measurement does not show.
//
// Reading one message out of a packed block means decompressing the whole
// block. The current format decompresses only the message, and for a message
// below the threshold that is a slice copy rather than a decode. So packing
// converts a free read into a decode whose cost is set by the block size rather
// than by the message size, and the ratio between the two is the price.
//
// The single-message rows below are the worst case and they are reported
// because they are the number that looks alarming: a 200-byte message read out
// of a 64 KiB block pays 300 times its own size. But a detail fetch does not
// read one message, it reads a page of them, and a page that shares a block
// pays the decode once. BenchmarkPackedBlockPageRead is the comparison that
// decides, and this one exists to bound the case where a caller asks for a
// single message that happens to live in a large block.
func BenchmarkPackedBlockReadAmplification(b *testing.B) {
	messages := conversationCorpusForBench(1024*1024, 11)
	// Pick a short message, because that is the case packing makes worse: a
	// 200-byte message read out of a 64 KiB block pays 300 times its own size.
	var target conversationMessage
	for _, message := range messages {
		if len(message.text) < 512 {
			target = message
			break
		}
	}
	if target.text == "" {
		b.Fatal("corpus has no short message")
	}

	b.Run("current_per_message", func(b *testing.B) {
		codec, stored, rawBytes, err := compressBlock([]byte(target.text))
		if err != nil {
			b.Fatal(err)
		}
		if codec != blockCodecRaw {
			b.Fatalf("target message is compressed (%q); the row is meant to measure the raw path", codec)
		}
		b.SetBytes(int64(len(target.text)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			decoded, err := decodeBlock(codec, blockCodecVersion, int64(rawBytes), stored)
			if err != nil {
				b.Fatal(err)
			}
			if len(decoded) != len(target.text) {
				b.Fatalf("decoded %d bytes, want %d", len(decoded), len(target.text))
			}
		}
	})

	for _, blockBytes := range []int{32 * 1024, 64 * 1024, 128 * 1024} {
		// Build one packed block containing the target message, then measure
		// decoding it and slicing the message back out.
		var builder strings.Builder
		offset := 0
		targetOffset := -1
		for _, message := range messages {
			if builder.Len()+len(message.text) > blockBytes {
				break
			}
			if message.text == target.text && targetOffset < 0 {
				targetOffset = offset
			}
			builder.WriteString(message.text)
			builder.WriteString("\n")
			offset += len(message.text) + 1
		}
		if targetOffset < 0 {
			b.Fatalf("%d-byte block does not contain the target message", blockBytes)
		}
		blockText := builder.String()
		codec, stored, rawBytes, err := compressBlock([]byte(blockText))
		if err != nil {
			b.Fatal(err)
		}
		if codec != blockCodecZstd {
			b.Fatalf("%d-byte block chose codec %q, want %q", blockBytes, codec, blockCodecZstd)
		}
		b.Run(fmt.Sprintf("packed_%dKiB", blockBytes/1024), func(b *testing.B) {
			// SetBytes reports the block size, not the message size, because the
			// work done is proportional to the block. Reporting the message size
			// would make packing look efficient by dividing by the wrong number.
			b.SetBytes(int64(len(blockText)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				decoded, err := decodeBlock(codec, blockCodecVersion, int64(rawBytes), stored)
				if err != nil {
					b.Fatal(err)
				}
				end := targetOffset + len(target.text)
				if end > len(decoded) {
					b.Fatalf("target message runs past the decoded block: %d > %d", end, len(decoded))
				}
				if string(decoded[targetOffset:end]) != target.text {
					b.Fatal("sliced message does not match the original")
				}
			}
		})
	}
}

// BenchmarkPackedBlockPageRead is the comparison that decides whether packing
// is worth building, because it measures the operation a user actually waits
// for: fetching a page of a conversation.
//
// The single-message benchmark above makes packing look catastrophic, and it is
// misleading on its own. A detail fetch returns a page of messages, and under
// packing those messages share blocks, so the decode is paid once per block
// rather than once per message. The honest comparison is therefore "decode N
// messages individually" against "decode the blocks that hold those N
// messages", and that is what this benchmark does.
//
// The page size is 20, which is defaultScrollLimit and the order of magnitude a
// detail fetch returns. The corpus is the same conversation fixture, so the
// messages in a page have the size mix a real page has.
func BenchmarkPackedBlockPageRead(b *testing.B) {
	const pageSize = 20
	messages := conversationCorpusForBench(1024*1024, 17)
	if len(messages) < pageSize {
		b.Fatalf("corpus has %d messages, need %d", len(messages), pageSize)
	}
	// Take a page from the middle so it is not dominated by the corpus's first
	// few messages, which the generator makes short.
	page := messages[len(messages)/2 : len(messages)/2+pageSize]

	b.Run("current_per_message", func(b *testing.B) {
		type encoded struct {
			codec      string
			stored     []byte
			rawBytes   int64
			wantLength int
		}
		encodedPage := make([]encoded, len(page))
		for index, message := range page {
			codec, stored, rawBytes, err := compressBlock([]byte(message.text))
			if err != nil {
				b.Fatal(err)
			}
			encodedPage[index] = encoded{codec, stored, int64(rawBytes), len(message.text)}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			total := 0
			for _, item := range encodedPage {
				decoded, err := decodeBlock(item.codec, blockCodecVersion, item.rawBytes, item.stored)
				if err != nil {
					b.Fatal(err)
				}
				if len(decoded) != item.wantLength {
					b.Fatalf("decoded %d bytes, want %d", len(decoded), item.wantLength)
				}
				total += len(decoded)
			}
			if total == 0 {
				b.Fatal("page decoded to nothing")
			}
		}
	})

	for _, blockBytes := range []int{32 * 1024, 64 * 1024, 128 * 1024} {
		// Pack the whole corpus, then find the blocks that hold the page. A page
		// of 20 messages spans one or two blocks at these sizes, which is the
		// point: the decode count drops from 20 to 1 or 2.
		type packedBlock struct {
			codec    string
			stored   []byte
			rawBytes int64
			length   int
		}
		var blocks []packedBlock
		var builder strings.Builder
		flush := func() {
			if builder.Len() == 0 {
				return
			}
			text := builder.String()
			codec, stored, rawBytes, err := compressBlock([]byte(text))
			if err != nil {
				b.Fatal(err)
			}
			blocks = append(blocks, packedBlock{codec, stored, int64(rawBytes), len(text)})
			builder.Reset()
		}
		// Record which block each message lands in, so the page's blocks can be
		// identified without re-deriving the packing.
		blockOf := make([]int, len(messages))
		for index, message := range messages {
			if builder.Len() > 0 && builder.Len()+len(message.text) > blockBytes {
				flush()
			}
			blockOf[index] = len(blocks)
			builder.WriteString(message.text)
			builder.WriteString("\n")
		}
		flush()

		first := blockOf[len(messages)/2]
		last := blockOf[len(messages)/2+pageSize-1]
		pageBlocks := blocks[first : last+1]
		if len(pageBlocks) == 0 {
			b.Fatalf("%d-byte blocks: page spans no blocks", blockBytes)
		}
		b.Run(fmt.Sprintf("packed_%dKiB", blockBytes/1024), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				total := 0
				for _, block := range pageBlocks {
					decoded, err := decodeBlock(block.codec, blockCodecVersion, block.rawBytes, block.stored)
					if err != nil {
						b.Fatal(err)
					}
					if len(decoded) != block.length {
						b.Fatalf("decoded %d bytes, want %d", len(decoded), block.length)
					}
					total += len(decoded)
				}
				if total == 0 {
					b.Fatal("page decoded to nothing")
				}
			}
		})
		b.Logf("%d-byte blocks: a %d-message page spans %d block(s)", blockBytes, pageSize, len(pageBlocks))
	}
}

// TestPackedBlockOffsetsAreRuneSafe pins the property the read path depends on
// if packing is ever built: a message boundary must fall on a rune boundary.
//
// The schema stores byte_offset and byte_length, and the detail path reports
// UTF-8 offsets to callers. If a block boundary could split a multi-byte rune,
// then a message's stored offset would point into the middle of a character and
// every consumer that slices the decoded block would produce invalid UTF-8.
// Packing whole messages rather than fixed-size chunks is what makes this true,
// and this test states it so a later implementation that switches to fixed-size
// chunking has to confront it.
func TestPackedBlockOffsetsAreRuneSafe(t *testing.T) {
	messages := conversationCorpusForBench(256*1024, 13)
	for _, blockBytes := range []int{32 * 1024, 64 * 1024, 128 * 1024} {
		var builder strings.Builder
		offset := 0
		for _, message := range messages {
			if builder.Len() > 0 && builder.Len()+len(message.text) > blockBytes {
				break
			}
			// The offset recorded for this message is where it starts in the
			// assembled block, which is the current end of the buffer. Both
			// facts have to hold for a reader to slice the block by offset:
			// the offset must equal the assembled length, and the byte at that
			// offset must begin a rune. The second is checked on the message
			// itself rather than on the assembled buffer, because the offset is
			// one past the buffer's end until the message is appended.
			if offset != builder.Len() {
				t.Fatalf("%d-byte blocks: offset %d does not match assembled length %d", blockBytes, offset, builder.Len())
			}
			if !utf8.ValidString(message.text) {
				t.Fatalf("%d-byte blocks: message at offset %d is not valid UTF-8", blockBytes, offset)
			}
			if !utf8.RuneStart(message.text[0]) {
				t.Fatalf("%d-byte blocks: message at offset %d starts mid-rune", blockBytes, offset)
			}
			builder.WriteString(message.text)
			builder.WriteString("\n")
			offset += len(message.text) + 1
		}
		if builder.Len() == 0 {
			t.Fatalf("%d-byte blocks: no messages fit", blockBytes)
		}
		// The assembled block must itself be valid UTF-8, which is what makes
		// every recorded offset a legal slice point.
		if !utf8.ValidString(builder.String()) {
			t.Fatalf("%d-byte blocks: assembled block is not valid UTF-8", blockBytes)
		}
	}
}
