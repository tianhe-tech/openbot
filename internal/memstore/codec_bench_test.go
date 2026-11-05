package memstore

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// variedTextForBench builds size bytes of text that is distinct throughout.
//
// The first version of this benchmark built the larger rows by repeating a
// 4 KiB body, and the result was an inversion: the 4 KiB row cost 110 µs while
// the 16 KiB row cost 62 µs. That is not the codec being fast on large input,
// it is zstd finding the repetition immediately and spending almost no time on
// the second, third, and fourth copies. A repeated fixture measures the
// compressor's ability to detect a repeat, not its cost on a real long answer,
// and a real long answer does not contain the same paragraph four times.
//
// So each row gets its own vocabulary walk, seeded differently, and the text is
// assembled from many distinct words rather than one repeated block.
//
// That fixed the inversion but not the whole story. After the fixture change
// the rows were still non-monotonic, and the remaining cause was not the
// fixture at all: it was the encoder's first-call table allocation being
// charged to whichever row ran first. Both benchmarks now warm the codec before
// the timer starts. With that in place the per-byte throughput is deliberately
// not monotonic in size — a dictionary compressor amortizes its table build
// over the input, so small blocks look slower per byte than large ones. The
// number that matters is the absolute per-block cost against the capture
// deadline, not the MB/s column.
func variedTextForBench(size int, seed int) string {
	var builder strings.Builder
	builder.Grow(size + 64)
	for index := 0; builder.Len() < size; index++ {
		if index%29 == 28 {
			builder.WriteString(benchCJKWord(seed*977 + index))
		} else {
			builder.WriteString(benchWord(seed*977 + index))
		}
		if index%11 == 10 {
			builder.WriteString(".\n")
		} else {
			builder.WriteByte(' ')
		}
	}
	// Trim to an exact size on a rune boundary so the fixture is valid UTF-8,
	// which is what the capture path requires of real text.
	text := builder.String()
	if len(text) <= size {
		return text
	}
	cut := size
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut]
}

// BenchmarkCompressBlockCost measures what the codec adds to the capture path.
//
// The plan says "compress finalized large messages asynchronously". That is the
// right shape eventually, but it is not free to adopt: compressing after the
// commit means writing the block raw, then rewriting it compressed in a second
// transaction that repoints the message and removes the old block. That is
// exactly the crash-safe repacking the PR 6 gate names and that is not built
// yet. Doing the compression asynchronously without the repack machinery would
// trade a bounded, measured CPU cost on the capture path for an unbounded,
// unmeasured window in which a crash leaves a block half-rewritten.
//
// So the question is not "is synchronous compression ideal" but "is the cost it
// adds small enough to be the right choice until repacking exists". This
// benchmark answers that with a number instead of an opinion.
//
// The comparison that matters is against the capture deadline, not against
// zero. Capture commits are measured at 329 ms worst case in a 40k archive
// against a 2 s deadline, so a per-block cost in the tens of microseconds is
// three orders of magnitude below the budget it competes for.
func BenchmarkCompressBlockCost(b *testing.B) {
	cases := []struct {
		name string
		text string
		// wantCodec is the codec this row must actually exercise. Without it a
		// row can silently measure the wrong branch: a fixture that fails to
		// compress reports the bail-out path's cost under a name that claims to
		// measure compression, and nothing in the output reveals the swap.
		wantCodec string
	}{
		// The common case: a short reply, stored raw without touching the
		// encoder at all. This row should be effectively free, and it is the
		// row that proves the threshold is doing its job.
		{"short_reply_200B", strings.Repeat("ok ", 66), blockCodecRaw},
		// Just under the threshold, so the encoder is still not entered.
		{"below_threshold_4KiB", variedTextForBench(4000, 1), blockCodecRaw},
		// The smallest payload that is actually compressed.
		{"at_threshold_4KiB", variedTextForBench(4096, 2), blockCodecZstd},
		// A typical long assistant answer.
		{"long_answer_16KiB", variedTextForBench(16*1024, 3), blockCodecZstd},
		// A large tool output, the payload class the plan calls out for dedupe.
		{"tool_output_64KiB", variedTextForBench(64*1024, 4), blockCodecZstd},
		// The worst case the writer will accept. submit() rejects an operation
		// larger than QueueBytes (16 MiB), so a single message can be this big
		// and its whole compression cost lands inside one commit against the
		// fixed 2 s deadline. This row exists so the worst case is measured
		// rather than extrapolated from the 64 KiB row.
		{"max_message_1MiB", variedTextForBench(1024*1024, 5), blockCodecZstd},
		// Incompressible input: the encoder runs and its output is discarded.
		// This is the worst case for the write path, because the CPU is spent
		// and the archive gains nothing.
		{"incompressible_16KiB", incompressibleTextForTest(16 * 1024), blockCodecRaw},
	}
	for _, testCase := range cases {
		payload := []byte(testCase.text)
		// Check the precondition once, outside the timed loop, so a fixture that
		// stops compressing fails loudly instead of quietly changing what the
		// row measures.
		if codec, _, _, err := compressBlock(payload); err != nil {
			b.Fatalf("%s: %v", testCase.name, err)
		} else if codec != testCase.wantCodec {
			b.Fatalf("%s: fixture chose codec %q, want %q", testCase.name, codec, testCase.wantCodec)
		}
		b.Run(testCase.name, func(b *testing.B) {
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			// Warm the encoder before the timer starts. The first EncodeAll on a
			// fresh encoder allocates its match tables, which at 4 KiB is 299 KB
			// and 28 allocations against a steady-state 4 KB and 1 allocation.
			// Without this the first row of the run reports that one-time cost as
			// if it were the per-block cost, and the row ordering inverts: a
			// 20-iteration run showed 4 KiB at 435 µs and 16 KiB at 353 µs, which
			// reads as "compression gets cheaper with size" and is purely the
			// warm-up landing on whichever row ran first.
			if _, _, _, err := compressBlock(payload); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				codec, stored, rawBytes, err := compressBlock(payload)
				if err != nil {
					b.Fatal(err)
				}
				// Consume the results so the compiler cannot elide the work.
				if rawBytes != len(payload) || len(stored) == 0 || codec == "" {
					b.Fatalf("unexpected result: codec=%q stored=%d raw=%d", codec, len(stored), rawBytes)
				}
			}
		})
	}
}

// BenchmarkDecodeBlockCost measures the read side, which is paid on every
// detail fetch, scroll page, and scan fallback. It is reported separately
// because a codec that is cheap to write and expensive to read would move cost
// from the path that must not fail onto the path a user waits for.
func BenchmarkDecodeBlockCost(b *testing.B) {
	cases := []struct {
		name string
		text string
	}{
		{"short_reply_200B", strings.Repeat("ok ", 66)},
		{"at_threshold_4KiB", variedTextForBench(4096, 2)},
		{"long_answer_16KiB", variedTextForBench(16*1024, 3)},
		{"tool_output_64KiB", variedTextForBench(64*1024, 4)},
	}
	for _, testCase := range cases {
		payload := []byte(testCase.text)
		codec, stored, rawBytes, err := compressBlock(payload)
		if err != nil {
			b.Fatal(err)
		}
		// The short row is stored raw, so this benchmark would be timing a
		// slice copy under a name that claims to measure decoding. Assert the
		// row is compressed so the number means what the label says.
		if codec != blockCodecZstd && testCase.name != "short_reply_200B" {
			b.Fatalf("%s: fixture chose codec %q, want %q", testCase.name, codec, blockCodecZstd)
		}
		b.Run(testCase.name, func(b *testing.B) {
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			// Same reason as the encode benchmark: the decoder allocates its
			// window on first use, and charging that to the first row makes the
			// row ordering depend on which row ran first rather than on size.
			if _, err := decodeBlock(codec, blockCodecVersion, int64(rawBytes), stored); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				decoded, err := decodeBlock(codec, blockCodecVersion, int64(rawBytes), stored)
				if err != nil {
					b.Fatal(err)
				}
				if len(decoded) != len(payload) {
					b.Fatalf("decoded %d bytes, want %d", len(decoded), len(payload))
				}
			}
		})
	}
}
