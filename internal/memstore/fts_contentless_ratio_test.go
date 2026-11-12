package memstore

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"
)

// This file used to measure the contentless FTS5 saving on a realistic archive,
// and it is kept because the measurement it produced is the reason schema v7
// exists. It now asserts the outcome instead of the hypothesis.
//
// The probe in fts_contentless_probe_test.go established that the saving is a
// constant — one copy of the chunk text — and that the percentage it represents
// depends entirely on how large the postings are. That left the decision resting
// on a number the probe could not supply: what fraction of a real archive's index
// is the duplicated text. The probe's own corpus is a bag of words with no
// structure, so its ratio is a property of the generator, not of conversation.
//
// The measurement was taken on a conversation-shaped corpus written through the
// real write path and indexed by the real index worker. It found the archive
// stored 2.24-2.26 copies of the chunk text and that the duplicated copy was
// 35.9-37.4% of the archive file. That is what justified the migration, and the
// read-path cost was measured separately at 1.13x at 2k messages and
// indistinguishable at 20k.
//
// With v7 in place the duplicated copy is gone, so the same measurement now
// returns zero. This test asserts that zero, because a regression that
// reintroduced a content column would otherwise be invisible: search would keep
// working, the archive would just quietly grow by a third.

// TestContentlessSavingOnARealisticArchive asserts that a conversation-shaped
// archive built through the real write path stores no text in its indexes.
//
// It measures two corpora, because the old measurement needed both to bracket
// the answer and the regression check is stronger for covering both shapes. The
// trigram index's size is driven by how much text the corpus shares, and the two
// generators sit at opposite ends of that:
//
//   - random words share almost no trigrams, so the trigram index is as large as
//     it can be and the stored text looked like a small fraction of it.
//   - prose drawn from a Zipf distribution over a realistic vocabulary repeats
//     trigrams constantly, so the trigram index is small and the stored text
//     dominated it.
//
// Both must now report zero stored text. A content column reintroduced for one
// corpus shape and not the other would be caught here.
func TestContentlessSavingOnARealisticArchive(t *testing.T) {
	for _, corpus := range []struct {
		name     string
		messages []conversationMessage
	}{
		{"random-words", conversationCorpusForBench(2*1024*1024, 7)},
		{"prose", proseCorpusForTest(2*1024*1024, 11)},
	} {
		t.Run(corpus.name, func(t *testing.T) {
			assertContentlessArchive(t, corpus.messages)
		})
	}
}

// assertContentlessArchive writes a corpus through the real write path, indexes
// it with the real index worker, and asserts the indexes hold no text.
func assertContentlessArchive(t *testing.T, corpus []conversationMessage) {
	t.Helper()
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()

	var payloadBytes int
	for index, message := range corpus {
		archive := archiveMessageForTest("ratio-" + itoa(index))
		archive.Role = message.role
		archive.Text = message.text
		// Sequence is unique per session, so it has to advance with the corpus.
		archive.Sequence = int64(index)
		// Each message gets its own turn so the archive looks like a
		// conversation rather than one enormous turn.
		archive.Turn = ArchiveTurn{
			ID:          "turn-ratio-" + itoa(index),
			DeliveryKey: "delivery-ratio-" + itoa(index),
			StartedAt:   1700000000000 + int64(index),
		}
		if err := writer.RecordMessage(ctx, archive); err != nil {
			t.Fatal(err)
		}
		payloadBytes += len(message.text)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for {
		count, err := writer.IndexPending(ctx, maxIndexBatch)
		if err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			break
		}
	}

	report, err := writer.store.SpaceReport(ctx, SearchScope{Adapter: "test", UserID: "owner"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if !report.TableBreakdownAvailable {
		t.Skip("this SQLite build has no dbstat; the index size cannot be measured")
	}

	// The chunk text is measured directly so the assertion has a denominator:
	// the indexes must be non-empty, and the text they hold must be zero. Without
	// the chunk count a zero could mean "contentless" or "nothing was indexed".
	var chunkBytes int64
	var chunkCount int64
	if err := writer.store.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(end_byte - start_byte),0), COUNT(*) FROM mem_v2_chunks`).
		Scan(&chunkBytes, &chunkCount); err != nil {
		t.Fatal(err)
	}

	t.Logf("messages:             %d", len(corpus))
	t.Logf("payload text:         %d bytes", payloadBytes)
	t.Logf("archive file:         %d bytes", report.FileBytes)
	t.Logf("full-text index:      %d bytes (%.2fx payload)", report.FTSBytes,
		float64(report.FTSBytes)/float64(payloadBytes))
	t.Logf("chunks indexed:       %d (%d bytes of chunk text)", chunkCount, chunkBytes)
	t.Logf("stored text in index: %d bytes", report.FTSContentBytes)

	if chunkCount == 0 || chunkBytes == 0 {
		t.Fatal("no chunks were indexed; the assertion below would pass vacuously")
	}
	if report.FTSBytes <= 0 {
		t.Fatalf("full-text index measured %d bytes; the shadow tables are not attributed",
			report.FTSBytes)
	}
	// This is the migration's measurement. Before v7 it was 2.24-2.26 copies of
	// the chunk text; a non-zero value now means a content column came back.
	if report.FTSContentBytes != 0 {
		t.Fatalf("stored text in index = %d bytes; schema v7 made both indexes contentless, "+
			"so this must be zero (chunk text is %d bytes)", report.FTSContentBytes, chunkBytes)
	}
}

// proseCorpusForTest builds a conversation-shaped corpus out of prose rather
// than random words.
//
// The difference that matters is trigram redundancy. A random six-character word
// contributes six trigrams that appear nowhere else, so the trigram index grows
// in proportion to the text. Prose reuses "the", "and", "配置", "文件" constantly,
// so the same text produces far fewer distinct trigrams and a much smaller
// index. Since the contentless saving is a fixed one copy of the text, a smaller
// index means the saving is a larger share of it.
//
// The vocabulary is drawn with a Zipf-ish skew because that is what natural
// language does: a few words carry most of the text. A uniform draw over the
// same vocabulary would be a different corpus with a different index size.
func proseCorpusForTest(totalBytes int, seed int) []conversationMessage {
	words := proseVocabularyForTest()
	var messages []conversationMessage
	index := 0
	for size := 0; size < totalBytes; index++ {
		var target int
		var role string
		switch index % 10 {
		case 0, 1, 2, 3:
			target, role = 120+index%80, "user"
		case 4, 5, 6:
			target, role = 600+index%900, "assistant"
		case 7, 8:
			target, role = 3000+index%2000, "assistant"
		default:
			target, role = 20*1024+index%4096, "tool"
		}
		text := proseTextForTest(words, target, seed+index)
		messages = append(messages, conversationMessage{role, text})
		size += len(text)
	}
	return messages
}

// proseVocabularyForTest returns a vocabulary of ordinary technical words,
// including CJK terms, so the corpus exercises both tokenizers the way a real
// bilingual conversation does.
func proseVocabularyForTest() []string {
	return []string{
		"the", "gateway", "forwards", "requests", "to", "the", "upstream", "session",
		"and", "returns", "a", "response", "when", "the", "turn", "completes",
		"memory", "recall", "search", "index", "chunk", "archive", "block", "codec",
		"configuration", "timeout", "retry", "queue", "worker", "lease", "schema",
		"migration", "backup", "restore", "delete", "scope", "principal", "adapter",
		"message", "turn", "attempt", "partial", "tool", "call", "attachment",
		"is", "was", "will", "be", "has", "been", "not", "but", "so", "because",
		"this", "that", "these", "those", "it", "they", "we", "you", "I",
		"配置文件", "并发数", "超时时间", "会话管理", "索引状态", "检索结果",
		"归档数据", "重试次数", "内存", "搜索", "压缩", "迁移", "备份", "删除",
	}
}

// proseTextForTest builds roughly size bytes of prose from the vocabulary.
//
// The draw is skewed so common words repeat far more often than rare ones, which
// is the property that makes a trigram index small. A uniform draw would produce
// a corpus that looks like prose but indexes like random words.
func proseTextForTest(words []string, size int, seed int) string {
	var builder strings.Builder
	builder.Grow(size + 64)
	state := uint32(seed)*2654435761 + 1013904223
	for builder.Len() < size {
		state = state*1664525 + 1013904223
		// Square the uniform draw to skew it toward the front of the vocabulary,
		// where the common words are.
		u := float64(state%1000000) / 1000000
		index := int(u * u * float64(len(words)))
		if index >= len(words) {
			index = len(words) - 1
		}
		builder.WriteString(words[index])
		if state%13 == 0 {
			builder.WriteString(".\n")
		} else {
			builder.WriteByte(' ')
		}
	}
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
