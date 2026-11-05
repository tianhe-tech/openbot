package memstore

import (
	"context"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// Block codecs. A block records the name of the codec that produced its
// payload, so a reader never has to infer it from the bytes. The name is stored
// per row rather than per archive because the decision is made per payload: a
// small message stays raw while a large one beside it is compressed.
const (
	blockCodecRaw  = "raw"
	blockCodecZstd = "zstd"

	// blockCodecVersion is the version of the codec contract, not of the
	// library that implements it. It changes only if the meaning of a codec
	// name changes, which would make previously written payloads undecodable by
	// new code. Upgrading the zstd library does not change it, because the
	// frame format is stable and the decoder reads every frame the encoder has
	// ever produced.
	blockCodecVersion = 1
)

// blockCompressMinBytes is the payload size below which a block is stored raw.
//
// Compression is not free at small sizes. A zstd frame carries a header and a
// checksum, so a few hundred bytes of text can grow, and the encoder has to
// build its match tables before it emits anything. Below this threshold the
// saving is not worth the CPU on the capture path, which is the one path that
// must not fail.
const blockCompressMinBytes = 4 * 1024

// blockCompressMinSaving is the divisor that sets how much a payload must
// shrink before the compressed form is kept. A payload is stored compressed
// only if it saves at least one part in this many, so incompressible data —
// base64 blobs, already-compressed artifacts, dense JSON of random identifiers
// — is stored raw instead of paying decode cost for a few bytes.
const blockCompressMinSaving = 8

// maxBlockDecodeBytes bounds how large a decoded block may be.
//
// This is enforced in both directions on purpose. A payload larger than this is
// stored raw, so a compressed block can never legitimately decode to more than
// this, which means a decode that would exceed it is evidence of corruption
// rather than a large message. Enforcing it only on the read side would turn a
// legitimate large message into an unreadable one; enforcing it only on the
// write side would leave the decoder open to a frame that claims an enormous
// output.
const maxBlockDecodeBytes = 64 * 1024 * 1024

// blockCompressLevel is the zstd level used for archival payloads.
//
// The default level is chosen rather than a higher one because compression runs
// on the capture path, where the cost is paid by a user waiting for a message
// to be acknowledged. Archival text is read far less often than it is written,
// but the write is the operation that must not fail, so the level is set where
// the ratio is already most of the way to its ceiling and the CPU is not.
const blockCompressLevel = zstd.SpeedDefault

// blockEncoder and blockDecoder are shared across goroutines. EncodeAll and
// DecodeAll are documented as safe for concurrent use, and sharing them avoids
// rebuilding the match tables and the decoder's window for every block.
var (
	blockEncoder = mustBlockEncoder()
	blockDecoder = mustBlockDecoder()
)

func mustBlockEncoder() *zstd.Encoder {
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(blockCompressLevel),
		// A frame checksum costs four bytes and lets the decoder reject a
		// corrupted payload instead of returning plausible garbage. That trade
		// is worth making for data that is meant to be read back years later.
		zstd.WithEncoderCRC(true),
	)
	if err != nil {
		panic("memstore: build zstd encoder: " + err.Error())
	}
	return encoder
}

func mustBlockDecoder() *zstd.Decoder {
	decoder, err := zstd.NewReader(nil,
		// Bound what a single frame may make the decoder allocate. Without this
		// a corrupt or hostile frame header could ask for an allocation far
		// larger than any block this archive writes.
		zstd.WithDecoderMaxMemory(maxBlockDecodeBytes),
		// Blocks are decoded whole and never streamed, so there is no reason to
		// keep a window alive between calls.
		zstd.WithDecoderConcurrency(1),
	)
	if err != nil {
		panic("memstore: build zstd decoder: " + err.Error())
	}
	return decoder
}

// compressBlock decides how one payload should be stored.
//
// It returns the codec name, the bytes to write, and the uncompressed length.
// The decision is made here rather than at the call site so every block in the
// archive follows the same rule, and so the rule can be tested directly instead
// of through a database.
//
// The returned payload aliases text when the payload is stored raw. Callers
// must not mutate it.
func compressBlock(text []byte) (codec string, payload []byte, rawBytes int, err error) {
	rawBytes = len(text)
	if len(text) < blockCompressMinBytes || len(text) > maxBlockDecodeBytes {
		return blockCodecRaw, text, rawBytes, nil
	}
	compressed := blockEncoder.EncodeAll(text, nil)
	// Keep the compressed form only if it saves enough to be worth the decode.
	// The comparison is against the original length, not against the frame
	// overhead, because the question is whether the archive gets smaller.
	if len(compressed) > len(text)-len(text)/blockCompressMinSaving {
		return blockCodecRaw, text, rawBytes, nil
	}
	return blockCodecZstd, compressed, rawBytes, nil
}

// decodeBlock returns the original payload of a block.
//
// Every read path goes through this function, so the codec contract is enforced
// in one place. It rejects an unknown codec rather than returning the stored
// bytes, because returning compressed bytes as if they were text would surface
// as garbled conversation rather than as an error, and garbled text is much
// harder to notice than a failure.
func decodeBlock(codec string, codecVersion int, rawBytes int64, payload []byte) ([]byte, error) {
	switch codec {
	case blockCodecRaw:
		// A raw block's payload is the text. raw_bytes is not consulted: for a
		// truncated tool output it records the untruncated length, which is
		// deliberately larger than what was kept.
		return payload, nil
	case blockCodecZstd:
		if codecVersion != blockCodecVersion {
			return nil, fmt.Errorf("memstore: block codec version %d is not supported", codecVersion)
		}
		if rawBytes < 0 || rawBytes > maxBlockDecodeBytes {
			return nil, fmt.Errorf("memstore: block declares %d decoded bytes, outside the 0..%d range",
				rawBytes, maxBlockDecodeBytes)
		}
		decoded, err := blockDecoder.DecodeAll(payload, nil)
		if err != nil {
			return nil, fmt.Errorf("memstore: decode block: %w", err)
		}
		// The declared length is checked against the actual one. A frame that
		// decodes cleanly but to a different size than the row promised means
		// the row and the payload disagree, and the row is what every offset in
		// the archive is computed from.
		if int64(len(decoded)) != rawBytes {
			return nil, fmt.Errorf("memstore: block decoded to %d bytes, row declares %d", len(decoded), rawBytes)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("memstore: unknown block codec %q", codec)
	}
}

// BlockCodecStats reports how much of one principal's archive is stored
// compressed, and how many bytes compression removed.
type BlockCodecStats struct {
	Blocks        int64 `json:"blocks"`
	RawBlocks     int64 `json:"raw_blocks"`
	ZstdBlocks    int64 `json:"zstd_blocks"`
	RawBytes      int64 `json:"raw_bytes"`
	StoredBytes   int64 `json:"stored_bytes"`
	CompressedRaw int64 `json:"compressed_raw_bytes"`
}

// SavedBytes is the number of bytes compression removed from the archive.
func (stats BlockCodecStats) SavedBytes() int64 {
	return stats.RawBytes - stats.StoredBytes
}

// BlockCodecStats reads the codec distribution for one scope.
//
// The sums are over mem_v2_blocks, which holds message bodies, partial
// snapshots, and retained tool output. Partial snapshots and tool output are
// written raw, so the raw counts include them; the compressed counts are
// message bodies only.
//
// This is a full scan of mem_v2_blocks, so it belongs in a diagnostic that is
// asked for explicitly, not in a path that runs on every request.
func (s *Store) BlockCodecStats(ctx context.Context, scope SearchScope) (BlockCodecStats, error) {
	principal, err := scope.principal()
	if err != nil {
		return BlockCodecStats{}, err
	}
	var stats BlockCodecStats
	err = s.db.QueryRowContext(ctx, `SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN codec=? THEN 1 ELSE 0 END),0),
			COALESCE(SUM(CASE WHEN codec=? THEN 1 ELSE 0 END),0),
			COALESCE(SUM(raw_bytes),0),
			COALESCE(SUM(stored_bytes),0),
			COALESCE(SUM(CASE WHEN codec=? THEN raw_bytes ELSE 0 END),0)
		FROM mem_v2_blocks WHERE principal_id=?`,
		blockCodecRaw, blockCodecZstd, blockCodecZstd, principal).Scan(
		&stats.Blocks, &stats.RawBlocks, &stats.ZstdBlocks,
		&stats.RawBytes, &stats.StoredBytes, &stats.CompressedRaw)
	if err != nil {
		return stats, fmt.Errorf("memstore: block codec stats: %w", err)
	}
	return stats, nil
}
