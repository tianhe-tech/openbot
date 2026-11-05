package memstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
)

// artifactIdentity is the scoped content identity of a retained tool output.
//
// The principal is part of the identity rather than a filter applied afterwards.
// A global hash index would let one principal discover that another principal
// has stored a particular payload by observing that its own write was deduped,
// which is a hash-existence side channel. Scoping the identity means a
// cross-principal collision is not a collision at all: the two rows are simply
// different identities that happen to share a digest.
//
// rawBytes is part of the identity because a block records the untruncated
// length of the output it came from. Two calls whose retained payloads are
// identical but whose originals were not would otherwise share one block, and
// the second call would inherit the first call's raw_bytes — a number that
// describes a different output.
type artifactIdentity struct {
	principal string
	checksum  string
	rawBytes  int
}

// artifactBlockID derives the block ID for a scoped content identity.
//
// The ID is a function of the identity, so two writes of the same payload by
// the same principal compute the same ID and the primary key enforces the
// sharing. Deriving it rather than looking it up means the common case — a
// first write — costs no extra query.
func artifactBlockID(identity artifactIdentity) string {
	return archiveID("artifact-block", identity.principal, identity.checksum, fmt.Sprint(identity.rawBytes))
}

// storeArtifactBlock writes a retained tool output, sharing an existing block
// when this principal has already stored the same bytes.
//
// The returned block ID is the one the caller should reference. Sharing is
// verified rather than assumed: a digest match is followed by a byte
// comparison, because a hash collision would otherwise silently substitute one
// tool's output for another's, and that is a data-corruption bug that would
// surface as a wrong answer rather than as an error.
//
// rawBytes is the untruncated length of the original output, which may be
// larger than the retained payload. It is recorded on the block so a reader can
// tell how much was dropped.
func storeArtifactBlock(ctx context.Context, tx *sql.Tx, principal string, payload []byte, rawBytes int) (string, error) {
	checksum := sha256.Sum256(payload)
	identity := artifactIdentity{
		principal: principal,
		checksum:  hex.EncodeToString(checksum[:]),
		rawBytes:  rawBytes,
	}
	blockID := artifactBlockID(identity)

	// The conflict clause makes a repeat write a no-op, so an existing payload
	// is never rewritten. It is applied at every size: a small output repeated
	// verbatim should still occupy one row rather than accumulate duplicates,
	// and the derived ID makes that free.
	if _, err := tx.ExecContext(ctx, `INSERT INTO mem_v2_blocks
		(id,principal_id,codec,codec_version,raw_bytes,stored_bytes,checksum,payload)
		VALUES(?,?,?,?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET id=excluded.id WHERE checksum=excluded.checksum`,
		blockID, principal, blockCodecRaw, blockCodecVersion, rawBytes, len(payload),
		identity.checksum, payload); err != nil {
		return "", fmt.Errorf("memstore: store tool output block: %w", err)
	}

	// Verify the bytes now stored under this identity. This runs on every
	// write, not only on a suspected conflict, because the case that matters is
	// the one where the digest matched and the content did not — and that case
	// is indistinguishable from a first write without reading the row back.
	var stored []byte
	if err := tx.QueryRowContext(ctx,
		`SELECT payload FROM mem_v2_blocks WHERE id=? AND principal_id=?`,
		blockID, principal).Scan(&stored); err != nil {
		return "", fmt.Errorf("memstore: verify tool output block: %w", err)
	}
	if !bytesEqual(stored, payload) {
		return "", fmt.Errorf("memstore: tool output block %s holds different bytes than its checksum claims", blockID)
	}
	return blockID, nil
}

// bytesEqual compares two byte slices.
//
// It exists so the comparison is a named decision rather than an inline
// bytes.Equal that a later reader might mistake for a redundant check. The
// comparison is the defence against a hash collision substituting one tool's
// output for another's.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
