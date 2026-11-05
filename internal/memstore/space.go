package memstore

import (
	"context"
	"fmt"
	"os"
)

// SpaceReport describes where the archive's bytes go.
//
// Two kinds of number appear here and they must never be added together:
//
//   - FileBytes, WALBytes, SHMBytes, FreeBytes, and FTSBytes describe the
//     database file on disk. They are whole-file measurements and cannot be
//     attributed to one principal, because every principal shares the same
//     pages.
//   - BlockBytes, ArtifactBytes, and EmbeddingBytes are logical sizes of rows
//     belonging to one principal. They are subsets of the file, not additions
//     to it.
//
// A report that summed the two would claim the archive is larger than the disk
// it sits on, so the struct keeps them in separate groups and the formatter
// labels them. This is also why the file-level numbers are not scoped: a
// per-principal share of a shared page is a fiction, and inventing one would
// make the report look precise while being wrong.
type SpaceReport struct {
	// File-level, whole database.
	FileBytes int64 `json:"file_bytes"`
	WALBytes  int64 `json:"wal_bytes"`
	SHMBytes  int64 `json:"shm_bytes"`
	// FreeBytes is space already allocated to the file but not in use. It is
	// what a VACUUM would return to the filesystem, so it is reported rather
	// than folded into FileBytes.
	FreeBytes int64 `json:"free_bytes"`
	// FTSBytes is the page cost of the full-text indexes, measured with
	// dbstat. It is a subset of FileBytes.
	FTSBytes int64 `json:"fts_bytes"`
	// TableBreakdownAvailable is false when the SQLite build has no dbstat
	// virtual table. FTSBytes is then zero because it could not be measured,
	// not because the index is empty, and a caller must not read zero as
	// "no index".
	TableBreakdownAvailable bool `json:"table_breakdown_available"`

	// Principal-scoped logical sizes.
	BlockBytes    int64 `json:"block_bytes"`
	ArtifactBytes int64 `json:"artifact_bytes"`
	// EmbeddingBytes is always zero today: no embeddings are stored. It is
	// reported so the field exists when they are, rather than being silently
	// absent from a report that claims to account for everything.
	EmbeddingBytes int64 `json:"embedding_bytes"`

	// BackupBytes is the size of a backup file. It is reported separately
	// because a backup is not part of the live archive's footprint, and
	// counting it would make the archive appear to grow every time it is
	// backed up.
	BackupBytes int64 `json:"backup_bytes"`
}

// SpaceReport measures the archive's footprint.
//
// backupPath may be empty, in which case BackupBytes is zero. The path is
// measured with os.Stat rather than by opening it, so a backup that has been
// moved or deleted reports zero instead of failing the whole report.
//
// The file-level numbers come from the filesystem and from PRAGMAs, which are
// cheap. FTSBytes comes from dbstat, which walks the page map and therefore
// costs time proportional to the database size; this is a diagnostic call, not
// a per-request one.
func (s *Store) SpaceReport(ctx context.Context, scope SearchScope, backupPath string) (SpaceReport, error) {
	principal, err := scope.principal()
	if err != nil {
		return SpaceReport{}, err
	}
	var report SpaceReport

	path, err := s.databasePath(ctx)
	if err != nil {
		return report, err
	}
	report.FileBytes = fileBytes(path)
	report.WALBytes = fileBytes(path + "-wal")
	report.SHMBytes = fileBytes(path + "-shm")
	if backupPath != "" {
		report.BackupBytes = fileBytes(backupPath)
	}

	var pageCount, pageSize, freePages int64
	if err := s.db.QueryRowContext(ctx, `SELECT
			(SELECT page_count FROM pragma_page_count),
			(SELECT page_size FROM pragma_page_size),
			(SELECT freelist_count FROM pragma_freelist_count)`).Scan(
		&pageCount, &pageSize, &freePages); err != nil {
		return report, fmt.Errorf("memstore: space report page counts: %w", err)
	}
	report.FreeBytes = freePages * pageSize

	// dbstat is a compile-time option. When it is missing the report still
	// answers the questions that matter (how big is the file, how much is
	// free), and says so rather than reporting a zero that reads as "empty".
	if ftsBytes, err := s.ftsBytes(ctx); err != nil {
		report.TableBreakdownAvailable = false
	} else {
		report.TableBreakdownAvailable = true
		report.FTSBytes = ftsBytes
	}

	if err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(stored_bytes),0) FROM mem_v2_blocks WHERE principal_id=?`,
		principal).Scan(&report.BlockBytes); err != nil {
		return report, fmt.Errorf("memstore: space report block bytes: %w", err)
	}
	// Artifacts are blocks referenced by a tool call. The IN subquery
	// deduplicates, because two tool calls can reference the same retained
	// output and counting it twice would overstate the archive.
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(stored_bytes),0)
			FROM mem_v2_blocks
			WHERE principal_id=?
			  AND id IN (SELECT artifact_ref FROM mem_v2_tool_calls
			             WHERE principal_id=? AND artifact_ref != '')`,
		principal, principal).Scan(&report.ArtifactBytes); err != nil {
		return report, fmt.Errorf("memstore: space report artifact bytes: %w", err)
	}
	return report, nil
}

// databasePath returns the file backing the main database.
//
// It asks SQLite rather than storing the path on Store, so a handle opened
// through any route reports the file it is actually reading. That matters for
// read-only handles and for tests, which open the same file by different paths.
func (s *Store) databasePath(ctx context.Context) (string, error) {
	rows, err := s.db.QueryContext(ctx, `PRAGMA database_list`)
	if err != nil {
		return "", fmt.Errorf("memstore: database list: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var seq int
		var name, file string
		if err := rows.Scan(&seq, &name, &file); err != nil {
			return "", fmt.Errorf("memstore: database list: %w", err)
		}
		if name == "main" {
			if file == "" {
				return "", fmt.Errorf("memstore: main database is in memory")
			}
			return file, nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("memstore: database list: %w", err)
	}
	return "", fmt.Errorf("memstore: main database not found")
}

// ftsBytes sums the page cost of every full-text index.
//
// Both indexes are matched by prefix because FTS5 stores its data in several
// shadow tables (data, idx, docsize, config, content) and the index is the sum
// of all of them. Counting only the table named in the CREATE statement would
// report a fraction of the real cost.
func (s *Store) ftsBytes(ctx context.Context) (int64, error) {
	var total int64
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(pgsize),0) FROM dbstat
		WHERE name LIKE 'mem_v2_chunk_fts%' OR name LIKE 'mem_v2_chunk_word_fts%'`).Scan(&total)
	if err != nil {
		return 0, err
	}
	return total, nil
}

// fileBytes returns the size of a file, or zero if it does not exist.
//
// A missing WAL or SHM file is normal: SQLite removes them on a clean close.
// Treating that as an error would make the report fail on a healthy archive.
func fileBytes(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}
