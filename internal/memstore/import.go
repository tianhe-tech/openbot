package memstore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type ImportProgress struct {
	Imported int   `json:"imported"`
	Pending  int64 `json:"pending"`
	Unmapped int64 `json:"unmapped"`
}

func PrepareArchive(ctx context.Context, path, backupPath string) error {
	if backupPath == "" {
		return fmt.Errorf("memstore: backup path is required for initial migration")
	}
	absBackup, err := filepath.Abs(backupPath)
	if err != nil {
		return err
	}
	if _, err := os.Stat(absBackup); err == nil {
		return fmt.Errorf("memstore: backup destination already exists")
	} else if !os.IsNotExist(err) {
		return err
	}
	store, err := openDatabaseMode(path, false, false)
	if err != nil {
		return err
	}
	defer store.Close()
	rows, err := store.db.QueryContext(ctx, `SELECT id,adapter,user_id,ts,request,response,summary,project,work_dir,action,tags,strength,recall_cnt,next_review FROM mem_records LIMIT 0`)
	if err != nil {
		return fmt.Errorf("memstore: incompatible legacy database; upgrade the gateway first: %w", err)
	}
	rows.Close()
	if _, err := store.db.ExecContext(ctx, `VACUUM INTO ?`, absBackup); err != nil {
		return fmt.Errorf("memstore: consistent backup failed (inspect destination before retrying): %w", err)
	}
	if err := os.Chmod(absBackup, 0o600); err != nil {
		return fmt.Errorf("memstore: backup permissions: %w", err)
	}
	backupFile, err := os.OpenFile(absBackup, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	syncErr := backupFile.Sync()
	closeErr := backupFile.Close()
	if syncErr != nil {
		return fmt.Errorf("memstore: backup sync failed: %w", syncErr)
	}
	if closeErr != nil {
		return closeErr
	}
	backup, err := openDatabaseMode(absBackup, true, false)
	if err != nil {
		return fmt.Errorf("memstore: backup verification: %w", err)
	}
	var integrity string
	err = backup.db.QueryRowContext(ctx, `PRAGMA quick_check`).Scan(&integrity)
	backup.Close()
	if err != nil || integrity != "ok" {
		return fmt.Errorf("memstore: backup integrity check failed: %q (%v)", integrity, err)
	}
	return store.MigrateArchive(ctx)
}

func (writer *ArchiveWriter) ImportLegacyBatch(ctx context.Context, limit int) (ImportProgress, error) {
	if limit < 1 || limit > 1000 {
		return ImportProgress{}, fmt.Errorf("memstore: import batch size must be 1-1000")
	}
	rows, err := writer.store.db.QueryContext(ctx, `SELECT r.id,r.adapter,r.user_id,r.ts,r.request,r.response,r.summary,r.project,r.work_dir,r.action,r.tags,r.strength,r.recall_cnt,r.next_review
		FROM mem_records r LEFT JOIN mem_v2_legacy_map m ON m.legacy_id=r.id
		WHERE m.legacy_id IS NULL AND TRIM(r.adapter) != '' AND TRIM(r.user_id) != ''
		ORDER BY r.ts,r.id LIMIT ?`, limit)
	if err != nil {
		return ImportProgress{}, err
	}
	records, size, err := readImportRecords(rows, writer.options.QueueBytes)
	rows.Close()
	if err != nil {
		return ImportProgress{}, err
	}
	var progress ImportProgress
	if len(records) > 0 {
		imported := 0
		err = writer.submit(ctx, size, func(ctx context.Context, tx *sql.Tx) error {
			for _, record := range records {
				var exists int
				if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM mem_v2_legacy_map WHERE legacy_id=?`, record.ID).Scan(&exists); err != nil {
					return err
				}
				if exists != 0 {
					continue
				}
				timestamp := record.Ts.UnixMilli()
				key := archiveID("legacy", record.Adapter, record.UserID, record.ID)
				message := ArchiveMessage{
					Session: ArchiveSession{ID: "legacy-session-" + key, Adapter: record.Adapter, UserID: record.UserID, Project: record.Project, WorkDir: record.WorkDir, CreatedAt: timestamp},
					Turn:    ArchiveTurn{ID: "legacy-turn-" + key, DeliveryKey: "legacy:" + record.ID, StartedAt: timestamp},
					ID:      "legacy-user-" + key, Role: "user", State: "completed", Text: record.Request, CreatedAt: timestamp, Fidelity: "legacy_unknown",
				}
				if err := writeArchiveMessage(ctx, tx, message); err != nil {
					return fmt.Errorf("memstore: import %s request: %w", record.ID, err)
				}
				message.ID = "legacy-assistant-" + key
				message.Sequence = 1
				message.Role = "assistant"
				message.Text = record.Response
				if err := writeArchiveMessage(ctx, tx, message); err != nil {
					return fmt.Errorf("memstore: import %s response: %w", record.ID, err)
				}
				if _, err := tx.ExecContext(ctx, `UPDATE mem_v2_turns SET status='legacy_unknown',dispatch_state='legacy' WHERE id=?`, message.Turn.ID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, `INSERT INTO mem_v2_legacy_map(legacy_id,turn_id,import_version) VALUES(?,?,1)`, record.ID, message.Turn.ID); err != nil {
					return err
				}
				imported++
			}
			return nil
		})
		if err != nil {
			return ImportProgress{}, err
		}
		progress.Imported = imported
	}
	err = writer.store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mem_records r LEFT JOIN mem_v2_legacy_map m ON m.legacy_id=r.id
		WHERE m.legacy_id IS NULL AND TRIM(r.adapter) != '' AND TRIM(r.user_id) != ''`).Scan(&progress.Pending)
	if err != nil {
		return progress, err
	}
	err = writer.store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mem_records r LEFT JOIN mem_v2_legacy_map m ON m.legacy_id=r.id
		WHERE m.legacy_id IS NULL AND (TRIM(r.adapter) = '' OR TRIM(r.user_id) = '')`).Scan(&progress.Unmapped)
	return progress, err
}

func readImportRecords(rows *sql.Rows, budget int) ([]MemRecord, int, error) {
	var records []MemRecord
	size := 0
	for rows.Next() {
		var record MemRecord
		var timestamp, nextReview int64
		if err := rows.Scan(&record.ID, &record.Adapter, &record.UserID, &timestamp, &record.Request, &record.Response,
			&record.Summary, &record.Project, &record.WorkDir, &record.Action, &record.Tags, &record.Strength, &record.RecallCnt, &nextReview); err != nil {
			return nil, 0, err
		}
		recordSize := 1024 + len(record.ID) + len(record.Adapter) + len(record.UserID) + len(record.Request) + len(record.Response) + len(record.Summary) + len(record.Project) + len(record.WorkDir) + len(record.Action) + len(record.Tags)
		if recordSize > budget-size {
			if len(records) == 0 {
				return nil, 0, fmt.Errorf("%w: legacy record %s; explicitly raise the writer byte budget to import it", ErrArchiveTooLarge, record.ID)
			}
			break
		}
		record.Ts = time.Unix(timestamp, 0)
		record.NextReview = time.Unix(nextReview, 0)
		records = append(records, record)
		size += recordSize
	}
	return records, size, rows.Err()
}
