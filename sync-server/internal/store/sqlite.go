package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"qwerty-learner/sync-server/internal/protocol"

	_ "modernc.org/sqlite"
)

// Store wraps SQLite persistence for shared authoritative data and idempotent event logs.
type Store struct {
	db *sql.DB
}

// Open initializes the SQLite database in WAL mode and ensures required tables exist.
func Open(dbPath string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("create sqlite dir: %w", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// SQLite works best with a single writer connection in WAL mode to avoid SQLITE_BUSY.
	db.SetMaxOpenConns(1)

	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA busy_timeout=5000;",
		"PRAGMA foreign_keys=ON;",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("exec %q: %w", p, err)
		}
	}

	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate() error {
	const schema = `
CREATE TABLE IF NOT EXISTS sync_events (
	server_seq       INTEGER PRIMARY KEY AUTOINCREMENT,
	event_id         TEXT NOT NULL UNIQUE,
	user_id          TEXT NOT NULL,
	source_device_id TEXT NOT NULL,
	entity           TEXT NOT NULL,
	op               TEXT NOT NULL,
	sync_uid         TEXT NOT NULL,
	payload          TEXT NOT NULL,
	client_ts        INTEGER NOT NULL,
	created_at       INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sync_events_user_seq
	ON sync_events (user_id, server_seq);

CREATE TABLE IF NOT EXISTS shared_records (
	user_id    TEXT NOT NULL,
	entity     TEXT NOT NULL,
	sync_uid   TEXT NOT NULL,
	payload    TEXT NOT NULL,
	updated_at INTEGER NOT NULL,
	deleted    INTEGER NOT NULL DEFAULT 0,
	server_seq INTEGER NOT NULL,
	PRIMARY KEY (user_id, entity, sync_uid)
);

CREATE INDEX IF NOT EXISTS idx_shared_records_user_entity
	ON shared_records (user_id, entity, deleted);
`
	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("migrate sqlite schema: %w", err)
	}
	return nil
}

// ApplyResult indicates whether an event was newly applied or deduplicated.
type ApplyResult struct {
	Duplicate bool
	Event     protocol.SyncEvent
}

// ApplyEvent persists an upstream event idempotently, updates shared_records with
// entity-specific merge rules, and assigns a monotonic serverSeq.
func (s *Store) ApplyEvent(ctx context.Context, ev protocol.SyncEvent) (ApplyResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ApplyResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	// 1. Idempotency check by event_id
	var existingSeq int64
	err = tx.QueryRowContext(ctx, `SELECT server_seq FROM sync_events WHERE event_id = ?`, ev.EventID).Scan(&existingSeq)
	if err == nil {
		ev.ServerSeq = existingSeq
		return ApplyResult{Duplicate: true, Event: ev}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ApplyResult{}, fmt.Errorf("check duplicate event: %w", err)
	}

	// 2. Insert immutable event log and allocate global server_seq
	nowMs := time.Now().UnixMilli()
	payloadStr := string(ev.Payload)
	if payloadStr == "" {
		payloadStr = "{}"
	}
	res, err := tx.ExecContext(ctx, `
		INSERT INTO sync_events (event_id, user_id, source_device_id, entity, op, sync_uid, payload, client_ts, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, ev.EventID, ev.UserID, ev.SourceDeviceID, ev.Entity, ev.Op, ev.SyncUID, payloadStr, ev.ClientTs, nowMs)
	if err != nil {
		return ApplyResult{}, fmt.Errorf("insert sync_event: %w", err)
	}
	seq, err := res.LastInsertId()
	if err != nil {
		return ApplyResult{}, fmt.Errorf("read last insert id: %w", err)
	}
	ev.ServerSeq = seq

	// 3. Apply event to authoritative shared_records
	if err := applyToSharedRecords(ctx, tx, &ev); err != nil {
		return ApplyResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return ApplyResult{}, fmt.Errorf("commit tx: %w", err)
	}
	return ApplyResult{Duplicate: false, Event: ev}, nil
}

func applyToSharedRecords(ctx context.Context, tx *sql.Tx, ev *protocol.SyncEvent) error {
	switch ev.Op {
	case protocol.OpDelete:
		_, err := tx.ExecContext(ctx, `
			INSERT INTO shared_records (user_id, entity, sync_uid, payload, updated_at, deleted, server_seq)
			VALUES (?, ?, ?, ?, ?, 1, ?)
			ON CONFLICT(user_id, entity, sync_uid) DO UPDATE SET
				deleted = 1,
				updated_at = MAX(shared_records.updated_at, excluded.updated_at),
				server_seq = excluded.server_seq
		`, ev.UserID, ev.Entity, ev.SyncUID, string(ev.Payload), ev.ClientTs, ev.ServerSeq)
		return err

	case protocol.OpDeleteByWord:
		// Matches useDeleteWordRecord(word, dict): mark all matching wordRecords as deleted.
		var delReq struct {
			Word string `json:"word"`
			Dict string `json:"dict"`
		}
		if err := json.Unmarshal(ev.Payload, &delReq); err != nil {
			return fmt.Errorf("parse DELETE_BY_WORD payload: %w", err)
		}
		_, err := tx.ExecContext(ctx, `
			UPDATE shared_records
			SET deleted = 1, updated_at = MAX(updated_at, ?), server_seq = ?
			WHERE user_id = ? AND entity = ?
			  AND json_extract(payload, '$.word') = ?
			  AND json_extract(payload, '$.dict') = ?
			  AND updated_at <= ?
		`, ev.ClientTs, ev.ServerSeq, ev.UserID, protocol.EntityWordRecords, delReq.Word, delReq.Dict, ev.ClientTs)
		return err

	case protocol.OpCreate, protocol.OpUpdate:
		return upsertRecord(ctx, tx, ev)
	default:
		return fmt.Errorf("unhandled op: %s", ev.Op)
	}
}

func upsertRecord(ctx context.Context, tx *sql.Tx, ev *protocol.SyncEvent) error {
	var existingPayload string
	var existingUpdatedAt int64
	err := tx.QueryRowContext(ctx, `
		SELECT payload, updated_at FROM shared_records
		WHERE user_id = ? AND entity = ? AND sync_uid = ?
	`, ev.UserID, ev.Entity, ev.SyncUID).Scan(&existingPayload, &existingUpdatedAt)

	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO shared_records (user_id, entity, sync_uid, payload, updated_at, deleted, server_seq)
			VALUES (?, ?, ?, ?, ?, 0, ?)
		`, ev.UserID, ev.Entity, ev.SyncUID, string(ev.Payload), ev.ClientTs, ev.ServerSeq)
		return err
	}
	if err != nil {
		return err
	}

	mergedPayload := string(ev.Payload)
	mergedUpdatedAt := ev.ClientTs

	switch ev.Entity {
	case protocol.EntityReviewRecords:
		// Monotonic merge for review progress: keep max(index) and logical OR of isFinished.
		merged, mergeErr := mergeReviewPayload(existingPayload, string(ev.Payload))
		if mergeErr == nil {
			mergedPayload = merged
			ev.Payload = json.RawMessage(merged)
		}
		if existingUpdatedAt > mergedUpdatedAt {
			mergedUpdatedAt = existingUpdatedAt
		}
	case protocol.EntityConfig:
		// LWW per config key: ignore older updates.
		if ev.ClientTs < existingUpdatedAt {
			return nil
		}
	default:
		// Append-only entities (wordRecords / chapterRecords): keep latest payload if updated.
		if ev.ClientTs < existingUpdatedAt {
			return nil
		}
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE shared_records
		SET payload = ?, updated_at = ?, deleted = 0, server_seq = ?
		WHERE user_id = ? AND entity = ? AND sync_uid = ?
	`, mergedPayload, mergedUpdatedAt, ev.ServerSeq, ev.UserID, ev.Entity, ev.SyncUID)
	return err
}

func mergeReviewPayload(oldJSON, newJSON string) (string, error) {
	var oldMap, newMap map[string]any
	if err := json.Unmarshal([]byte(oldJSON), &oldMap); err != nil {
		return "", err
	}
	if err := json.Unmarshal([]byte(newJSON), &newMap); err != nil {
		return "", err
	}

	oldIdx, _ := oldMap["index"].(float64)
	newIdx, _ := newMap["index"].(float64)
	if oldIdx > newIdx {
		newMap["index"] = oldIdx
	}

	oldDone, _ := oldMap["isFinished"].(bool)
	newDone, _ := newMap["isFinished"].(bool)
	newMap["isFinished"] = oldDone || newDone

	out, err := json.Marshal(newMap)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// GetSnapshot returns active authoritative records for a user (optionally filtered by afterSeq)
// along with the user's latest server_seq.
func (s *Store) GetSnapshot(ctx context.Context, userID string, afterSeq int64) (protocol.SnapshotResponse, error) {
	resp := protocol.SnapshotResponse{
		UserID:  userID,
		Records: make([]protocol.SnapshotRecord, 0),
	}

	var latestSeq sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MAX(server_seq) FROM sync_events WHERE user_id = ?`, userID).Scan(&latestSeq); err != nil {
		return resp, err
	}
	if latestSeq.Valid {
		resp.LatestServerSeq = latestSeq.Int64
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT entity, sync_uid, payload, updated_at, server_seq
		FROM shared_records
		WHERE user_id = ? AND deleted = 0 AND server_seq > ?
		ORDER BY server_seq ASC
	`, userID, afterSeq)
	if err != nil {
		return resp, err
	}
	defer rows.Close()

	for rows.Next() {
		var rec protocol.SnapshotRecord
		var payloadStr string
		if err := rows.Scan(&rec.Entity, &rec.SyncUID, &payloadStr, &rec.UpdatedAt, &rec.ServerSeq); err != nil {
			return resp, err
		}
		rec.Payload = json.RawMessage(payloadStr)
		resp.Records = append(resp.Records, rec)
	}
	return resp, rows.Err()
}
