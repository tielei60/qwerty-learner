package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	EntityWordRecords    = "wordRecords"
	EntityChapterRecords = "chapterRecords"
	EntityReviewRecords  = "reviewRecords"
	EntityConfig         = "config"

	OpCreate       = "CREATE"
	OpUpdate       = "UPDATE"
	OpDelete       = "DELETE"
	OpDeleteByWord = "DELETE_BY_WORD"
)

// SyncEvent is the unified envelope exchanged over sync.up.{userId} and sync.down.{userId}.
type SyncEvent struct {
	EventID        string          `json:"eventId"`
	UserID         string          `json:"userId"`
	SourceDeviceID string          `json:"sourceDeviceId"`
	Entity         string          `json:"entity"`
	Op             string          `json:"op"`
	SyncUID        string          `json:"syncUid"`
	Payload        json.RawMessage `json:"payload"`
	ClientTs       int64           `json:"clientTs"`
	ServerSeq      int64           `json:"serverSeq,omitempty"`
}

// Validate checks required fields and allowed entity/op values.
func (e *SyncEvent) Validate() error {
	if e.EventID == "" {
		return errors.New("eventId is required")
	}
	if e.UserID == "" {
		return errors.New("userId is required")
	}
	if e.SourceDeviceID == "" {
		return errors.New("sourceDeviceId is required")
	}
	if e.SyncUID == "" && e.Op != OpDeleteByWord {
		return errors.New("syncUid is required")
	}
	switch e.Entity {
	case EntityWordRecords, EntityChapterRecords, EntityReviewRecords, EntityConfig:
	default:
		return fmt.Errorf("unsupported entity: %s", e.Entity)
	}
	switch e.Op {
	case OpCreate, OpUpdate, OpDelete, OpDeleteByWord:
	default:
		return fmt.Errorf("unsupported op: %s", e.Op)
	}
	return nil
}

// SnapshotRequest is sent via NATS Request-Reply on sync.req.snapshot.{userId}
// when a new device initializes or an offline device falls behind the JetStream window.
type SnapshotRequest struct {
	AfterRow int64  `json:"afterRow,omitempty"`
	UserID   string `json:"userId"`
	DeviceID string `json:"deviceId"`
	AfterSeq int64  `json:"afterSeq,omitempty"`
}

// SnapshotRecord represents one active authoritative record in the shared database.
type SnapshotRecord struct {
	Deleted   bool            `json:"deleted,omitempty"`
	Entity    string          `json:"entity"`
	SyncUID   string          `json:"syncUid"`
	Payload   json.RawMessage `json:"payload"`
	UpdatedAt int64           `json:"updatedAt"`
	ServerSeq int64           `json:"serverSeq"`
}

// SnapshotResponse returns the authoritative state and the latest server sequence.
type SnapshotResponse struct {
	NextAfterRow    int64            `json:"nextAfterRow,omitempty"`
	NextAfterSeq    int64            `json:"nextAfterSeq,omitempty"`
	UserID          string           `json:"userId"`
	LatestServerSeq int64            `json:"latestServerSeq"`
	Records         []SnapshotRecord `json:"records"`
}
