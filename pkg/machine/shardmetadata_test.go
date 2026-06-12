package machine

import (
	"strings"
	"testing"
)

func TestShardMetadata_EncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()
	m := Machine{
		ID:            "m-1",
		ShardMetadata: EncodeShardMetadata(900_000, 8192, 0.5, "fp-abc"),
	}
	if err := m.DecodeShardMetadata(); err != nil {
		t.Fatalf("DecodeShardMetadata: %v", err)
	}
	if m.AssignedPriority != 900_000 {
		t.Errorf("AssignedPriority = %d, want 900000", m.AssignedPriority)
	}
	if m.AssignedInterruptionPenaltyDollars != 8192 {
		t.Errorf("AssignedInterruptionPenaltyDollars = %v, want 8192", m.AssignedInterruptionPenaltyDollars)
	}
	if m.AssignedReclamationPenaltyDollars != 0.5 {
		t.Errorf("AssignedReclamationPenaltyDollars = %v, want 0.5", m.AssignedReclamationPenaltyDollars)
	}
	if m.AssignedNeedFingerprint != "fp-abc" {
		t.Errorf("AssignedNeedFingerprint = %q, want fp-abc", m.AssignedNeedFingerprint)
	}
}

// Absent keys leave fields untouched: the in-process fake delivers
// Assigned* directly on the struct, and decode must not zero them just
// because the echo map is empty or partial.
func TestShardMetadata_DecodeAbsentKeysLeaveFieldsUntouched(t *testing.T) {
	t.Parallel()
	m := Machine{
		AssignedPriority:                   42,
		AssignedInterruptionPenaltyDollars: 7,
		ShardMetadata:                      map[string]string{"some-future-shard/key": "x"},
	}
	if err := m.DecodeShardMetadata(); err != nil {
		t.Fatalf("DecodeShardMetadata: %v", err)
	}
	if m.AssignedPriority != 42 || m.AssignedInterruptionPenaltyDollars != 7 {
		t.Errorf("absent keys mutated fields: %+v", m)
	}
}

// One mangled entry must not void the rest of the machine's protection
// state — decode key-by-key, report the failure, keep the good values.
func TestShardMetadata_DecodeMalformedValueSkippedNotFatal(t *testing.T) {
	t.Parallel()
	m := Machine{ShardMetadata: map[string]string{
		ShardMetadataKeyAssignedPriority:            "not-a-number",
		ShardMetadataKeyAssignedInterruptionPenalty: "64",
		ShardMetadataKeyAssignedNeedFingerprint:     "fp-1",
	}}
	err := m.DecodeShardMetadata()
	if err == nil {
		t.Fatal("expected an error for the malformed priority")
	}
	if !strings.Contains(err.Error(), ShardMetadataKeyAssignedPriority) {
		t.Errorf("error doesn't name the bad key: %v", err)
	}
	if m.AssignedPriority != 0 {
		t.Errorf("malformed priority decoded to %d", m.AssignedPriority)
	}
	if m.AssignedInterruptionPenaltyDollars != 64 {
		t.Errorf("good penalty lost alongside the bad key: %v", m.AssignedInterruptionPenaltyDollars)
	}
	if m.AssignedNeedFingerprint != "fp-1" {
		t.Errorf("good fingerprint lost alongside the bad key: %q", m.AssignedNeedFingerprint)
	}
}

func TestShardMetadata_DecodeNilMapIsNoOp(t *testing.T) {
	t.Parallel()
	m := Machine{AssignedPriority: 5}
	if err := m.DecodeShardMetadata(); err != nil {
		t.Fatalf("DecodeShardMetadata on nil map: %v", err)
	}
	if m.AssignedPriority != 5 {
		t.Errorf("nil map mutated fields: %+v", m)
	}
}
