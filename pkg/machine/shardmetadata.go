package machine

import (
	"errors"
	"fmt"
	"strconv"
)

// Well-known shard_metadata keys (provider.proto, M72). These are
// BigFleet vocabulary, not provider vocabulary: the provider contract is
// store-and-echo-verbatim, so providers never see these names as fields
// they could be tempted to interpret. The shard writes them at Configure
// time and decodes them at reconcile ingest to rebuild the assignment
// state a restart would otherwise zero (preemption protection — paper §8
// victim scoring inputs — and Phase 1's 1:1 Need attribution).
//
// Prefix matches the repo's CRD group (bigfleet.lucy.sh) so a metadata
// dump is self-identifying.
const (
	ShardMetadataKeyAssignedPriority            = "bigfleet.lucy.sh/assigned-priority"
	ShardMetadataKeyAssignedInterruptionPenalty = "bigfleet.lucy.sh/assigned-interruption-penalty-dollars"
	ShardMetadataKeyAssignedReclamationPenalty  = "bigfleet.lucy.sh/assigned-reclamation-penalty-dollars"
	ShardMetadataKeyAssignedNeedFingerprint     = "bigfleet.lucy.sh/assigned-need-fingerprint"
)

// EncodeShardMetadata builds the shard_metadata map for one assignment.
// Called with the values executeBootstrap stamps on the machine at
// Configure time; the provider stores the map verbatim and echoes it on
// Get/List, making it the durable copy a restarted shard decodes back
// via Machine.DecodeShardMetadata.
func EncodeShardMetadata(priority int32, interruptionPenaltyDollars, reclamationPenaltyDollars float64, needFingerprint string) map[string]string {
	return map[string]string{
		ShardMetadataKeyAssignedPriority:            strconv.FormatInt(int64(priority), 10),
		ShardMetadataKeyAssignedInterruptionPenalty: strconv.FormatFloat(interruptionPenaltyDollars, 'g', -1, 64),
		ShardMetadataKeyAssignedReclamationPenalty:  strconv.FormatFloat(reclamationPenaltyDollars, 'g', -1, 64),
		ShardMetadataKeyAssignedNeedFingerprint:     needFingerprint,
	}
}

// DecodeShardMetadata restores the Assigned* fields from the
// provider-echoed m.ShardMetadata map. Each well-known key is decoded
// independently: absent keys leave the corresponding field untouched
// (an in-process fake may deliver Assigned* directly on the struct),
// unknown keys are ignored (they belong to a newer shard), and a
// malformed value is skipped so one mangled entry doesn't void the
// rest of the machine's protection state. All decode failures are
// joined into the returned error for the caller to log.
func (m *Machine) DecodeShardMetadata() error {
	md := m.ShardMetadata
	if len(md) == 0 {
		return nil
	}
	var errs []error
	if v, ok := md[ShardMetadataKeyAssignedPriority]; ok {
		if p, err := strconv.ParseInt(v, 10, 32); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", ShardMetadataKeyAssignedPriority, err))
		} else {
			m.AssignedPriority = int32(p)
		}
	}
	if v, ok := md[ShardMetadataKeyAssignedInterruptionPenalty]; ok {
		if d, err := strconv.ParseFloat(v, 64); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", ShardMetadataKeyAssignedInterruptionPenalty, err))
		} else {
			m.AssignedInterruptionPenaltyDollars = d
		}
	}
	if v, ok := md[ShardMetadataKeyAssignedReclamationPenalty]; ok {
		if d, err := strconv.ParseFloat(v, 64); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", ShardMetadataKeyAssignedReclamationPenalty, err))
		} else {
			m.AssignedReclamationPenaltyDollars = d
		}
	}
	if v, ok := md[ShardMetadataKeyAssignedNeedFingerprint]; ok {
		m.AssignedNeedFingerprint = v
	}
	return errors.Join(errs...)
}
