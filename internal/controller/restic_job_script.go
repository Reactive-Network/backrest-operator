package controller

import (
	"fmt"
	"strings"

	operatorv1alpha1 "github.com/Reactive-Network/backrest-operator/api/v1alpha1"
	"github.com/Reactive-Network/backrest-operator/internal/backrest"
)

// Shared restic Job CLI flags. --json emits machine-readable progress on stdout
// (message_type=status with percent_done) so UIs can tail Job logs.
const (
	resticRetryLockFlag = "--retry-lock 5m"
	resticJSONFlag      = "--json"
)

// resticBackupJobScript builds the shell script for PVCBackup restic Jobs.
func resticBackupJobScript(paths []string, excludes []string, planID, instance string, keepLast *int32) string {
	var b strings.Builder
	b.WriteString("restic unlock || true; restic snapshots >/dev/null 2>&1 || restic init; ")
	b.WriteString("restic backup ")
	b.WriteString(resticJSONFlag)
	b.WriteByte(' ')
	b.WriteString(resticRetryLockFlag)
	b.WriteByte(' ')
	b.WriteString(strings.Join(shellQuote(paths), " "))
	for _, ex := range excludes {
		b.WriteString(" --exclude ")
		b.WriteString(shellQuoteOne(ex))
	}
	b.WriteString(" --tag ")
	b.WriteString(shellQuoteOne(backrest.PlanTag(planID)))
	b.WriteString(" --tag ")
	b.WriteString(shellQuoteOne(backrest.InstanceTag(instance)))
	if keepLast != nil {
		// Empty --group-by: Job pods use unique hostnames, so host grouping would keep everything;
		// plan-scoped tags leave orphans after backup selection flips between nodes.
		b.WriteString("; ")
		b.WriteString(resticForgetPruneClause(*keepLast))
	}
	return b.String()
}

func resticForgetPruneClause(keepLast int32) string {
	return fmt.Sprintf("restic forget %s %s --group-by '' --keep-last %d --prune || true",
		resticJSONFlag, resticRetryLockFlag, keepLast)
}

func pruneJobScript(b *operatorv1alpha1.PVCBackup) string {
	keepLast := *b.Spec.Retention.KeepLast
	// Repo-wide retention: backup Jobs use unique pod hostnames, so default
	// host grouping never expires anything. Empty --group-by keeps the newest
	// N snapshots in the whole repository (including leftover plans from
	// previous selected nodes). Plan-scoped --tag forget left those orphans.
	return "restic unlock || true; " + resticForgetPruneClause(keepLast)
}

func resticRestoreJobScript(snapshotID string, pathFilters []string) string {
	var b strings.Builder
	b.WriteString("restic unlock || true; restic restore ")
	b.WriteString(resticJSONFlag)
	b.WriteByte(' ')
	b.WriteString(resticRetryLockFlag)
	b.WriteByte(' ')
	b.WriteString(shellQuoteOne(snapshotID))
	b.WriteString(" --target /data")
	for _, p := range pathFilters {
		b.WriteString(" --include ")
		b.WriteString(shellQuoteOne(p))
	}
	return b.String()
}
