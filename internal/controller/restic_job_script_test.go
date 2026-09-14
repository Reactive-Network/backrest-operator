package controller

import (
	"strings"
	"testing"

	operatorv1alpha1 "github.com/Reactive-Network/backrest-operator/api/v1alpha1"
	"github.com/Reactive-Network/backrest-operator/internal/backrest"
)

func TestResticBackupJobScriptIncludesJSONProgress(t *testing.T) {
	t.Parallel()
	keep := int32(12)
	script := resticBackupJobScript(
		[]string{"/data/pvc"},
		[]string{"**/*.lock"},
		"ns-node-backup",
		"main",
		&keep,
	)
	for _, want := range []string{
		"restic backup --json --retry-lock 5m",
		shellQuoteOne("/data/pvc"),
		"--exclude " + shellQuoteOne("**/*.lock"),
		"--tag " + shellQuoteOne(backrest.PlanTag("ns-node-backup")),
		"--tag " + shellQuoteOne(backrest.InstanceTag("main")),
		"restic forget --json --retry-lock 5m --group-by '' --keep-last 12 --prune || true",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script missing %q\n%s", want, script)
		}
	}
}

func TestResticBackupJobScriptOmitsForgetWithoutKeepLast(t *testing.T) {
	t.Parallel()
	script := resticBackupJobScript([]string{"/data"}, nil, "p", "main", nil)
	if strings.Contains(script, "forget") {
		t.Fatalf("unexpected forget in script: %s", script)
	}
	if !strings.Contains(script, "restic backup --json") {
		t.Fatalf("missing json backup: %s", script)
	}
}

func TestPruneJobScriptIncludesJSON(t *testing.T) {
	t.Parallel()
	keep := int32(3)
	b := &operatorv1alpha1.PVCBackup{Spec: operatorv1alpha1.PVCBackupSpec{
		Retention: operatorv1alpha1.PVCBackupRetention{KeepLast: &keep},
	}}
	script := pruneJobScript(b)
	if !strings.Contains(script, "restic forget --json --retry-lock 5m") {
		t.Fatalf("prune script = %s", script)
	}
}

func TestResticRestoreJobScriptIncludesJSON(t *testing.T) {
	t.Parallel()
	script := resticRestoreJobScript("abcdef", []string{"/data/x"})
	for _, want := range []string{
		"restic restore --json --retry-lock 5m",
		shellQuoteOne("abcdef"),
		"--target /data",
		"--include " + shellQuoteOne("/data/x"),
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script missing %q\n%s", want, script)
		}
	}
}
