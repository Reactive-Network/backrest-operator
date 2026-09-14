package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	operatorv1alpha1 "github.com/Reactive-Network/backrest-operator/api/v1alpha1"
)

func TestSyncBackupLastSuccessSetsBothGauges(t *testing.T) {
	const ns, name = "test-ns", "test-backup"
	const want = 1700000000.0

	SyncBackupLastSuccess(ns, name, want)

	if got, ok := gaugeValue(BackupLastSuccessSeconds, ns, name); !ok || got != want {
		t.Fatalf("BackupLastSuccessSeconds = %v, ok=%v; want %v", got, ok, want)
	}
	if got, ok := gaugeValue(BackupLastSuccess, ns, name); !ok || got != want {
		t.Fatalf("BackupLastSuccess = %v, ok=%v; want %v", got, ok, want)
	}

	SyncBackupLastSuccess(ns, name, 0)

	if got, ok := gaugeValue(BackupLastSuccessSeconds, ns, name); !ok || got != 0 {
		t.Fatalf("BackupLastSuccessSeconds after zero = %v, ok=%v; want 0", got, ok)
	}
	if got, ok := gaugeValue(BackupLastSuccess, ns, name); !ok || got != 0 {
		t.Fatalf("BackupLastSuccess after zero = %v, ok=%v; want 0", got, ok)
	}
}

func TestLastSuccessUnixFromStatus(t *testing.T) {
	validTS := "2023-11-14T22:13:20Z"
	validUnix := float64(mustParseRFC3339(t, validTS).Unix())

	tests := []struct {
		name   string
		status operatorv1alpha1.PVCBackupStatus
		want   float64
	}{
		{
			name:   "valid lastSuccessTime",
			status: operatorv1alpha1.PVCBackupStatus{LastSuccessTime: validTS},
			want:   validUnix,
		},
		{
			name: "invalid lastSuccessTime fallback scheduled lastBackupTime",
			status: operatorv1alpha1.PVCBackupStatus{
				Phase:           "Scheduled",
				LastSuccessTime: "not-a-date",
				LastBackupTime:  validTS,
			},
			want: validUnix,
		},
		{
			name: "invalid lastSuccessTime phase failed",
			status: operatorv1alpha1.PVCBackupStatus{
				Phase:           "Failed",
				LastSuccessTime: "not-a-date",
				LastBackupTime:  validTS,
			},
			want: 0,
		},
		{
			name: "failed only lastBackupTime",
			status: operatorv1alpha1.PVCBackupStatus{
				Phase:          "Failed",
				LastBackupTime: validTS,
			},
			want: 0,
		},
		{
			name: "failed with valid lastSuccessTime",
			status: operatorv1alpha1.PVCBackupStatus{
				Phase:           "Failed",
				LastSuccessTime: validTS,
				LastBackupTime:  "2024-01-01T00:00:00Z",
			},
			want: validUnix,
		},
		{
			name: "invalid both fields scheduled",
			status: operatorv1alpha1.PVCBackupStatus{
				Phase:           "Scheduled",
				LastSuccessTime: "bad",
				LastBackupTime:  "also-bad",
			},
			want: 0,
		},
		{
			name:   "all empty",
			status: operatorv1alpha1.PVCBackupStatus{},
			want:   0,
		},
		{
			name: "succeeded migration fallback lastBackupTime",
			status: operatorv1alpha1.PVCBackupStatus{
				Phase:          "Succeeded",
				LastBackupTime: validTS,
			},
			want: validUnix,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := LastSuccessUnixFromStatus(tt.status)
			if got != tt.want {
				t.Fatalf("LastSuccessUnixFromStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestObserveBackupSuccessStillIncrementsCounters(t *testing.T) {
	const ns, name = "observe-ns", "observe-backup"
	before := counterValue(t, BackupTotal.WithLabelValues(ns, name, "success"))

	ObserveBackupSuccess(ns, name, 1234567890.0)

	after := counterValue(t, BackupTotal.WithLabelValues(ns, name, "success"))
	if after != before+1 {
		t.Fatalf("success counter increment: before=%v after=%v", before, after)
	}
	if got, ok := gaugeValue(BackupLastSuccessSeconds, ns, name); !ok || got != 1234567890.0 {
		t.Fatalf("BackupLastSuccessSeconds = %v, ok=%v", got, ok)
	}
}

func counterValue(t *testing.T, c prometheusCounter) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := c.Write(m); err != nil {
		t.Fatalf("write counter: %v", err)
	}
	return m.GetCounter().GetValue()
}

type prometheusCounter interface {
	Write(*dto.Metric) error
}

func mustParseRFC3339(t *testing.T, s string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse RFC3339 %q: %v", s, err)
	}
	return parsed
}

// TestAlertMetricsAreExposedByControllerRuntimeRegistry pins the registry the metrics
// land in. The operator serves /metrics only from controller-runtime's metrics server,
// which gathers exclusively from crmetrics.Registry; these metrics used to be
// registered into prometheus.DefaultRegisterer instead, so they were collected
// in-process and never scraped. Nothing looked broken -- the scrape target stayed up
// and served controller_runtime_* and go_* -- while every alert keyed on a backrest_*
// series sat silently at "no data" for months.
func TestAlertMetricsAreExposedByControllerRuntimeRegistry(t *testing.T) {
	// Registering an already-registered collector returns AlreadyRegisteredError, which
	// is a direct assertion of membership. A Gather would miss *Vec collectors that have
	// no child series yet (an untouched HistogramVec reports no family at all), so it
	// cannot tell "registered elsewhere" from "registered here but unused".
	collectors := map[string]prometheus.Collector{
		"backrest_backup_last_success_timestamp_seconds":  BackupLastSuccessSeconds,
		"backrest_backup_failed_total":                    BackupFailedTotal,
		"backrest_restore_failed_total":                   RestoreFailedTotal,
		"backrest_operator_backup_total":                  BackupTotal,
		"backrest_operator_backup_duration_seconds":       BackupDuration,
		"backrest_operator_backup_last_success_timestamp": BackupLastSuccess,
		"backrest_operator_reconcile_errors_total":        ReconcileErrors,
	}
	for name, c := range collectors {
		err := crmetrics.Registry.Register(c)
		if err == nil {
			// It was not in there: we just added it, so undo and fail.
			crmetrics.Registry.Unregister(c)
			t.Errorf("%s is not registered with the scraped registry; alerts on it can never fire", name)
			continue
		}
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			t.Errorf("%s: unexpected register error: %v", name, err)
		}
	}

	// End-to-end for the metric the backup SLA alert reads: set a series and confirm it
	// comes out of the endpoint's registry.
	SyncBackupLastSuccess("gather-ns", "gather-backup", 1700000000)
	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather from controller-runtime registry: %v", err)
	}
	for _, f := range families {
		if f.GetName() == "backrest_backup_last_success_timestamp_seconds" {
			return
		}
	}
	t.Fatal("backrest_backup_last_success_timestamp_seconds missing from gather; CatalystNetworkBackupStale cannot fire")
}
