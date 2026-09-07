package metrics

import (
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

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
