package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	operatorv1alpha1 "github.com/Reactive-Network/backrest-operator/api/v1alpha1"
)

var (
	// Alert-facing metrics (names must match VMRule / PrometheusRule).
	BackupFailedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "backrest_backup_failed_total",
		Help: "PVC backup failures (alert: BackrestBackupFailed)",
	}, []string{"namespace", "name"})

	BackupLastSuccessSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backrest_backup_last_success_timestamp_seconds",
		Help: "Unix timestamp of last successful backup (alert SLA)",
	}, []string{"namespace", "name"})

	RestoreFailedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "backrest_restore_failed_total",
		Help: "PVC restore failures (alert: BackrestRestoreFailed)",
	}, []string{"namespace", "name"})

	// Legacy / detailed counters kept for dashboards.
	BackupTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "backrest_operator_backup_total",
		Help: "PVC backup attempts",
	}, []string{"namespace", "name", "result"})

	BackupDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "backrest_operator_backup_duration_seconds",
		Help:    "PVC backup duration",
		Buckets: []float64{30, 60, 120, 300, 600, 1800, 3600, 7200},
	}, []string{"namespace", "name"})

	BackupLastSuccess = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backrest_operator_backup_last_success_timestamp",
		Help: "Unix timestamp of last successful backup (legacy name)",
	}, []string{"namespace", "name"})

	ReconcileErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "backrest_operator_reconcile_errors_total",
		Help: "Reconcile errors",
	}, []string{"kind"})
)

// Register into controller-runtime's registry, not prometheus.DefaultRegisterer.
// The operator's only /metrics endpoint is the one controller-runtime serves
// (metricsserver.Options{BindAddress} in cmd/operator/main.go), and that endpoint
// gathers exclusively from crmetrics.Registry. Registering into the default registry
// meant every backrest_* series was collected in-process and never exposed: the scrape
// target was up and returning controller_runtime_* and go_*, so nothing looked broken,
// while alerts keyed on backrest_backup_last_success_timestamp_seconds could never fire.
func init() {
	crmetrics.Registry.MustRegister(
		BackupFailedTotal,
		BackupLastSuccessSeconds,
		RestoreFailedTotal,
		BackupTotal,
		BackupDuration,
		BackupLastSuccess,
		ReconcileErrors,
	)
}

// SyncBackupLastSuccess sets both last-success gauges without incrementing counters.
func SyncBackupLastSuccess(namespace, name string, unixTs float64) {
	BackupLastSuccess.WithLabelValues(namespace, name).Set(unixTs)
	BackupLastSuccessSeconds.WithLabelValues(namespace, name).Set(unixTs)
}

// LastSuccessUnixFromStatus derives the unix timestamp for gauge seed/sync from CR status.
func LastSuccessUnixFromStatus(status operatorv1alpha1.PVCBackupStatus) float64 {
	if status.LastSuccessTime != "" {
		if t, err := time.Parse(time.RFC3339, status.LastSuccessTime); err == nil {
			return float64(t.Unix())
		}
	}
	if status.Phase == "Succeeded" || status.Phase == "Scheduled" {
		if status.LastBackupTime != "" {
			if t, err := time.Parse(time.RFC3339, status.LastBackupTime); err == nil {
				return float64(t.Unix())
			}
			return 0
		}
	}
	return 0
}

// ObserveBackupSuccess updates success metrics used by SLA alerts.
func ObserveBackupSuccess(namespace, name string, unixTs float64) {
	BackupTotal.WithLabelValues(namespace, name, "success").Inc()
	SyncBackupLastSuccess(namespace, name, unixTs)
}

// ForgetBackup drops every per-PVCBackup series once the CR is gone. Label sets on a
// *Vec live in process memory until deleted, so without this a deleted PVCBackup kept
// exporting its last gauge value until the operator restarted: one that never succeeded
// sat at 0 and fired CatalystNetworkBackupNeverSucceeded, one that had succeeded aged
// into CatalystNetworkBackupStale, both for disks that no longer exist.
func ForgetBackup(namespace, name string) {
	labels := prometheus.Labels{"namespace": namespace, "name": name}
	BackupFailedTotal.DeletePartialMatch(labels)
	BackupLastSuccessSeconds.DeletePartialMatch(labels)
	BackupTotal.DeletePartialMatch(labels)
	BackupDuration.DeletePartialMatch(labels)
	BackupLastSuccess.DeletePartialMatch(labels)
}

// ObserveBackupFailure increments failure counters used by BackrestBackupFailed.
func ObserveBackupFailure(namespace, name string) {
	BackupTotal.WithLabelValues(namespace, name, "failure").Inc()
	BackupFailedTotal.WithLabelValues(namespace, name).Inc()
}

// ObserveRestoreFailure increments restore failure counters.
func ObserveRestoreFailure(namespace, name string) {
	RestoreFailedTotal.WithLabelValues(namespace, name).Inc()
}

// gaugeValue reads the current value of a GaugeVec label set (for tests).
func gaugeValue(g *prometheus.GaugeVec, namespace, name string) (float64, bool) {
	metric := &dto.Metric{}
	if err := g.WithLabelValues(namespace, name).Write(metric); err != nil {
		return 0, false
	}
	return metric.GetGauge().GetValue(), true
}

// StartServer is gone: it had no callers and served promhttp.Handler(), i.e. the
// default registry, which is exactly the endpoint/registry mismatch that kept
// backrest_* metrics invisible. The operator's metrics come from controller-runtime's
// server; there is no second endpoint to start.
