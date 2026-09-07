package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type PVCBackupSpec struct {
	// PVCName is a single claim to back up. Prefer PVCNames for multi-volume plans.
	PVCName string `json:"pvcName,omitempty"`
	// PVCNames backs up multiple PVCs in one quiesce window (same Job).
	PVCNames                []string              `json:"pvcNames,omitempty"`
	RepositoryRef           ObjectReference       `json:"repositoryRef"`
	Strategy                PVCBackupStrategy     `json:"strategy,omitempty"`
	VolumeSnapshotClassName string                `json:"volumeSnapshotClassName,omitempty"`
	Flush                   FlushSpec             `json:"flush,omitempty"`
	Quiesce                 QuiesceSpec           `json:"quiesce,omitempty"`
	Paths                   []string              `json:"paths,omitempty"`
	Excludes                []string              `json:"excludes,omitempty"`
	Schedule                string                `json:"schedule,omitempty"`
	Retention               PVCBackupRetention    `json:"retention,omitempty"`
	NodeSelector            map[string]string     `json:"nodeSelector,omitempty"`
	// Retries is the Job backoffLimit for the restic Job. Default 0 (no retries).
	Retries *int32 `json:"retries,omitempty"`
	// BackoffLimit is deprecated; use Retries. Kept for compatibility.
	BackoffLimit            *int32 `json:"backoffLimit,omitempty"`
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

type PVCBackupStrategy struct {
	Pipeline []string `json:"pipeline,omitempty"`
}

type FlushSpec struct {
	Enabled   bool                   `json:"enabled,omitempty"`
	Mode      string                 `json:"mode,omitempty"`
	TargetPod map[string]interface{} `json:"targetPod,omitempty"`
	Script    string                 `json:"script,omitempty"`
}

type QuiesceSpec struct {
	Enabled        bool             `json:"enabled,omitempty"`
	TimeoutSeconds int32            `json:"timeoutSeconds,omitempty"`
	LeaveDown      bool             `json:"leaveDown,omitempty"`
	Targets        []QuiesceTarget  `json:"targets,omitempty"`
}

type QuiesceTarget struct {
	APIVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	Namespace  string `json:"namespace,omitempty"`
	Action     string `json:"action,omitempty"`
}

type PVCBackupRetention struct {
	KeepLast                       *int32 `json:"keepLast,omitempty"`
	DeleteVolumeSnapshotAfterUpload *bool  `json:"deleteVolumeSnapshotAfterUpload,omitempty"`
}

type PVCBackupStatus struct {
	Phase                string      `json:"phase,omitempty"`
	LastBackupTime       string      `json:"lastBackupTime,omitempty"`
	LastSuccessTime      string      `json:"lastSuccessTime,omitempty"`
	LastSnapshotName     string      `json:"lastSnapshotName,omitempty"`
	LastResticSnapshotID string      `json:"lastResticSnapshotID,omitempty"`
	LastJobName          string      `json:"lastJobName,omitempty"`
	LastDurationSeconds  int64       `json:"lastDurationSeconds,omitempty"`
	LastForceRun         string      `json:"lastForceRun,omitempty"`
	LastForcePrune       string      `json:"lastForcePrune,omitempty"`
	Conditions           []Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type PVCBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              PVCBackupSpec   `json:"spec,omitempty"`
	Status            PVCBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type PVCBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PVCBackup `json:"items"`
}
