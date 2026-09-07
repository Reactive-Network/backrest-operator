package controller

import (
	"testing"

	operatorv1alpha1 "github.com/Reactive-Network/backrest-operator/api/v1alpha1"
)

func TestCreateResticBackupJobMountsPVCReadWrite(t *testing.T) {
	tests := []struct {
		name     string
		backup   operatorv1alpha1.PVCBackup
		pvcNames []string
	}{
		{
			name: "auto paths multi pvc",
			backup: operatorv1alpha1.PVCBackup{
				Spec: operatorv1alpha1.PVCBackupSpec{},
			},
			pvcNames: []string{"data-a", "data-b"},
		},
		{
			name: "explicit paths",
			backup: operatorv1alpha1.PVCBackup{
				Spec: operatorv1alpha1.PVCBackupSpec{
					Paths: []string{"/mnt/a", "/mnt/b"},
				},
			},
			pvcNames: []string{"pvc-a", "pvc-b"},
		},
		{
			name: "single explicit path reused",
			backup: operatorv1alpha1.PVCBackup{
				Spec: operatorv1alpha1.PVCBackupSpec{
					Paths: []string{"/custom"},
				},
			},
			pvcNames: []string{"only-pvc"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vols, mounts, _ := buildResticBackupJobVolumes(&tt.backup, tt.pvcNames)
			if len(vols) != len(tt.pvcNames) {
				t.Fatalf("volumes count = %d, want %d", len(vols), len(tt.pvcNames))
			}
			if len(mounts) != len(tt.pvcNames) {
				t.Fatalf("mounts count = %d, want %d", len(mounts), len(tt.pvcNames))
			}
			for i, vol := range vols {
				pvc := vol.VolumeSource.PersistentVolumeClaim
				if pvc == nil {
					t.Fatalf("volume %d: missing PVC source", i)
				}
				if pvc.ReadOnly {
					t.Fatalf("volume %d PVC ReadOnly = true, want false", i)
				}
				if pvc.ClaimName != tt.pvcNames[i] {
					t.Fatalf("volume %d claim = %q, want %q", i, pvc.ClaimName, tt.pvcNames[i])
				}
			}
			for i, mount := range mounts {
				if mount.ReadOnly {
					t.Fatalf("mount %d ReadOnly = true, want false", i)
				}
			}
		})
	}
}
