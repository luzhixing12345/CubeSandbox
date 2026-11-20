// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
package templatecenter

import (
	"context"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/db/models"
	sandboxtypes "github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/types"
	"gorm.io/gorm"
)

// Existing orchestration fixtures mock repositories. Keep those fixtures at
// that boundary; the transaction itself is exercised below with a real DB.
func patchSnapshotCompletionForTest(p *gomonkey.Patches) {
	p.ApplyFunc(completeSnapshotCreate, func(ctx context.Context, id, typ string, replica ReplicaStatus, snapshot map[string]any, job string, updates map[string]any) error {
		if err := UpsertReplica(ctx, id, typ, replica); err != nil {
			return err
		}
		if err := updateSnapshotFields(ctx, id, snapshot); err != nil {
			return err
		}
		return updateTemplateImageJob(ctx, job, updates)
	})
}

func TestSnapshotCompletionAtomic(t *testing.T) {
	db := newMigrateSharedStoreTestDB(t)
	require.NoError(t, db.AutoMigrate(&models.SnapshotRecord{}, &models.TemplateReplica{}, &models.TemplateImageJob{}))
	require.NoError(t, db.Create(&models.SnapshotRecord{SnapshotID: "snap-atomic", Status: StatusCreating}).Error)
	replica := ReplicaStatus{NodeID: "node", Status: ReplicaStatusReady}
	finish := func() error {
		return completeSnapshotCreate(context.Background(), "snap-atomic", "cubebox", replica, map[string]any{"status": StatusReady, "request_json": "{\"envd\":\"v1\"}"}, "job", map[string]any{"status": JobStatusReady, "progress": 100})
	}
	require.ErrorIs(t, finish(), gorm.ErrRecordNotFound) // final write fails
	var rec models.SnapshotRecord
	require.NoError(t, db.Where("snapshot_id = ?", "snap-atomic").First(&rec).Error)
	require.Equal(t, StatusCreating, rec.Status)
	var count int64
	require.NoError(t, db.Model(&models.TemplateReplica{}).Count(&count).Error)
	require.Zero(t, count)
	require.NoError(t, db.Create(&models.TemplateImageJob{JobID: "job", TemplateID: "snap-atomic", RequestID: "req", Status: JobStatusRunning}).Error)
	require.NoError(t, finish())
	require.NoError(t, db.Where("snapshot_id = ?", "snap-atomic").First(&rec).Error)
	require.Equal(t, StatusReady, rec.Status)
	var job models.TemplateImageJob
	require.NoError(t, db.Where("job_id = ?", "job").First(&job).Error)
	require.Equal(t, JobStatusReady, job.Status)
	require.NoError(t, db.Model(&models.TemplateReplica{}).Count(&count).Error)
	require.EqualValues(t, 1, count)
}

func testSnapshotArtifactConcurrentReaders(t *testing.T, db *gorm.DB) {
	t.Helper()
	id := "rfs-shared-lock-test"
	require.NoError(t, db.Create(&models.RootfsArtifact{ArtifactID: id, TemplateSpecFingerprint: id, Status: ArtifactStatusReady}).Error)
	req := &sandboxtypes.CreateCubeSandboxReq{Annotations: map[string]string{constants.CubeAnnotationRootfsArtifactID: id}}
	tx := db.Begin()
	require.NoError(t, tx.Error)
	defer tx.Rollback()
	require.NoError(t, createSnapshotTx(context.Background(), tx, "shared-lock-a", req, "cubebox", "v2", nil))
	done := make(chan error, 1)
	go func() {
		done <- db.Transaction(func(other *gorm.DB) error {
			return createSnapshotTx(context.Background(), other, "shared-lock-b", req, "cubebox", "v2", nil)
		})
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		tx.Rollback()
		<-done
		t.Fatal("snapshot readers serialized on the artifact")
	}
	require.NoError(t, tx.Commit().Error)
}
