// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package templatecenter

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/db/models"
)

// A hybrid snapshot publishes asynchronously: the RPC returns once the data is
// durable locally, then a background merge copies the local overlay into the
// shared image. Deleting during that window would either fail on the publisher's
// exclusive lock ("cubestorcli ... is in use") or remove a volume mid-copy, so
// delete must refuse until remote_status settles.
func TestDeleteSnapshotRefusesWhileRemotePublishing(t *testing.T) {
	t.Parallel()

	for _, status := range []string{
		constants.RemoteStatusInProgress,
		constants.RemoteStatusPending,
		constants.RemoteStatusRunning,
	} {
		rec := &models.SnapshotRecord{Backend: constants.SnapshotBackendHybrid, RemoteStatus: status}
		err := ensureSnapshotRemoteSettled(rec, "snap-publishing")
		require.Error(t, err, "remote_status=%s must block delete", status)
		require.True(t, errors.Is(err, ErrTemplateAttemptInProgress), "got %v", err)
	}
}

// Once the merge finished the package is complete in the shared image and can be
// deleted normally. A failed publish is also deletable: nothing complete exists
// remotely, so removing the leftovers is the only cleanup path.
func TestDeleteSnapshotAllowsSettledRemoteStates(t *testing.T) {
	t.Parallel()

	for _, status := range []string{
		constants.RemoteStatusReady,
		constants.RemoteStatusFailed,
		"", // rows predating remote tracking
	} {
		rec := &models.SnapshotRecord{Backend: constants.SnapshotBackendHybrid, RemoteStatus: status}
		require.NoError(t, ensureSnapshotRemoteSettled(rec, "snap-settled"),
			"remote_status=%q must allow delete", status)
	}
}

// Backends that keep everything locally (xfs) or upload independent objects (s3)
// have no shared image to protect: the check must not touch them.
func TestDeleteSnapshotRemoteCheckSkipsLocalBackends(t *testing.T) {
	t.Parallel()

	for _, backend := range []string{"", constants.SnapshotBackendXFS, constants.SnapshotBackendS3} {
		rec := &models.SnapshotRecord{Backend: backend, RemoteStatus: constants.RemoteStatusInProgress}
		require.NoError(t, ensureSnapshotRemoteSettled(rec, "snap-local"), "backend=%q", backend)
	}
	require.NoError(t, ensureSnapshotRemoteSettled(nil, "snap-nil"))
}

// jfs publishes into the shared namespace the same way, so it gets the same gate.
func TestDeleteSnapshotRemoteCheckCoversJFS(t *testing.T) {
	t.Parallel()

	rec := &models.SnapshotRecord{Backend: constants.SnapshotBackendJFS, RemoteStatus: constants.RemoteStatusInProgress}
	require.Error(t, ensureSnapshotRemoteSettled(rec, "snap-jfs"))
}
