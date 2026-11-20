// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
package storage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

func setupHybridMetadataTest(t *testing.T) {
	t.Helper()
	old := localStorage
	localStorage = &local{config: &Config{DataPath: t.TempDir()}}
	oldRoots := snapshotCatalogRootsSnapshot(cow.BackendHybrid)
	SetSnapshotCatalogRootsFor(cow.BackendHybrid, catalogKindRoots(cow.BackendHybrid)...)
	t.Cleanup(func() { localStorage = old; SetSnapshotCatalogRootsFor(cow.BackendHybrid, oldRoots...) })
}

func TestHybridMetadataPublicationAndFallback(t *testing.T) {
	setupHybridMetadataTest(t)
	ctx := context.Background()
	id := "snap-metadata-test"
	home := SnapshotHome(cow.BackendHybrid, SnapshotKindNormal, id)
	require.True(t, pathWithin(HybridLocalStorageRoot(), home))
	require.NoError(t, EnsureSnapshotPackage(cow.BackendHybrid, home))
	meta := filepath.Join(home, SnapshotMetadataDir)
	require.NoError(t, os.WriteFile(filepath.Join(meta, "metadata.json"), []byte("state"), 0644))
	require.NoError(t, EnsureShimSpecDirLink(home, "2C2000M"))
	entry := &SnapshotCatalogEntry{SnapshotID: id, SnapshotPath: home, RootfsVol: "rootfs", Kind: CatalogKindRuntimeSnapshot}
	require.NoError(t, WriteSnapshotCatalogFor(cow.BackendHybrid, entry))
	require.Equal(t, 1, entry.HybridMetadataVersion)
	ready, err := hybridMetadataPublished(ctx, id)
	require.NoError(t, err)
	require.False(t, ready)
	// Cancellation cannot make a partial shared package discoverable.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, publishHybridMetadata(canceled, id), context.Canceled)
	require.NoFileExists(t, filepath.Join(hybridRemoteHome(entry), SnapshotMetadataDir, snapshotCatalogFileName))
	require.NoError(t, publishHybridMetadata(ctx, id))
	require.NoError(t, publishHybridMetadata(ctx, id)) // retry is idempotent
	ready, err = hybridMetadataPublished(ctx, id)
	require.NoError(t, err)
	require.True(t, ready)
	found, err := GetLocalSnapshotFor(ctx, cow.BackendHybrid, id)
	require.NoError(t, err)
	require.Equal(t, home, found.SnapshotPath)
	// Shared catalog is usable by older nodes which do not rebase paths.
	body, err := os.ReadFile(filepath.Join(hybridRemoteHome(entry), SnapshotMetadataDir, snapshotCatalogFileName))
	require.NoError(t, err)
	var remote SnapshotCatalogEntry
	require.NoError(t, json.Unmarshal(body, &remote))
	require.Equal(t, hybridRemoteHome(entry), remote.SnapshotPath)
	// Evict only the local metadata cache: a cached catalog must not mask fallback.
	require.NoError(t, os.RemoveAll(home))
	found, err = GetLocalSnapshotFor(ctx, cow.BackendHybrid, id)
	require.NoError(t, err)
	require.Equal(t, remote.SnapshotPath, found.SnapshotPath)
	got, err := os.ReadFile(filepath.Join(found.SnapshotPath, "2C2000M", "metadata.json"))
	require.NoError(t, err)
	require.Equal(t, "state", string(got))
	// A restart cannot report remote ready if the metadata marker is missing.
	require.NoError(t, os.Remove(filepath.Join(remote.SnapshotPath, hybridMetadataMarker)))
	DeleteSnapshotCatalogFor(cow.BackendHybrid, id)
	_, err = GetLocalSnapshotFor(ctx, cow.BackendHybrid, id)
	require.ErrorIs(t, err, ErrSnapshotCatalogNotFound)
	require.NoError(t, RemoveHybridSnapshotMetadata(id))
	require.NoDirExists(t, remote.SnapshotPath)
}

func TestHybridLegacyMetadataAndStaging(t *testing.T) {
	setupHybridMetadataTest(t)
	ctx := context.Background()
	id := "snap-legacy"
	home := filepath.Join(HybridStorageRoot(), SnapshotKindNormal, id)
	require.NoError(t, WriteSnapshotCatalogFor(cow.BackendHybrid, &SnapshotCatalogEntry{SnapshotID: id, SnapshotPath: home, RootfsVol: "rootfs"}))
	DeleteSnapshotCatalogFor(cow.BackendHybrid, id)
	entry, err := GetLocalSnapshotFor(ctx, cow.BackendHybrid, id)
	require.NoError(t, err)
	require.Equal(t, home, entry.SnapshotPath)
	ready, err := hybridMetadataPublished(ctx, id)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, publishHybridMetadata(ctx, id))
	// Discovery ignores unfinished local/staging catalogs, including legacy ones.
	require.NoError(t, os.Rename(home, home+".tmp"))
	DeleteSnapshotCatalogFor(cow.BackendHybrid, id)
	entries, err := ListLocalSnapshotsFor(ctx, cow.BackendHybrid)
	require.NoError(t, err)
	require.Empty(t, entries)
	_, err = GetLocalSnapshotFor(ctx, cow.BackendHybrid, id)
	require.True(t, errors.Is(err, ErrSnapshotCatalogNotFound))
}
