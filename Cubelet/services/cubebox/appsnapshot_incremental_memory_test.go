// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package cubebox

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/log"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// installAppSnapshotMemoryHooks swaps the memory-artifact collaborators for the
// duration of one test. AppSnapshot has no direct unit-test seam (it runs the
// whole create -> capture -> destroy pipeline), so the decision function it now
// delegates to is tested on its own.
func installAppSnapshotMemoryHooks(
	t *testing.T,
	base *storage.CowSnapshotObject,
	baseErr error,
	clone func(context.Context, string, *storage.CowSnapshotObject, string, uint64) (*storage.CowSnapshotObject, error),
	create func(context.Context, string, string, uint64) (*storage.CowSnapshotObject, error),
) {
	t.Helper()
	origResolve := resolveMemoryObjectFromSnapshotFn
	origClone := commitMemoryFromBaseFor
	origCreate := createMemoryVolumeFor
	t.Cleanup(func() {
		resolveMemoryObjectFromSnapshotFn = origResolve
		commitMemoryFromBaseFor = origClone
		createMemoryVolumeFor = origCreate
	})
	resolveMemoryObjectFromSnapshotFn = func(context.Context, string, string) (*storage.CowSnapshotObject, error) {
		return base, baseErr
	}
	if clone != nil {
		commitMemoryFromBaseFor = clone
	}
	if create != nil {
		createMemoryVolumeFor = create
	}
}

// A template with a committed memory image must be cloned as the restore base
// and captured incrementally: that is what makes AppSnapshot write only the
// pages the guest dirtied instead of rewriting the whole image.
func TestPrepareAppSnapshotMemoryClonesTemplateBase(t *testing.T) {
	base := &storage.CowSnapshotObject{
		Name:    "tpl-t-memory",
		Kind:    storage.CowKindVolume,
		DevPath: "/dev/nbd0",
	}
	clonedName := ""
	installAppSnapshotMemoryHooks(t, base, nil,
		func(_ context.Context, _ string, src *storage.CowSnapshotObject, id string, _ uint64) (*storage.CowSnapshotObject, error) {
			require.Equal(t, base.Name, src.Name, "must clone the template memory image")
			clonedName = "tpl-" + id + "-memory"
			return &storage.CowSnapshotObject{
				Name:    clonedName,
				Kind:    storage.CowKindSnapshot,
				DevPath: "/dev/nbd1",
			}, nil
		},
		func(context.Context, string, string, uint64) (*storage.CowSnapshotObject, error) {
			t.Fatal("must not create an empty volume when a base is available")
			return nil, nil
		})

	memoryObject, snapshotType, err := prepareAppSnapshotMemoryArtifact(
		context.Background(), log.G(context.Background()), cow.BackendHybrid, "tpl-t", 2<<30,
	)
	require.NoError(t, err)
	require.Equal(t, snapshotTypeIncremental, snapshotType,
		"a cloned base must be captured incrementally, not as a full dump")
	require.Equal(t, clonedName, memoryObject.Name)
}

// A rootfs-only template has no memory image. The lookup fails and the snapshot
// must still work: an empty volume plus a full capture, exactly as before.
func TestPrepareAppSnapshotMemoryFallsBackToFullWithoutBase(t *testing.T) {
	created := ""
	installAppSnapshotMemoryHooks(t, nil, ErrNoBaseMemoryForIncremental,
		func(context.Context, string, *storage.CowSnapshotObject, string, uint64) (*storage.CowSnapshotObject, error) {
			t.Fatal("must not clone when there is no base")
			return nil, nil
		},
		func(_ context.Context, _ string, id string, _ uint64) (*storage.CowSnapshotObject, error) {
			created = "tpl-" + id + "-memory"
			return &storage.CowSnapshotObject{
				Name:    created,
				Kind:    storage.CowKindVolume,
				DevPath: "/dev/nbd2",
			}, nil
		})

	memoryObject, snapshotType, err := prepareAppSnapshotMemoryArtifact(
		context.Background(), log.G(context.Background()), cow.BackendHybrid, "tpl-t", 2<<30,
	)
	require.NoError(t, err)
	require.Equal(t, snapshotTypeFull, snapshotType)
	require.Equal(t, created, memoryObject.Name)
}

// A base that exists but cannot be cloned (backend error) must not fail the
// snapshot: fall back to the empty-volume full capture.
func TestPrepareAppSnapshotMemoryFallsBackWhenCloneFails(t *testing.T) {
	base := &storage.CowSnapshotObject{Name: "tpl-t-memory", Kind: storage.CowKindVolume}
	installAppSnapshotMemoryHooks(t, base, nil,
		func(context.Context, string, *storage.CowSnapshotObject, string, uint64) (*storage.CowSnapshotObject, error) {
			return nil, errors.New("copy_file_range failed")
		},
		func(_ context.Context, _ string, id string, _ uint64) (*storage.CowSnapshotObject, error) {
			return &storage.CowSnapshotObject{
				Name:    "tpl-" + id + "-memory",
				Kind:    storage.CowKindVolume,
				DevPath: "/dev/nbd3",
			}, nil
		})

	_, snapshotType, err := prepareAppSnapshotMemoryArtifact(
		context.Background(), log.G(context.Background()), cow.BackendHybrid, "tpl-t", 2<<30,
	)
	require.NoError(t, err)
	require.Equal(t, snapshotTypeFull, snapshotType)
}

// A taken destination name is a real conflict: another attempt owns the volume,
// so silently capturing full into it would corrupt that attempt's image.
func TestPrepareAppSnapshotMemorySurfacesNameConflict(t *testing.T) {
	base := &storage.CowSnapshotObject{Name: "tpl-t-memory", Kind: storage.CowKindVolume}
	installAppSnapshotMemoryHooks(t, base, nil,
		func(context.Context, string, *storage.CowSnapshotObject, string, uint64) (*storage.CowSnapshotObject, error) {
			return nil, storage.ErrCowObjectAlreadyExists
		},
		func(context.Context, string, string, uint64) (*storage.CowSnapshotObject, error) {
			t.Fatal("must not fall back to a full capture on a name conflict")
			return nil, nil
		})

	_, _, err := prepareAppSnapshotMemoryArtifact(
		context.Background(), log.G(context.Background()), cow.BackendHybrid, "tpl-t", 2<<30,
	)
	require.ErrorIs(t, err, storage.ErrCowObjectAlreadyExists)
}
