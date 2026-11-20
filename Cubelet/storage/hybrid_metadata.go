// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/pathutil"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

const hybridMetadataMarker = ".metadata-published"

// HybridLocalStorageRoot is outside the flat cubestor image directory.
func HybridLocalStorageRoot() string {
	return filepath.Join(CubeletStorageWorkPath(), cow.BackendHybrid, "packages")
}

func pathWithin(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

func hybridCatalogPresent(entry *SnapshotCatalogEntry) bool {
	_, err := os.Stat(filepath.Join(entry.MetaDir, snapshotCatalogFileName))
	return err == nil
}

// Catalog paths refer to the copy actually found, not the producing node's
// absolute paths. This also supports shared-only legacy packages.
func rebaseHybridCatalog(entry *SnapshotCatalogEntry, catalog string) error {
	entry.MetaDir = filepath.Dir(catalog)
	entry.SnapshotPath = filepath.Dir(entry.MetaDir)
	if filepath.Base(entry.SnapshotPath) != entry.SnapshotID {
		return ErrSnapshotCatalogNotFound // never discover .tmp or publisher staging
	}
	if entry.HybridMetadataVersion > 0 && pathWithin(HybridStorageRoot(), entry.SnapshotPath) {
		body, err := os.ReadFile(filepath.Join(entry.SnapshotPath, hybridMetadataMarker))
		if err != nil || string(body) != entry.SnapshotID {
			return ErrSnapshotCatalogNotFound
		}
	}
	return nil
}

func syncHybridPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func hybridRemoteHome(entry *SnapshotCatalogEntry) string {
	return filepath.Join(HybridStorageRoot(), normalizeSnapshotKind(entry.Kind), entry.SnapshotID)
}

func hybridMetadataPublished(ctx context.Context, id string) (bool, error) {
	entry, err := GetLocalSnapshotFor(ctx, cow.BackendHybrid, id)
	if err != nil {
		return false, err
	}
	if entry.HybridMetadataVersion == 0 {
		return true, nil
	} // legacy shared metadata
	body, err := os.ReadFile(filepath.Join(hybridRemoteHome(entry), hybridMetadataMarker))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return err == nil && string(body) == id, err
}

// publishHybridMetadata runs after all data images have been uploaded. Each
// file is synced on the writeback-disabled shared mount before the whole
// package becomes visible through an atomic directory rename.
func publishHybridMetadata(ctx context.Context, id string) error {
	entry, err := GetLocalSnapshotFor(ctx, cow.BackendHybrid, id)
	if err != nil {
		return err
	}
	if entry.HybridMetadataVersion == 0 {
		return nil
	}
	if err := pathutil.ValidateSafeID(id); err != nil {
		return err
	}
	ready, err := hybridMetadataPublished(ctx, id)
	if err != nil || ready {
		return err
	}
	src, dst := entry.SnapshotPath, hybridRemoteHome(entry)
	if !pathWithin(HybridLocalStorageRoot(), src) {
		return fmt.Errorf("hybrid package %s has no complete local metadata", id)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(filepath.Dir(dst), "."+id+"-publish-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	var dirs []string
	err = filepath.WalkDir(src, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		dest := filepath.Join(tmp, rel)
		if d.IsDir() {
			dirs = append(dirs, dest)
			return os.MkdirAll(dest, 0755)
		}
		if d.Type()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			if filepath.IsAbs(target) || !pathWithin(src, filepath.Join(filepath.Dir(path), target)) {
				return fmt.Errorf("hybrid metadata link escapes package: %s", path)
			}
			return os.Symlink(target, dest)
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("unsupported hybrid metadata file: %s", path)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if err != nil {
			return err
		}
		var reader io.Reader = in
		if rel == filepath.Join(SnapshotMetadataDir, snapshotCatalogFileName) {
			// Older nodes read the persisted paths without rebasing them.
			remote := cloneCatalogEntry(entry)
			remote.SnapshotPath = dst
			remote.MetaDir = filepath.Join(dst, SnapshotMetadataDir)
			body, marshalErr := json.MarshalIndent(remote, "", "  ")
			if marshalErr != nil {
				out.Close()
				return marshalErr
			}
			reader = strings.NewReader(string(body))
		}
		_, copyErr := io.Copy(out, reader)
		if copyErr == nil {
			copyErr = out.Sync()
		}
		return errors.Join(copyErr, out.Close())
	})
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(tmp, hybridMetadataMarker), []byte(id), 0644); err != nil {
		return err
	}
	if err := syncHybridPath(filepath.Join(tmp, hybridMetadataMarker)); err != nil {
		return err
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := syncHybridPath(dirs[i]); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		return err
	}
	return syncHybridPath(filepath.Dir(dst))
}

// Wait for an in-flight publication before deleting its source objects. Normal
// Master cleanup arrives after remote ready, but failed commits and pause GC
// can arrive earlier. Do not let a late publisher resurrect a deleted package.
func WaitHybridSnapshotPublish(ctx context.Context, id string) error {
	if localStorage == nil {
		return nil
	}
	hybrid, ok := localStorage.hybridManager.(*HybridStore)
	if !ok || hybrid == nil {
		return nil
	}
	cached, ok := hybridPublishers.Load(hybrid)
	if !ok {
		return nil
	}
	p := cached.(*hybridPublisher)
	p.mu.Lock()
	state := p.states[id]
	p.mu.Unlock()
	if state == nil {
		return nil
	}
	select {
	case <-state.done:
		return nil // failure also permits cleanup
	case <-ctx.Done():
		return ctx.Err()
	}
}

func RemoveHybridSnapshotMetadata(id string) error {
	if err := pathutil.ValidateSafeID(id); err != nil {
		return err
	}
	// Both kinds are checked because retries may have lost the local catalog.
	for _, root := range catalogKindRoots(cow.BackendHybrid) {
		if err := os.RemoveAll(filepath.Join(root, id)); err != nil {
			return err
		}
	}
	DeleteSnapshotCatalogFor(cow.BackendHybrid, id)
	return nil
}

// Persist the small local metadata files, including shim-written metadata.json,
// before acknowledging a locally recoverable package. Symlinks are not followed.
func syncHybridMetadataTree(home string) error {
	var dirs []string
	err := filepath.WalkDir(home, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			dirs = append(dirs, path)
			return nil
		}
		if d.Type().IsRegular() {
			return syncHybridPath(path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := syncHybridPath(dirs[i]); err != nil {
			return err
		}
	}
	return syncHybridPath(filepath.Dir(home))
}
