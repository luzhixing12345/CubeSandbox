// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

func newFakeHybridStore(t *testing.T) (*HybridStore, *[]string) {
	t.Helper()
	root := t.TempDir()
	localDir := filepath.Join(root, "local")
	sharedDir := filepath.Join(root, "shared")
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(sharedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	device := "/dev/nbd7"
	calls := []string{}
	attached := map[string]bool{}
	var runnerMu sync.Mutex
	store := &HybridStore{
		cubestorCLIPath: "cubestorcli",
		socketPath:      filepath.Join(root, "cubestor.sock"),
		localDir:        localDir,
		sharedDir:       sharedDir,
		timeout:         time.Second,
	}
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		runnerMu.Lock()
		defer runnerMu.Unlock()
		calls = append(calls, strings.Join(args, " "))
		switch args[0] {
		case "devices":
			var lines []string
			for name := range attached {
				lines = append(lines, "nbd7 size 8388608 plugin cubestor name "+name)
			}
			return []byte(strings.Join(lines, "\n")), nil
		case "attach":
			name := args[len(args)-1]
			for _, dir := range []string{localDir, sharedDir} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte("image"), 0o600); err != nil {
					return nil, err
				}
			}
			attached[name] = true
			return []byte("name " + name + "\ndevice " + device + "\nsize 8388608\n"), nil
		case "size":
			return []byte("8388608\n"), nil
		default:
			return []byte("ok\n"), nil
		}
	}
	return store, &calls
}

func TestHybridResolveDevPathReusesAttachedDevice(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)

	first, err := store.ResolveDevPath(context.Background(), "demo", cowKindVolume)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.ResolveDevPath(context.Background(), "demo", cowKindSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("device changed: first=%q second=%q", first, second)
	}
	attaches := 0
	for _, call := range *calls {
		if strings.HasPrefix(call, "attach ") {
			attaches++
		}
	}
	if attaches != 1 {
		t.Fatalf("attach calls=%d, want 1; calls=%v", attaches, *calls)
	}
}

func TestHybridResolveDevPathConcurrentConnectsOnce(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)

	const workers = 16
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			device, err := store.ResolveDevPath(context.Background(), "concurrent", cowKindVolume)
			if err == nil && device != "/dev/nbd7" {
				err = fmt.Errorf("device=%q, want /dev/nbd7", device)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	attaches := 0
	for _, call := range *calls {
		if strings.HasPrefix(call, "attach ") {
			attaches++
		}
	}
	if attaches != 1 {
		t.Fatalf("attach calls=%d, want 1; calls=%v", attaches, *calls)
	}
}

func TestHybridCreateMemoryVolumeUsesCubestor(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)

	volume, err := store.CreateMemoryVolume(context.Background(), "template-a", 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	if volume.VolumeName != "tpl-template-a-memory" || volume.SizeBytes != 8<<20 {
		t.Fatalf("unexpected volume: %+v", volume)
	}
	if len(*calls) < 3 ||
		!strings.Contains((*calls)[0], "create --size 8388608") ||
		!strings.HasPrefix((*calls)[1], "devices") ||
		!strings.HasPrefix((*calls)[2], "attach ") {
		t.Fatalf("unexpected calls: %v", *calls)
	}
}

func TestHybridCreateAlignsSizeToOverlayBlock(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)

	volume, err := store.CreateMemoryVolume(context.Background(), "unaligned", 4097)
	if err != nil {
		t.Fatal(err)
	}
	if volume.SizeBytes != 8192 {
		t.Fatalf("size=%d, want 8192", volume.SizeBytes)
	}
	if !strings.Contains((*calls)[0], "create --size 8192") {
		t.Fatalf("create was not aligned: calls=%v", *calls)
	}
}

func TestHybridCreateRefusesOrphanLocalObject(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	name := "tpl-orphan-memory"
	if err := os.WriteFile(filepath.Join(store.localDir, name), []byte("dirty"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := store.create(context.Background(), name, 4096, cowKindVolume)
	if !errors.Is(err, ErrCowObjectAlreadyExists) {
		t.Fatalf("create error=%v, want ErrCowObjectAlreadyExists", err)
	}
	if len(*calls) != 0 {
		t.Fatalf("cubestor called despite local collision: %v", *calls)
	}
}

func TestHybridCreateCleansObjectWhenConnectResultIsMalformed(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	name := "tpl-bad-attach-memory"
	attached := false
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		switch args[0] {
		case "create":
			if err := os.WriteFile(filepath.Join(store.sharedDir, name), []byte("shared"), 0o600); err != nil {
				return nil, err
			}
		case "attach":
			attached = true
			return []byte("name " + name + "\n"), nil
		case "devices":
			if attached {
				return []byte("nbd7 size 8192 plugin cubestor name " + name), nil
			}
		case "detach":
			attached = false
		}
		return []byte("ok\n"), nil
	}

	_, err := store.create(context.Background(), name, 4096, cowKindVolume)
	if err == nil || !strings.Contains(err.Error(), "returned no device") {
		t.Fatalf("create error=%v, want malformed attach error", err)
	}
	joined := strings.Join(*calls, "\n")
	if !strings.Contains(joined, "detach --device /dev/nbd7") ||
		!strings.Contains(joined, "delete --dir "+store.sharedDir+" --name "+name) {
		t.Fatalf("create did not clean malformed attachment:\n%s", joined)
	}
}

func TestHybridCloneFlushesSourceAndReturnsSize(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	source := "tpl-source-rootfs"
	if err := os.WriteFile(filepath.Join(store.localDir, source), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store.sharedDir, source), []byte("shared"), 0o600); err != nil {
		t.Fatal(err)
	}
	device := "/dev/nbd7"
	if _, err := store.ResolveDevPath(context.Background(), source, cowKindVolume); err != nil {
		t.Fatal(err)
	}

	volume, err := store.clone(context.Background(), source, "tpl-dest-rootfs", cowKindSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if volume.SizeBytes != 8<<20 {
		t.Fatalf("size=%d, want %d", volume.SizeBytes, 8<<20)
	}
	joined := strings.Join(*calls, "\n")
	if !strings.Contains(joined, "flush --device "+device) ||
		!strings.Contains(joined, "clone --local-dir "+store.localDir) ||
		!strings.Contains(joined, "size --dir "+store.localDir+" --name tpl-dest-rootfs") {
		t.Fatalf("unexpected calls:\n%s", joined)
	}
}

// The local overlay carries the same virtual_size as the shared image, and
// reading it avoids a JuiceFS round trip (measured 80ms per size call).
func TestHybridGetSizeBytesPrefersLocalOverlay(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	name := "tpl-size-memory"
	if err := os.WriteFile(filepath.Join(store.localDir, name), []byte("image"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store.sharedDir, name), []byte("image"), 0o600); err != nil {
		t.Fatal(err)
	}
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		return []byte("8192\n"), nil
	}

	size, err := store.GetSizeBytes(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	if size != 8192 {
		t.Fatalf("size=%d, want 8192", size)
	}
	joined := strings.Join(*calls, "\n")
	if !strings.Contains(joined, "size --dir "+store.localDir) {
		t.Fatalf("size must read the local overlay:\n%s", joined)
	}
	if strings.Contains(joined, "size --dir "+store.sharedDir) {
		t.Fatalf("size must not touch the shared image when local exists:\n%s", joined)
	}
}

// An object published on another node has no local overlay here, so the shared
// image is the only place left to ask.
func TestHybridGetSizeBytesFallsBackToShared(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	name := "tpl-remote-only-memory"
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		dir := ""
		for i, a := range args {
			if a == "--dir" && i+1 < len(args) {
				dir = args[i+1]
			}
		}
		if dir == store.localDir {
			return []byte("cubestorcli: open " + store.localDir + "/" + name +
				": No such file or directory"), errors.New("exit status 1")
		}
		return []byte("4096\n"), nil
	}

	size, err := store.GetSizeBytes(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	if size != 4096 {
		t.Fatalf("size=%d, want 4096", size)
	}
	joined := strings.Join(*calls, "\n")
	if !strings.Contains(joined, "size --dir "+store.sharedDir) {
		t.Fatalf("size must fall back to the shared image:\n%s", joined)
	}
}

// Missing in both places is the only case that means "the object is gone".
func TestHybridGetSizeBytesReportsMissingWhenBothSidesLackIt(t *testing.T) {
	t.Parallel()
	store, _ := newFakeHybridStore(t)
	name := "tpl-absent-memory"
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		return []byte("cubestorcli: open /x/" + name + ": No such file or directory"),
			errors.New("exit status 1")
	}

	_, err := store.GetSizeBytes(context.Background(), name)
	if !errors.Is(err, ErrCowObjectMissing) {
		t.Fatalf("error=%v, want ErrCowObjectMissing", err)
	}
}

func TestHybridCloneStopsWhenDeviceDiscoveryFails(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	source := "tpl-source-rootfs"
	for _, dir := range []string{store.localDir, store.sharedDir} {
		if err := os.WriteFile(filepath.Join(dir, source), []byte("image"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		if args[0] == "devices" {
			return nil, errors.New("device enumeration failed")
		}
		return []byte("ok\n"), nil
	}

	_, err := store.clone(context.Background(), source, "tpl-dest-rootfs", cowKindSnapshot)
	if err == nil || !strings.Contains(err.Error(), "device enumeration failed") {
		t.Fatalf("clone error=%v, want device discovery failure", err)
	}
	for _, call := range *calls {
		if strings.HasPrefix(call, "clone ") {
			t.Fatalf("clone continued after discovery failure: %v", *calls)
		}
	}
}

func TestHybridCloneCleansDestinationWhenInfoFails(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	source, destination := "tpl-source-rootfs", "tpl-dest-rootfs"
	for _, dir := range []string{store.localDir, store.sharedDir} {
		if err := os.WriteFile(filepath.Join(dir, source), []byte("image"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		switch args[0] {
		case "devices":
			return nil, nil
		case "clone":
			for _, dir := range []string{store.localDir, store.sharedDir} {
				if err := os.WriteFile(filepath.Join(dir, destination), []byte("clone"), 0o600); err != nil {
					return nil, err
				}
			}
		case "size":
			return nil, errors.New("size failed")
		}
		return []byte("ok\n"), nil
	}

	_, err := store.clone(context.Background(), source, destination, cowKindSnapshot)
	if err == nil || !strings.Contains(err.Error(), "size failed") {
		t.Fatalf("clone error=%v, want size failure", err)
	}
	joined := strings.Join(*calls, "\n")
	if !strings.Contains(joined, "delete --dir "+store.localDir+" --dir "+store.sharedDir+" --name "+destination) {
		t.Fatalf("destination was not cleaned:\n%s", joined)
	}
}

func TestNewHybridStoreRequiresOptIn(t *testing.T) {
	if _, err := newHybridStore(&Config{}); !errorsIs(err, ErrHybridNotConfigured) {
		t.Fatalf("newHybridStore error=%v, want ErrHybridNotConfigured", err)
	}
}

func TestNewHybridStoreDefaultsToHybridLocalAndMountedRemote(t *testing.T) {
	root := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfg := &Config{DataPath: root}
	cfg.Cow.Hybrid.Enable = true
	cfg.Cow.Hybrid.CubestorCLIPath = executable
	cfg.Cow.JFS.MountPath = filepath.Join(root, "jfs")

	previous := checkJFSMount
	t.Cleanup(func() { checkJFSMount = previous })
	var checked string
	checkJFSMount = func(path string) error {
		checked = path
		return nil
	}

	store, err := newHybridStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if store.localDir != filepath.Join(root, "hybrid", "local") {
		t.Fatalf("localDir=%q", store.localDir)
	}
	if store.sharedDir != filepath.Join(root, "hybrid", "remote") {
		t.Fatalf("sharedDir=%q", store.sharedDir)
	}
	if checked != store.sharedDir {
		t.Fatalf("checked mount=%q, want %q", checked, store.sharedDir)
	}
}

func TestStoreForBackendHybrid(t *testing.T) {
	store, _ := newFakeHybridStore(t)
	previous := localStorage
	localStorage = &local{
		config:        &Config{StorageBackend: StorageBackendCow, Cow: CowInlineConfig{Hybrid: CowHybridUserConfig{Enable: true}}},
		hybridManager: store,
	}
	t.Cleanup(func() { localStorage = previous })

	got, err := StoreFor("cubestor")
	if err != nil {
		t.Fatal(err)
	}
	if got != store {
		t.Fatalf("StoreFor(cubestor)=%T, want injected HybridStore", got)
	}
}

func TestHybridIgnoresDeviceFromAnotherPlugin(t *testing.T) {
	store, calls := newFakeHybridStore(t)
	device := "/dev/nbd7"
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		if args[0] == "devices" {
			return []byte("nbd7 size 8388608 plugin file name demo\n"), nil
		}
		if args[0] == "attach" {
			for _, dir := range []string{store.localDir, store.sharedDir} {
				if err := os.WriteFile(filepath.Join(dir, "demo"), []byte("image"), 0o600); err != nil {
					return nil, err
				}
			}
			return []byte("name demo\ndevice " + device + "\nsize 8388608\n"), nil
		}
		return nil, nil
	}
	got, err := store.ResolveDevPath(context.Background(), "demo", cowKindVolume)
	if err != nil {
		t.Fatal(err)
	}
	if got != device {
		t.Fatalf("device=%q, want %q", got, device)
	}
	if !strings.HasPrefix((*calls)[1], "attach ") {
		t.Fatalf("foreign plugin device was reused: calls=%v", *calls)
	}
}

func TestHybridRefusesOverlayDeviceWithoutConfiguredObjectPair(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		if args[0] == "devices" {
			return []byte("nbd9 size 8388608 plugin cubestor name foreign"), nil
		}
		return nil, nil
	}

	_, err := store.ResolveDevPath(context.Background(), "foreign", cowKindVolume)
	if err == nil || !strings.Contains(err.Error(), "refuse unowned hybrid device") {
		t.Fatalf("ResolveDevPath error=%v, want ownership rejection", err)
	}
	if len(*calls) != 1 || (*calls)[0] != "devices" {
		t.Fatalf("unexpected calls for foreign device: %v", *calls)
	}
}

func TestHybridPublishesCloneNotLiveSource(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	source := "tpl-tpl-x-build-rootfs"
	destination := "tpl-tpl-x-rootfs"
	for _, dir := range []string{store.localDir, store.sharedDir} {
		for _, name := range []string{source, destination} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte("image"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	// The source is a live export: publish must clone it and never detach it,
	// because the temporary build sandbox may still be running against it.
	// Only the freshly created clone is synced.
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		switch args[0] {
		case "devices":
			return []byte("nbd0 size 8192 plugin cubestor name " + source), nil
		case "size":
			return []byte("8192\n"), nil
		}
		return []byte("ok\n"), nil
	}

	if _, err := store.CommitTemplateRootfs(context.Background(), source, "tpl-x"); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(*calls, "\n")
	if strings.Contains(joined, "detach") {
		t.Fatalf("publish detached a live source export:\n%s", joined)
	}
	cloneAt := strings.Index(joined, "clone --local-dir "+store.localDir)
	if cloneAt < 0 {
		t.Fatalf("commit did not clone the build rootfs:\n%s", joined)
	}
	syncAt := strings.Index(joined, "sync --local-dir "+store.localDir)
	if syncAt < 0 || syncAt < cloneAt {
		t.Fatalf("publish must sync the clone after cloning:\n%s", joined)
	}
	if !strings.Contains(joined[syncAt:], "--name "+destination) {
		t.Fatalf("publish synced the wrong object:\n%s", joined)
	}
}

func TestHybridCommitTemplateMemoryLeavesLiveSourceAttached(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	source := "sb-demo-memory"
	destination := "tpl-tpl-x-memory"
	for _, dir := range []string{store.localDir, store.sharedDir} {
		for _, name := range []string{source, destination} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte("image"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		switch args[0] {
		case "devices":
			return []byte("nbd2 size 8192 plugin cubestor name " + source), nil
		case "attach":
			return []byte("name " + destination + "\ndevice /dev/nbd3\nsize 8192\n"), nil
		case "size":
			return []byte("8192\n"), nil
		}
		return []byte("ok\n"), nil
	}

	if _, err := store.CommitTemplateMemory(context.Background(), source, "tpl-x", 8192); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(*calls, "\n")
	for _, forbidden := range []string{"detach", "sync "} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("memory commit must not %q the running source:\n%s", forbidden, joined)
		}
	}
}

// A template's memory image must reach the shared store before the template is
// announced READY. Without this the data lived only in this node's local XFS
// overlay (shared block_count was 0), so a peer cloning the template read an
// empty image and a restart on a node without that overlay booted zeroed memory.
func TestHybridPublishTemplateMemoryMergesIntoShared(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	name := "tpl-tpl-x-memory"
	for _, dir := range []string{store.localDir, store.sharedDir} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("image"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		switch args[0] {
		case "devices":
			return nil, nil // the template path detaches before publishing
		case "info":
			return []byte("flags clean\ngeneration 1\n"), nil
		}
		return []byte("ok\n"), nil
	}

	if err := store.PublishTemplateMemory(context.Background(), name); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(*calls, "\n")
	if !strings.Contains(joined, "sync --local-dir "+store.localDir+" --shared-dir "+store.sharedDir+" --name "+name) {
		t.Fatalf("template memory was not published to the shared store:\n%s", joined)
	}
	for _, forbidden := range []string{"detach", "attach "} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("publishing template memory must not %q:\n%s", forbidden, joined)
		}
	}
}

// Publishing a template memory image while an export still holds it must fail
// loudly. Template construction has no retry loop, so silently skipping would
// ship a template only this node can use.
func TestHybridPublishTemplateMemoryFailsWhenStillAttached(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	name := "tpl-tpl-busy-memory"
	for _, dir := range []string{store.localDir, store.sharedDir} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("image"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		if args[0] == "devices" {
			return []byte("nbd5 size 8192 plugin cubestor name " + name), nil
		}
		return []byte("ok\n"), nil
	}

	err := store.PublishTemplateMemory(context.Background(), name)
	if !errors.Is(err, errHybridPublishBusy) {
		t.Fatalf("error=%v, want busy", err)
	}
	if strings.Contains(strings.Join(*calls, "\n"), "sync ") {
		t.Fatalf("a busy template memory must not be synced: %v", *calls)
	}
}

// Deactivation must NOT publish inline: the merge is a whole-overlay copy that
// made snapshot latency 64% worse (1960ms of 3042ms) and it has to take an
// exclusive lock, so it belongs to the background publisher. This test pins
// that deactivate only detaches: detach carries the flush itself.
func TestHybridDeactivateDoesNotPublishInline(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	name := "tpl-tpl-x-memory"
	for _, dir := range []string{store.localDir, store.sharedDir} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("image"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store.ownedDevice = map[string]string{name: "/dev/nbd2"}
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		if args[0] == "devices" {
			return []byte("nbd2 size 8192 plugin cubestor name " + name), nil
		}
		return []byte("ok\n"), nil
	}

	if err := store.DeactivateByKind(context.Background(), name, cowKindVolume); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(*calls, "\n")
	if strings.Contains(joined, "sync ") {
		t.Fatalf("deactivate must not sync inline; the publisher owns that:\n%s", joined)
	}
	if !strings.Contains(joined, "detach --device /dev/nbd2") {
		t.Fatalf("deactivate must detach:\n%s", joined)
	}
	if !strings.Contains(joined, "flush --device /dev/nbd2") {
		t.Fatalf("deactivate must flush before detach; detach does not flush:\n%s", joined)
	}
}

func TestHybridBeginSnapshotDeactivateSealsAndRetainsDevice(t *testing.T) {
	store, calls := newFakeHybridStore(t)
	name := "tpl-snap-x-memory"
	for _, dir := range []string{store.localDir, store.sharedDir} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("image"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	store.readDevice = func(string) (hybridDeviceIdentity, error) {
		return hybridDeviceIdentity{PID: "123", Start: "456", Socket: store.socketPath, Name: name}, nil
	}
	store.rememberDevice(name, "/dev/nbd2")
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		if args[0] == "devices" {
			return []byte("nbd2 size 8192 plugin cubestor name " + name), nil
		}
		return []byte("ok\n"), nil
	}
	if err := store.BeginSnapshotDeactivate(context.Background(), name, cowKindSnapshot); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(*calls, "\n")
	if !strings.Contains(joined, "flush --to-page-cache --unix "+store.socketPath+" --device /dev/nbd2 --name "+name) {
		t.Fatalf("missing synchronous seal: %s", joined)
	}
	if !store.ownsDevice(name) || !store.isSealed(name) {
		t.Fatal("finalization must retain the sealed device")
	}
	*calls = nil
	if err := store.BeginSnapshotDeactivate(context.Background(), name, cowKindSnapshot); err != nil {
		t.Fatal(err)
	}
	if dev, err := store.ResolveSnapshotMemory(context.Background(), name, cowKindSnapshot); err != nil || dev != "/dev/nbd2" {
		t.Fatalf("restore did not reuse device: %s %v", dev, err)
	}
	if len(*calls) != 0 {
		t.Fatalf("restore performed persistence or preparation: %v", *calls)
	}
	if err := store.publishVolume(context.Background(), name); err != nil {
		t.Fatal(err)
	}
	for _, call := range *calls {
		if !strings.HasPrefix(call, "sync --immutable ") && !strings.HasPrefix(call, "flush --sync-page-cache ") {
			t.Fatalf("finalized capture repeated preparation: %s", call)
		}
	}
}

// publishVolume is what the background publisher runs: merge local into shared.
// It must NOT detach: the same object name is what a restore attaches right
// before the shim mmaps it, so detaching whatever findDevice returns destroyed a
// live restore (GuestMemoryRegion(MappingPastEof) on the freshly opened device).
// It must NOT re-attach either: an attached export after the snapshot RPC
// returns races delete/GC and surfaces as "cubestorcli delete ... is in use".
func TestHybridPublishVolumeNeverDetachesOrReattaches(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	name := "tpl-snap-x-memory"
	for _, dir := range []string{store.localDir, store.sharedDir} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("image"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		switch args[0] {
		case "devices":
			return nil, nil // nothing attached: the snapshot path already detached
		case "info":
			return []byte("flags clean\ngeneration 1\n"), nil
		}
		return []byte("ok\n"), nil
	}

	if err := store.publishVolume(context.Background(), name); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(*calls, "\n")
	if !strings.Contains(joined, "sync --immutable --local-dir "+store.localDir) {
		t.Fatalf("publish must sync:\n%s", joined)
	}
	for _, forbidden := range []string{"detach", "attach "} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("publish must not %q:\n%s", forbidden, joined)
		}
	}
}

// Attached sealed snapshots can publish without inspecting or touching devices.
func TestHybridPublishVolumeAllowsAttachedSnapshot(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	name := "tpl-snap-busy-memory"
	for _, dir := range []string{store.localDir, store.sharedDir} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("image"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		if args[0] == "devices" {
			return []byte("nbd5 size 8192 plugin cubestor name " + name), nil
		}
		return []byte("ok\n"), nil
	}

	err := store.publishVolume(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(*calls, "\n")
	for _, forbidden := range []string{"detach", "devices"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("immutable publish must not %q:\n%s", forbidden, joined)
		}
	}
}

// cubestorcli sync dies with "<path> is in use" when another export holds the
// lock. That has to be recognised as busy rather than as a broken package.
func TestHybridPublishVolumeTreatsLockContentionAsBusy(t *testing.T) {
	t.Parallel()
	store, _ := newFakeHybridStore(t)
	name := "tpl-snap-contended-memory"
	for _, dir := range []string{store.localDir, store.sharedDir} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("image"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		switch args[0] {
		case "info":
			return []byte("flags clean\ngeneration 1\n"), nil
		case "sync":
			return []byte("cubestorcli: " + store.localDir + "/" + name + " is in use"), errors.New("exit status 1")
		}
		return []byte("ok\n"), nil
	}

	err := store.publishVolume(context.Background(), name)
	if !errors.Is(err, errHybridPublishBusy) {
		t.Fatalf("publish error=%v, want busy", err)
	}
}

func TestHybridPublishVolumeFencesSameNameThroughSync(t *testing.T) {
	store, _ := newFakeHybridStore(t)
	name := "tpl-snap-fenced-memory"
	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		switch args[0] {
		case "devices":
			return nil, nil
		case "info":
			return []byte("flags clean\ngeneration 1\n"), nil
		case "sync":
			close(syncStarted)
			<-releaseSync
		}
		return []byte("ok\n"), nil
	}

	published := make(chan error, 1)
	go func() { published <- store.publishVolume(context.Background(), name) }()
	select {
	case <-syncStarted:
	case <-time.After(time.Second):
		t.Fatal("publisher did not reach sync")
	}

	lockAcquired := make(chan struct{})
	go func() {
		unlock := store.nameLocks.lock(name)
		close(lockAcquired)
		unlock()
	}()
	select {
	case <-lockAcquired:
		t.Fatal("same-name operation entered while publish sync was running")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseSync)
	if err := <-published; err != nil {
		t.Fatal(err)
	}
	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		t.Fatal("same-name operation remained fenced after publish completed")
	}
}

func TestHybridDeactivateSkipsPublishForSandboxVolumes(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	name := "sb-demo-rootfs-gen0"
	for _, dir := range []string{store.localDir, store.sharedDir} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("image"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store.ownedDevice = map[string]string{name: "/dev/nbd3"}
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		if args[0] == "devices" {
			return []byte("nbd3 size 8192 plugin cubestor name " + name), nil
		}
		return []byte("ok\n"), nil
	}

	if err := store.DeactivateByKind(context.Background(), name, cowKindSnapshot); err != nil {
		t.Fatal(err)
	}
	for _, call := range *calls {
		if strings.HasPrefix(call, "sync ") {
			t.Fatalf("sandbox volume must not be synced on deactivate: %v", *calls)
		}
	}
}

func TestHybridConcurrentCreateIsNotSerialized(t *testing.T) {
	t.Parallel()
	store, _ := newFakeHybridStore(t)
	// Each cubestorcli create blocks until released, so the test can prove that
	// unrelated names make progress at the same time. With the previous
	// store-wide mutex the second create could not even start, which is what
	// made template start (create rootfs + memory per sandbox) scale linearly
	// worse with concurrency.
	const concurrent = 4
	started := make(chan string, concurrent)
	release := make(chan struct{})
	var once sync.Once

	store.run = func(ctx context.Context, _ string, args ...string) ([]byte, error) {
		switch args[0] {
		case "create":
			name := args[len(args)-1]
			started <- name
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			for _, dir := range []string{store.localDir, store.sharedDir} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte("image"), 0o600); err != nil {
					return nil, err
				}
			}
		case "attach":
			name := args[len(args)-1]
			return []byte("name " + name + "\ndevice /dev/nbd7\nsize 8192\n"), nil
		case "devices":
			return nil, nil
		}
		return []byte("ok\n"), nil
	}

	var wg sync.WaitGroup
	for i := 0; i < concurrent; i++ {
		name := fmt.Sprintf("tpl-concurrent-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.create(context.Background(), name, 8192, cowKindVolume); err != nil {
				t.Errorf("create %s: %v", name, err)
			}
		}()
	}

	for i := 0; i < concurrent; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			once.Do(func() { close(release) })
			wg.Wait()
			t.Fatalf("only %d/%d creates started concurrently; creates are serialized", i, concurrent)
		}
	}
	once.Do(func() { close(release) })
	wg.Wait()
}

func TestHybridConcurrentSameNameCreateOnlyOneWins(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	name := "tpl-same-name"
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		switch args[0] {
		case "create":
			for _, dir := range []string{store.localDir, store.sharedDir} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte("image"), 0o600); err != nil {
					return nil, err
				}
			}
		case "attach":
			return []byte("name " + name + "\ndevice /dev/nbd7\nsize 8192\n"), nil
		case "devices":
			return nil, nil
		}
		return []byte("ok\n"), nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	created, existed := 0, 0
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.create(context.Background(), name, 8192, cowKindVolume)
			mu.Lock()
			switch {
			case err == nil:
				created++
			case errors.Is(err, ErrCowObjectAlreadyExists):
				existed++
			default:
				t.Errorf("unexpected create error: %v", err)
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if created != 1 || existed != 3 {
		t.Fatalf("created=%d existed=%d, want exactly one winner", created, existed)
	}
}

// Publishing must only ever touch the snapshot's own volumes. The volumes a
// running VM depends on -- its sb-<id>-rootfs-genN and the template's
// tpl-<id>-memory -- must never be detached by the publisher, or the VM loses
// its backing device while it is still running.
func TestHybridPublishTargetsOnlySnapshotOwnedVolumes(t *testing.T) {
	t.Parallel()
	store, _ := newFakeHybridStore(t)
	if err := WriteSnapshotCatalogFor(cow.BackendHybrid, &SnapshotCatalogEntry{
		SnapshotID:   "snap-owned",
		SnapshotPath: filepath.Join(store.sharedDir, "snap-owned"),
		RootfsVol:    "tpl-snap-owned-rootfs",
		RootfsKind:   cowKindSnapshot,
		MemoryVol:    "tpl-snap-owned-memory",
		MemoryKind:   cowKindSnapshot,
	}); err != nil {
		t.Fatal(err)
	}

	names, err := hybridPackageObjectNames(context.Background(), "snap-owned")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"tpl-snap-owned-rootfs", "tpl-snap-owned-memory"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("package volumes=%v, want %v", names, want)
	}
	for _, name := range names {
		if strings.HasPrefix(name, "sb-") {
			t.Fatalf("publisher must never receive a live sandbox volume: %s", name)
		}
	}
}

func TestHybridCloseDetachesOnlyOwnedDevices(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	store.readDevice = func(device string) (hybridDeviceIdentity, error) {
		for name, dev := range store.ownedDevice {
			if dev == device {
				return hybridDeviceIdentity{PID: "100", Start: "1", Name: name, Socket: store.socketPath}, nil
			}
		}
		return hybridDeviceIdentity{}, os.ErrNotExist
	}
	store.ownedDevice = map[string]string{"owned": "/dev/nbd7"}

	if err := store.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(*calls, "\n")
	if strings.Contains(joined, "devices") {
		t.Fatalf("Close enumerated foreign devices:\n%s", joined)
	}
	if !strings.Contains(joined, "detach --device /dev/nbd7") ||
		!strings.Contains(joined, "flush --device /dev/nbd7") {
		t.Fatalf("owned device was not flushed and closed:\n%s", joined)
	}
	if len(store.ownedDevice) != 0 {
		t.Fatalf("owned mappings remain after Close: %v", store.ownedDevice)
	}
}

func TestHybridCloseContinuesAfterOneDetachFailure(t *testing.T) {
	t.Parallel()
	store, calls := newFakeHybridStore(t)
	store.readDevice = func(device string) (hybridDeviceIdentity, error) {
		for name, dev := range store.ownedDevice {
			if dev == device {
				return hybridDeviceIdentity{PID: "100", Start: "1", Name: name, Socket: store.socketPath}, nil
			}
		}
		return hybridDeviceIdentity{}, os.ErrNotExist
	}
	store.ownedDevice = map[string]string{
		"bad":  "/dev/nbd7",
		"good": "/dev/nbd8",
	}
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		if args[0] == "detach" && args[2] == "/dev/nbd7" {
			return nil, errors.New("detach failed")
		}
		return []byte("ok\n"), nil
	}

	err := store.Close(context.Background())
	if err == nil || !strings.Contains(err.Error(), "detach failed") {
		t.Fatalf("Close error=%v, want detach failure", err)
	}
	joined := strings.Join(*calls, "\n")
	if !strings.Contains(joined, "detach --device /dev/nbd8") {
		t.Fatalf("Close did not continue with healthy device:\n%s", joined)
	}
	if _, ok := store.ownedDevice["good"]; ok {
		t.Fatalf("successfully closed device remained owned: %v", store.ownedDevice)
	}
	if _, ok := store.ownedDevice["bad"]; !ok {
		t.Fatalf("failed device ownership was lost: %v", store.ownedDevice)
	}
}

func TestLocalCloseClearsHybridManagerOnError(t *testing.T) {
	t.Parallel()
	store, _ := newFakeHybridStore(t)
	store.readDevice = func(device string) (hybridDeviceIdentity, error) {
		for name, dev := range store.ownedDevice {
			if dev == device {
				return hybridDeviceIdentity{PID: "100", Start: "1", Name: name, Socket: store.socketPath}, nil
			}
		}
		return hybridDeviceIdentity{}, os.ErrNotExist
	}
	store.ownedDevice = map[string]string{"bad": "/dev/nbd7"}
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if args[0] == "detach" {
			return nil, errors.New("detach failed")
		}
		return []byte("ok\n"), nil
	}
	l := &local{hybridManager: store}

	err := l.Close()
	if err == nil || !strings.Contains(err.Error(), "detach failed") {
		t.Fatalf("Close error=%v, want detach failure", err)
	}
	if l.hybridManager != nil {
		t.Fatal("hybrid manager was not cleared")
	}
}

func TestHybridMetricsContainCommonKeys(t *testing.T) {
	t.Parallel()
	store, _ := newFakeHybridStore(t)
	if err := os.WriteFile(filepath.Join(store.localDir, "local"), []byte("1234"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store.sharedDir, "shared"), []byte("12345678"), 0o600); err != nil {
		t.Fatal(err)
	}

	metrics, err := store.GetMetrics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range cowMetricKeys {
		if _, ok := metrics[key]; !ok {
			t.Fatalf("missing metric %q: %v", key, metrics)
		}
	}
	if metrics["used_bytes"] != 12 || metrics["volume_count"] != 2 {
		t.Fatalf("unexpected metrics: %v", metrics)
	}
}

func TestInspectHybridMissingObject(t *testing.T) {
	store, _ := newFakeHybridStore(t)
	store.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if args[0] == "size" {
			return []byte("cubestorcli: No such file or directory"), errors.New("exit status 1")
		}
		return nil, nil
	}
	previous := localStorage
	localStorage = &local{
		config:        &Config{StorageBackend: StorageBackendCow, Cow: CowInlineConfig{Hybrid: CowHybridUserConfig{Enable: true}}},
		hybridManager: store,
	}
	t.Cleanup(func() { localStorage = previous })

	statuses, err := InspectObjectsFor(context.Background(), cow.BackendHybrid, []CowObjectRef{{
		Name: "missing", Kind: cowKindVolume,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(statuses) != 1 || statuses[0].Exists {
		t.Fatalf("unexpected statuses: %+v", statuses)
	}
}

// Hybrid device paths are transient: /dev/nbdN gets reused, so a recorded path
// can later belong to a different object. refreshCowPaths must therefore drop
// them and let the live request resolve only what it needs, rather than
// re-attaching every recorded volume (which would exhaust the device pool).
func TestRefreshCowPathsClearsHybridDevicePaths(t *testing.T) {
	store, calls := newFakeHybridStore(t)
	l := &local{
		config: &Config{
			StorageBackend: StorageBackendCow,
			Cow:            CowInlineConfig{Hybrid: CowHybridUserConfig{Enable: true}},
		},
		hybridManager: store,
	}
	info := &StorageInfo{
		Backend: cow.BackendHybrid,
		Volumes: map[string]*BackendFileInfo{
			"root": {
				VolumeName: "sb-restart-rootfs-gen0",
				Kind:       cowKindSnapshot,
				FilePath:   "/dev/nbd7",
			},
		},
	}

	if err := l.refreshCowPaths(info); err != nil {
		t.Fatal(err)
	}
	if got := info.Volumes["root"].FilePath; got != "" {
		t.Fatalf("stale device path=%q, want it cleared", got)
	}
	if len(*calls) != 0 {
		t.Fatalf("refresh must not touch the device pool: %v", *calls)
	}
}

func errorsIs(err, target error) bool {
	return errors.Is(err, target)
}

func TestHybridCloneReturnsSizeAndSkipsFreshDeviceDiscovery(t *testing.T) {
	store, calls := newFakeHybridStore(t)
	ctx := context.Background()
	if _, err := store.ResolveDevPath(ctx, "source", cowKindVolume); err != nil {
		t.Fatal(err)
	}
	oldRun := store.run
	store.run = func(ctx context.Context, path string, args ...string) ([]byte, error) {
		if args[0] == "clone" {
			return []byte("cloned source -> destination\nsize 8388608\n"), nil
		}
		if args[0] == "size" {
			t.Fatal("clone must reuse returned size")
		}
		return oldRun(ctx, path, args...)
	}
	volume, err := store.clone(ctx, "source", "destination", cowKindSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if volume.SizeBytes != 8388608 {
		t.Fatalf("size=%d", volume.SizeBytes)
	}
	before := len(*calls)
	if err := store.DeactivateByKind(ctx, "destination", cowKindSnapshot); err != nil {
		t.Fatal(err)
	}
	if len(*calls) != before {
		t.Fatalf("fresh unattached object invoked CLI: %v", (*calls)[before:])
	}
	if _, err := store.ResolveDevPath(ctx, "destination", cowKindSnapshot); err != nil {
		t.Fatal(err)
	}
	for _, call := range (*calls)[before:] {
		if call == "devices" {
			t.Fatal("fresh attach enumerated devices")
		}
	}
	store.devMu.Lock()
	fresh := store.unattached["destination"]
	store.devMu.Unlock()
	if fresh {
		t.Fatal("attached object retained unattached state")
	}
}

func TestHybridPageCachePersistenceFailurePreventsUpload(t *testing.T) {
	s, calls := newFakeHybridStore(t)
	name := "tpl-snap-pending-memory"
	s.rememberSealed(name)
	s.pendingPersist = map[string]bool{name: true}
	base := s.run
	fail := true
	s.run = func(ctx context.Context, path string, args ...string) ([]byte, error) {
		if args[0] == "flush" && fail {
			return nil, errors.New("injected persistence failure")
		}
		return base(ctx, path, args...)
	}
	if err := s.publishVolume(context.Background(), name); err == nil {
		t.Fatal("expected persistence failure")
	}
	if len(*calls) != 0 || !s.pendingPersist[name] {
		t.Fatal("failed persistence uploaded or forgot pending state")
	}
	fail = false
	if err := s.publishVolume(context.Background(), name); err != nil {
		t.Fatal(err)
	}
	if len(*calls) != 2 || !strings.HasPrefix((*calls)[0], "flush --sync-page-cache ") || !strings.HasPrefix((*calls)[1], "sync --immutable ") {
		t.Fatalf("wrong publish order: %v", *calls)
	}
	if s.pendingPersist[name] {
		t.Fatal("durable snapshot remained pending")
	}
}
