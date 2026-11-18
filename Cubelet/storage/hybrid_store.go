// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

const (
	defaultHybridCubestorCLIPath = "cubestorcli"
	defaultHybridSocketPath      = "/run/cubestor.sock"
	defaultHybridTimeout         = 30 * time.Second
)

var (
	ErrHybridNotConfigured = errors.New("hybrid storage is not configured")
	ErrHybridNotReady      = errors.New("hybrid storage is not ready")
)

type hybridCommandRunner func(context.Context, string, ...string) ([]byte, error)

// HybridStore adapts cubestor's persistent local overlay to Cube's CoW Store
// contract. cubestor owns the on-disk format and kernel NBD lifecycle; Cube
// stores only stable export names and resolves transient /dev/nbdX paths.
//
// Locking: there is deliberately no store-wide lock around the cubestorcli
// invocations. create/clone are subprocess calls whose duration grows with the
// volume size (and on JuiceFS they used to grow with the page-map entry count),
// so a single mutex serialized every concurrent sandbox creation and made
// template start scale linearly worse with concurrency. Instead:
//
//   - devMu guards ownedDevice only; every critical section is a map read or
//     write.
//   - nameLocks serializes the attach/detach lifecycle of ONE object, which is
//     what actually needs ordering: two attaches for the same export would
//     otherwise race on the same image. Unrelated objects run in parallel.
//
// Device allocation itself needs no lock here: cubestorcli takes an exclusive
// flock on /dev/nbdN and the NBD worker inherits it, so the kernel arbitrates
// which device a concurrent attach gets.
type HybridStore struct {
	cubestorCLIPath string
	socketPath      string
	localDir        string
	sharedDir       string
	timeout         time.Duration
	run             hybridCommandRunner

	devMu       sync.Mutex
	ownedDevice map[string]string
	nameLocks   keyedMutex
}

// keyedMutex serializes work per key (one CoW object name) without blocking
// unrelated keys. The entry is dropped when its last holder releases, so a
// long-lived Cubelet does not accumulate one mutex per volume ever seen.
type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*keyedMutexEntry
}

type keyedMutexEntry struct {
	mu   sync.Mutex
	refs int
}

// lock acquires the mutex for name and returns its release function.
func (k *keyedMutex) lock(name string) func() {
	k.mu.Lock()
	if k.locks == nil {
		k.locks = make(map[string]*keyedMutexEntry)
	}
	entry := k.locks[name]
	if entry == nil {
		entry = &keyedMutexEntry{}
		k.locks[name] = entry
	}
	entry.refs++
	k.mu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		k.mu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(k.locks, name)
		}
		k.mu.Unlock()
	}
}

var _ cow.Store = (*HybridStore)(nil)

func newHybridStore(cfg *Config) (*HybridStore, error) {
	if cfg == nil || !cfg.Cow.Hybrid.Enable {
		return nil, ErrHybridNotConfigured
	}
	h := cfg.Cow.Hybrid
	cubestorCLIPath := strings.TrimSpace(h.CubestorCLIPath)
	if cubestorCLIPath == "" {
		cubestorCLIPath = defaultHybridCubestorCLIPath
	}
	resolved, err := exec.LookPath(cubestorCLIPath)
	if err != nil {
		return nil, fmt.Errorf("find cubestorcli %q: %w", cubestorCLIPath, err)
	}
	socketPath := strings.TrimSpace(h.SocketPath)
	if socketPath == "" {
		socketPath = defaultHybridSocketPath
	}
	localDir := strings.TrimSpace(h.LocalDir)
	if localDir == "" {
		localDir = filepath.Join(stripStoragePluginDataDir(cfg.DataPath), cow.BackendHybrid, "local")
	}
	sharedDir := strings.TrimSpace(h.SharedDir)
	if sharedDir == "" {
		sharedDir = filepath.Join(stripStoragePluginDataDir(cfg.DataPath), cow.BackendHybrid, "remote")
		if err := checkJFSMount(sharedDir); err != nil {
			return nil, fmt.Errorf("hybrid shared storage is not mounted: %w", err)
		}
	}
	for _, dir := range []string{localDir, sharedDir} {
		if err := os.MkdirAll(filepath.Clean(dir), 0o755); err != nil {
			return nil, fmt.Errorf("create hybrid directory %s: %w", dir, err)
		}
	}
	timeout := defaultHybridTimeout
	if h.CommandTimeoutMS > 0 {
		timeout = time.Duration(h.CommandTimeoutMS) * time.Millisecond
	}
	return &HybridStore{
		cubestorCLIPath: resolved,
		socketPath:      filepath.Clean(socketPath),
		localDir:        filepath.Clean(localDir),
		sharedDir:       filepath.Clean(sharedDir),
		timeout:         timeout,
		run:             runHybridCommand,
		ownedDevice:     make(map[string]string),
	}, nil
}

func runHybridCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

func (s *HybridStore) Name() string {
	return cow.NameHybrid
}

func (s *HybridStore) command(ctx context.Context, args ...string) ([]byte, error) {
	if s == nil || s.run == nil {
		return nil, ErrHybridNotReady
	}
	runCtx := ctx
	cancel := func() {}
	if s.timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, s.timeout)
	}
	defer cancel()
	out, err := s.run(runCtx, s.cubestorCLIPath, args...)
	if err != nil {
		message := strings.TrimSpace(string(out))
		if message == "" {
			message = err.Error()
		}
		return out, fmt.Errorf("cubestorcli %s: %s: %w", strings.Join(args, " "), message, err)
	}
	return out, nil
}

func validateHybridName(name string) error {
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." {
		return fmt.Errorf("invalid empty hybrid object name")
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-' {
			continue
		}
		return fmt.Errorf("invalid hybrid object name %q", name)
	}
	return nil
}

func validateHybridKind(kind string) error {
	switch kind {
	case cowKindVolume, cowKindSnapshot:
		return nil
	default:
		return fmt.Errorf("unsupported hybrid object kind %q", kind)
	}
}

func (s *HybridStore) create(ctx context.Context, name string, sizeBytes uint64, kind string) (*cowVolume, error) {
	if err := validateHybridName(name); err != nil {
		return nil, err
	}
	if err := validateHybridKind(kind); err != nil {
		return nil, err
	}
	if sizeBytes == 0 {
		return nil, fmt.Errorf("hybrid object %s size must be positive", name)
	}
	if sizeBytes > ^uint64(0)-(4096-1) {
		return nil, fmt.Errorf("hybrid object %s size overflows block alignment", name)
	}
	sizeBytes = (sizeBytes + 4095) &^ uint64(4095)
	// Hold the per-name lock across the whole sequence: existence check,
	// create, attach. Releasing it between check and create would let two
	// creators of the same name both pass the check.
	unlock := s.nameLocks.lock(name)
	defer unlock()
	for _, dir := range []string{s.localDir, s.sharedDir} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return nil, fmt.Errorf("%w: name=%s kind=%s", ErrCowObjectAlreadyExists, name, kind)
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	if _, err := s.command(ctx, "create", "--size", strconv.FormatUint(sizeBytes, 10),
		"--dir", s.sharedDir, "--name", name); err != nil {
		return nil, err
	}
	dev, err := s.attachLocked(ctx, name)
	if err != nil {
		cleanupErr := s.deleteLocked(context.Background(), name)
		return nil, errors.Join(err, cleanupErr)
	}
	return newCowVolumeWithSize(name, kind, 0, dev, sizeBytes), nil
}

func (s *HybridStore) clone(ctx context.Context, source, destination, kind string) (*cowVolume, error) {
	if err := validateHybridName(source); err != nil {
		return nil, err
	}
	if err := validateHybridName(destination); err != nil {
		return nil, err
	}
	if err := validateHybridKind(kind); err != nil {
		return nil, err
	}
	// Lock both names in a deterministic order. Cloning into a name while
	// another goroutine clones out of it (or deletes it) must not interleave,
	// and an unordered pair of locks would deadlock.
	first, second := source, destination
	if first > second {
		first, second = second, first
	}
	unlockFirst := s.nameLocks.lock(first)
	defer unlockFirst()
	if second != first {
		unlockSecond := s.nameLocks.lock(second)
		defer unlockSecond()
	}
	if _, err := os.Stat(filepath.Join(s.localDir, source)); errors.Is(err, os.ErrNotExist) {
		if _, resolveErr := s.attachLocked(ctx, source); resolveErr != nil {
			return nil, fmt.Errorf("materialize hybrid source %s: %w", source, resolveErr)
		}
	} else if err != nil {
		return nil, err
	}
	device, err := s.findDevice(ctx, source)
	if err != nil {
		return nil, fmt.Errorf("find hybrid source device %s: %w", source, err)
	}
	if device != "" {
		if _, err := s.command(ctx, "flush", "--device", device); err != nil {
			return nil, err
		}
	}
	if _, err := s.command(ctx, "clone", "--local-dir", s.localDir,
		"--shared-dir", s.sharedDir, source, destination); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return nil, fmt.Errorf("%w: name=%s kind=%s", ErrCowObjectAlreadyExists, destination, kind)
		}
		return nil, err
	}
	size, err := s.GetSizeBytes(ctx, destination)
	if err != nil {
		cleanupErr := s.deleteLocked(context.Background(), destination)
		return nil, errors.Join(err, cleanupErr)
	}
	return newCowVolumeWithSize(destination, kind, 0, "", size), nil
}

func (s *HybridStore) CreateDefaultMediumVolume(ctx context.Context, sandboxID, volumeName string, sizeBytes uint64) (*cowVolume, error) {
	name := fmt.Sprintf("sb-%s-%s", sandboxID, volumeName)
	volume, err := s.create(ctx, name, sizeBytes, cowKindVolume)
	if err != nil {
		return nil, err
	}
	if err := initDefaultMediumDevice(volume.FilePath); err != nil {
		cleanupErr := s.DeleteByKind(context.Background(), name, cowKindVolume)
		return nil, fmt.Errorf("initialize hybrid default medium %s: %w", name, errors.Join(err, cleanupErr))
	}
	return volume, nil
}

func (s *HybridStore) CreateSandboxRootfsFromTemplate(ctx context.Context, sandboxID, templateID string, gen uint32, desiredSizeBytes uint64) (*cowVolume, error) {
	return s.RollbackDeriveNewGen(ctx, sandboxID, cowTemplateRootfsName(templateID), gen, desiredSizeBytes)
}

func (s *HybridStore) RollbackDeriveNewGen(ctx context.Context, sandboxID, sourceName string, gen uint32, desiredSizeBytes uint64) (*cowVolume, error) {
	name := fmt.Sprintf("sb-%s-rootfs-gen%d", sandboxID, gen)
	volume, err := s.clone(ctx, sourceName, name, cowKindSnapshot)
	if err != nil {
		return nil, err
	}
	if desiredSizeBytes > volume.SizeBytes {
		resizeErr := fmt.Errorf("hybrid resize is not supported: source=%d requested=%d", volume.SizeBytes, desiredSizeBytes)
		cleanupErr := s.DeleteByKind(context.Background(), name, cowKindSnapshot)
		return nil, errors.Join(resizeErr, cleanupErr)
	}
	dev, err := s.ResolveDevPath(ctx, name, cowKindSnapshot)
	if err != nil {
		cleanupErr := s.DeleteByKind(context.Background(), name, cowKindSnapshot)
		return nil, errors.Join(err, cleanupErr)
	}
	volume.FilePath = dev
	volume.Gen = gen
	return volume, nil
}

func (s *HybridStore) CreateTemplateBuildRootfs(ctx context.Context, templateID string, sizeBytes uint64) (*cowVolume, error) {
	name := cowTemplateBuildRootfsName(templateID)
	// The build rootfs is scratch space that is never committed in place: the
	// template is published by cloning it into <template>-rootfs. A failed or
	// abandoned build therefore leaves this name behind in the shared remote
	// directory, and a retry with the same template ID would otherwise fail
	// with "already exists" forever. Drop the stale scratch pair first; the
	// shared-remote backend builds on a single node, so no concurrent writer
	// owns it. DeleteByKind is a no-op when the pair is already gone.
	if err := s.DeleteByKind(ctx, name, cowKindVolume); err != nil {
		return nil, fmt.Errorf("clear stale hybrid build rootfs %s: %w", name, err)
	}
	volume, err := s.create(ctx, name, sizeBytes, cowKindVolume)
	if err != nil {
		return nil, err
	}
	if err := initDefaultMediumDevice(volume.FilePath); err != nil {
		cleanupErr := s.DeleteByKind(context.Background(), name, cowKindVolume)
		return nil, fmt.Errorf("initialize hybrid template rootfs %s: %w", name, errors.Join(err, cleanupErr))
	}
	return volume, nil
}

// publish flushes the attached device (if any), copies the local overlay's
// client-written pages into the shared image, then re-attaches. A build rootfs
// only exists in the local overlay until this runs, so a template committed
// without it would clone an empty baseline that every other node -- and this
// node after the local overlay is dropped -- restores as zeroed storage.
//
// cubestorcli sync takes an exclusive flock on the local image, which the
// nbdkit worker holds for as long as the export is attached, so the device has
// to be detached for the copy.
// syncToShared copies client-written local pages into the shared image.
//
// cubestorcli sync takes an exclusive flock on the local image, and the nbdkit
// export holds one for its whole lifetime, so the target must not be attached
// when this runs.
func (s *HybridStore) syncToShared(ctx context.Context, name string) error {
	if _, err := s.command(ctx, "sync", "--local-dir", s.localDir,
		"--shared-dir", s.sharedDir, "--name", name); err != nil {
		return err
	}
	return nil
}

func (s *HybridStore) CommitTemplateRootfs(ctx context.Context, sourceName, templateID string) (*cowVolume, error) {
	// Publish the CLONE, never the source: the source is the build volume the
	// temporary sandbox may still be running against, and detaching it would
	// break that VM. cubestorcli clone copies the local overlay together with
	// the shared image, so the clone is already byte-complete; syncing the
	// clone folds its dirty pages into the shared image so other nodes -- which
	// have no local overlay for this name -- read real content.
	destination := cowTemplateRootfsName(templateID)
	volume, err := s.clone(ctx, sourceName, destination, cowKindSnapshot)
	if err != nil {
		return nil, err
	}
	if err := s.syncToShared(ctx, destination); err != nil {
		cleanupErr := s.DeleteByKind(context.Background(), destination, cowKindSnapshot)
		return nil, errors.Join(fmt.Errorf("publish hybrid template rootfs %s: %w", destination, err), cleanupErr)
	}
	return volume, nil
}

func (s *HybridStore) CreateMemoryVolume(ctx context.Context, templateID string, sizeBytes uint64) (*cowVolume, error) {
	return s.create(ctx, cowTemplateMemoryName(templateID), sizeBytes, cowKindVolume)
}

// CommitTemplateMemory clones sourceName into the canonical template memory
// name.
//
// The source may be a RUNNING sandbox's memory disk, so it must not be touched
// here: detaching it makes the guest fail its next memory access.
// cubestorcli clone copies the local overlay and the shared image together, so
// the clone is already a complete image; publishing the clone is the job of
// DeactivateByKind once whatever writes into it (the VMM or captureLegacyMemory)
// has finished and the device is detached.
func (s *HybridStore) CommitTemplateMemory(ctx context.Context, sourceName, templateID string, sizeBytes uint64) (*cowVolume, error) {
	volume, err := s.clone(ctx, sourceName, cowTemplateMemoryName(templateID), cowKindSnapshot)
	if err != nil {
		return nil, err
	}
	if sizeBytes > volume.SizeBytes {
		resizeErr := fmt.Errorf("hybrid memory resize is not supported: source=%d requested=%d", volume.SizeBytes, sizeBytes)
		cleanupErr := s.DeleteByKind(context.Background(), volume.VolumeName, volume.Kind)
		return nil, errors.Join(resizeErr, cleanupErr)
	}
	dev, err := s.ResolveDevPath(ctx, volume.VolumeName, volume.Kind)
	if err != nil {
		cleanupErr := s.DeleteByKind(context.Background(), volume.VolumeName, volume.Kind)
		return nil, errors.Join(err, cleanupErr)
	}
	volume.FilePath = dev
	return volume, nil
}

func (s *HybridStore) CloneSandboxMemory(ctx context.Context, sandboxID, sourceName string) (*cowVolume, error) {
	name := SandboxMemoryName(strings.TrimSpace(sandboxID))
	// The source is a RUNNING sandbox's memory disk: detaching it to publish
	// would make the guest fail its next memory access. cubestorcli clone opens
	// the source read-only without taking the exclusive lock, so cloning works
	// on a live export -- and the clone is what the pause package restores from,
	// so no publish of the source is needed here.
	volume, err := s.clone(ctx, strings.TrimSpace(sourceName), name, cowKindVolume)
	if err != nil {
		return nil, err
	}
	dev, err := s.ResolveDevPath(ctx, name, cowKindVolume)
	if err != nil {
		cleanupErr := s.DeleteByKind(context.Background(), name, cowKindVolume)
		return nil, errors.Join(err, cleanupErr)
	}
	volume.FilePath = dev
	return volume, nil
}

func (s *HybridStore) findDevice(ctx context.Context, name string) (string, error) {
	devices, err := s.listDevices(ctx)
	if err != nil {
		return "", err
	}
	return devices[name], nil
}

func (s *HybridStore) listDevices(ctx context.Context) (map[string]string, error) {
	out, err := s.command(ctx, "devices")
	if err != nil {
		return nil, err
	}
	devices := make(map[string]string)
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		foundName := ""
		plugin := ""
		for i := 1; i+1 < len(fields); i++ {
			if fields[i] == "name" {
				foundName = fields[i+1]
			}
			if fields[i] == "plugin" {
				plugin = fields[i+1]
			}
		}
		if foundName == "" || plugin != "cubestor" || !strings.HasPrefix(fields[0], "nbd") {
			continue
		}
		devices[foundName] = filepath.Join("/dev", fields[0])
	}
	return devices, nil
}

func (s *HybridStore) ResolveDevPath(ctx context.Context, name, kind string) (string, error) {
	if err := validateHybridName(name); err != nil {
		return "", err
	}
	if err := validateHybridKind(kind); err != nil {
		return "", err
	}
	unlock := s.nameLocks.lock(name)
	defer unlock()
	return s.attachLocked(ctx, name)
}

// attachLocked attaches name and returns its device path, reusing an existing
// attachment. The caller must hold s.nameLocks for name; that lock is what
// makes "look for an existing device, else attach" atomic, so two concurrent
// resolves of the same export cannot both spawn a worker.
func (s *HybridStore) attachLocked(ctx context.Context, name string) (string, error) {
	if device, err := s.findDevice(ctx, name); err != nil || device != "" {
		if err == nil {
			if objectErr := s.validateObjectPair(name); objectErr != nil {
				return "", fmt.Errorf("refuse unowned hybrid device %s for %s: %w", device, name, objectErr)
			}
			s.rememberDevice(name, device)
		}
		return device, err
	}
	out, err := s.command(ctx, "attach", "--unix", s.socketPath, "--name", name)
	if err != nil {
		return "", err
	}
	device := ""
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "device" {
			device = fields[1]
			break
		}
	}
	if device == "" {
		if discovered, findErr := s.findDevice(ctx, name); findErr == nil && discovered != "" {
			s.rememberDevice(name, discovered)
		}
		return "", fmt.Errorf("cubestorcli attach %s returned no device: %s", name, strings.TrimSpace(string(out)))
	}
	base := filepath.Base(device)
	if filepath.Dir(device) != "/dev" || !strings.HasPrefix(base, "nbd") {
		return "", fmt.Errorf("cubestorcli attach %s returned invalid device %q", name, device)
	}
	if _, err := strconv.ParseUint(strings.TrimPrefix(base, "nbd"), 10, 32); err != nil {
		return "", fmt.Errorf("cubestorcli attach %s returned invalid device %q", name, device)
	}
	if err := s.validateObjectPair(name); err != nil {
		_, detachErr := s.command(context.Background(), "detach", "--device", device)
		return "", errors.Join(fmt.Errorf("validate attached hybrid object %s: %w", name, err), detachErr)
	}
	s.rememberDevice(name, device)
	return device, nil
}

func (s *HybridStore) validateObjectPair(name string) error {
	for _, dir := range []string{s.localDir, s.sharedDir} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%s/%s is not a regular file", dir, name)
		}
	}
	return nil
}

// rememberDevice records our ownership of an attachment. devMu only guards the
// map; it is never held across a subprocess call.
func (s *HybridStore) rememberDevice(name, device string) {
	s.devMu.Lock()
	if s.ownedDevice == nil {
		s.ownedDevice = make(map[string]string)
	}
	s.ownedDevice[name] = device
	s.devMu.Unlock()
}

func (s *HybridStore) forgetDevice(name string) {
	s.devMu.Lock()
	delete(s.ownedDevice, name)
	s.devMu.Unlock()
}

func (s *HybridStore) ownsDevice(name string) bool {
	s.devMu.Lock()
	defer s.devMu.Unlock()
	_, owned := s.ownedDevice[name]
	return owned
}

func (s *HybridStore) DeactivateByKind(ctx context.Context, name, kind string) error {
	if err := validateHybridName(name); err != nil {
		return err
	}
	if err := validateHybridKind(kind); err != nil {
		return err
	}
	// The per-name lock keeps a detach from racing an attach/deactivate of the
	// same export. Publishing runs outside it: it is a full local->shared copy
	// and holding the name lock would block every other operation on this
	// object for its duration.
	unlock := s.nameLocks.lock(name)
	err := s.deactivateLocked(ctx, name)
	unlock()
	if err != nil {
		return err
	}
	// Publishing to the shared image is NOT done here. It copies the whole local
	// overlay and needs an exclusive lock, so doing it inline made it 64% of
	// snapshot latency (measured: 1960ms of 3042ms). The hybridPublisher merges
	// in the background while Master polls remote_status.
	return nil
}

// publishVolume merges one volume from the local overlay into the shared image.
//
// cubestorcli sync takes an exclusive flock on the local image, so the export
// must be detached first. The volume is deliberately NOT re-attached: published
// content already lives in the shared image, and leaving the export attached
// after the snapshot RPC has returned races the delete/GC path that is entitled
// to remove the volume as soon as the caller stops using it (observed as
// "cubestorcli delete ... is in use"). A later restore attaches on demand
// through ResolveDevPath.
// publishVolume merges one volume from the local overlay into the shared image.
//
// This only ever receives a snapshot's OWN volumes (tpl-snap-<id>-rootfs and
// -memory, resolved from the package catalog). The source volumes the running VM
// is using -- its sb-<id>-rootfs-genN and the template's tpl-<id>-memory -- are
// never passed here, so publishing cannot disturb a live sandbox.
//
// cubestorcli sync takes an exclusive flock on the local image, so the export
// must not be attached. The snapshot path detaches its own clones before this
// runs, which makes the detach below a no-op in the common case; it is kept
// because a restore that arrived between capture and publish leaves the export
// attached, and a retry must still work.
//
// The volume is deliberately NOT re-attached afterwards: the published content
// lives in the shared image, and leaving an export attached would race the
// delete/GC path that is entitled to remove the volume once the caller stops
// using it (observed as "cubestorcli delete ... is in use").
func (s *HybridStore) publishVolume(ctx context.Context, name string) error {
	if err := validateHybridName(name); err != nil {
		return err
	}
	unlock := s.nameLocks.lock(name)
	device, findErr := s.findDevice(ctx, name)
	if findErr != nil {
		unlock()
		return fmt.Errorf("find hybrid device %s before publish: %w", name, findErr)
	}
	if device != "" {
		// Already detached by the snapshot path in the normal flow.
		if _, err := s.command(ctx, "flush", "--device", device); err != nil {
			unlock()
			return err
		}
		if _, err := s.command(ctx, "detach", "--device", device); err != nil {
			unlock()
			return err
		}
		s.forgetDevice(name)
	}
	unlock()

	// Nothing to merge when the overlay already records itself as synced.
	if synced, err := s.volumeSynced(ctx, name); err == nil && synced {
		return nil
	}

	// syncToShared is the slow part; run it without the name lock so unrelated
	// operations on this object are not blocked behind a full overlay copy.
	if err := s.syncToShared(ctx, name); err != nil {
		// The package may have been deleted while we were merging. Delete now
		// refuses while remote_status is unsettled, so this is limited to TTL
		// expiry and operator cleanup of a FAILED row; nothing is left to
		// publish either way.
		if snapshotGoneFromLocal(s.localDir, name) {
			return nil
		}
		return err
	}
	return nil
}

// snapshotGoneFromLocal reports whether the local overlay no longer exists,
// which means the object was deleted underneath the publisher.
func snapshotGoneFromLocal(localDir, name string) bool {
	_, err := os.Stat(filepath.Join(localDir, name))
	return errors.Is(err, os.ErrNotExist)
}

// volumeSynced reports whether the shared image already contains every page of
// this volume, using the overlay's own OVL_F_SYNCED flag.
func (s *HybridStore) volumeSynced(ctx context.Context, name string) (bool, error) {
	if err := validateHybridName(name); err != nil {
		return false, err
	}
	out, err := s.command(ctx, "info", "--dir", s.localDir, "--name", name)
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[0] == "flags" {
			for _, flag := range fields[1:] {
				if flag == "synced" {
					return true, nil
				}
			}
			return false, nil
		}
	}
	return false, nil
}

func (s *HybridStore) deactivateLocked(ctx context.Context, name string) error {
	device, err := s.findDevice(ctx, name)
	if err != nil {
		return err
	}
	if device == "" {
		return nil
	}
	if !s.ownsDevice(name) {
		if err := s.validateObjectPair(name); err != nil {
			return fmt.Errorf("refuse to deactivate unowned hybrid device %s for %s: %w", device, name, err)
		}
	}
	if _, err := s.command(ctx, "flush", "--device", device); err != nil {
		return err
	}
	if _, err := s.command(ctx, "detach", "--device", device); err != nil {
		return err
	}
	s.forgetDevice(name)
	return nil
}

func (s *HybridStore) DeleteByKind(ctx context.Context, name, kind string) error {
	if err := validateHybridName(name); err != nil {
		return err
	}
	if err := validateHybridKind(kind); err != nil {
		return err
	}
	unlock := s.nameLocks.lock(name)
	defer unlock()
	return s.deleteLocked(ctx, name)
}

func (s *HybridStore) deleteLocked(ctx context.Context, name string) error {
	if err := s.deactivateLocked(ctx, name); err != nil {
		return err
	}
	args := []string{"delete"}
	for _, dir := range []string{s.localDir, s.sharedDir} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			args = append(args, "--dir", dir)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if len(args) == 1 {
		return nil
	}
	args = append(args, "--name", name)
	_, err := s.command(ctx, args...)
	return err
}

func (s *HybridStore) GetSizeBytes(ctx context.Context, name string) (uint64, error) {
	if err := validateHybridName(name); err != nil {
		return 0, err
	}
	out, err := s.command(ctx, "info", "--dir", s.sharedDir, "--name", name)
	if err != nil {
		if strings.Contains(err.Error(), "No such file") {
			return 0, &CowObjectMissingError{VolumeName: name, Cause: err}
		}
		return 0, err
	}
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || fields[0] != "virtual_size" {
			continue
		}
		size, parseErr := strconv.ParseUint(fields[1], 10, 64)
		if parseErr != nil {
			return 0, fmt.Errorf("parse cubestorcli size for %s: %w", name, parseErr)
		}
		return size, nil
	}
	return 0, fmt.Errorf("cubestorcli info %s returned no virtual_size", name)
}

func (s *HybridStore) GetVolumeInfo(ctx context.Context, name string) (*cubecow.Volume, error) {
	size, err := s.GetSizeBytes(ctx, name)
	if err != nil {
		return nil, err
	}
	device, err := s.findDevice(ctx, name)
	if err != nil {
		return nil, err
	}
	return &cubecow.Volume{Name: name, SizeBytes: size, DevicePath: device}, nil
}

func (s *HybridStore) GetMetrics(ctx context.Context) (map[string]uint64, error) {
	_ = ctx
	metrics := map[string]uint64{
		"total_bytes":    0,
		"used_bytes":     0,
		"volume_count":   0,
		"snapshot_count": 0,
	}
	objects := make(map[string]struct{})
	for key, dir := range map[string]string{"local_objects": s.localDir, "shared_objects": s.sharedDir} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if !entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
				metrics[key]++
				objects[entry.Name()] = struct{}{}
				info, err := entry.Info()
				if err != nil {
					return nil, err
				}
				if info.Size() > 0 {
					metrics["used_bytes"] += uint64(info.Size())
				}
			}
		}
	}
	// cubestor does not persist Cube's volume/snapshot kind in the image.
	// Report unique logical objects as volumes and retain the required common
	// metric keys; backend-specific local/shared counters carry the useful split.
	metrics["volume_count"] = uint64(len(objects))
	metrics["total_bytes"] = metrics["used_bytes"]
	return metrics, nil
}

func (s *HybridStore) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	// Snapshot our attachments, then release the map lock: Close must not hold
	// devMu across subprocess calls, or every concurrent attach would block on
	// shutdown. Names are re-checked per object under their own name lock.
	s.devMu.Lock()
	pending := make(map[string]string, len(s.ownedDevice))
	for name, device := range s.ownedDevice {
		pending[name] = device
	}
	s.devMu.Unlock()

	var closeErr error
	for name, device := range pending {
		unlock := s.nameLocks.lock(name)
		if !s.ownsDevice(name) {
			// Another goroutine already detached it while we waited.
			unlock()
			continue
		}
		if _, err := s.command(ctx, "flush", "--device", device); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("flush hybrid object %s: %w", name, err))
			unlock()
			continue
		}
		if _, err := s.command(ctx, "detach", "--device", device); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("detach hybrid object %s: %w", name, err))
			unlock()
			continue
		}
		s.forgetDevice(name)
		unlock()
	}
	return closeErr
}
