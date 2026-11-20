// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/log"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// hybridPublisher implements [cow.Uploader] for the hybrid backend.
//
// Hybrid keeps a volume in two layers: the local XFS overlay holds every page
// the guest wrote, and the shared JuiceFS image holds the baseline those writes
// were layered on. Merging the two (cubestorcli sync) is what makes the package
// readable from another node. Completed immutable exports can remain attached
// while the publisher persists the local page cache and merges the image.
//
// The snapshot RPC returns after local capture. Publishing runs in the
// background while Master polls UploadStatus. Hybrid's JuiceFS mount must
// disable writeback: cubestorcli sync fsyncs the shared image before returning,
// and only then may UploadStatus report that object storage publication is ready.
type hybridPublisher struct {
	store *HybridStore

	mu     sync.Mutex
	states map[string]*hybridPublishState
}

type hybridPublishState struct {
	done      chan struct{}
	started   time.Time
	finished  time.Time
	err       error
	uuids     *cow.RemoteUUIDs
	queuedMsg string

	// busyRetries counts how many times the merge was deferred because an
	// export still held the object. Surfaced through UploadStatus so a package
	// that stays busy is visible instead of looking stuck.
	busyRetries int
}

func newHybridPublisher(store *HybridStore) *hybridPublisher {
	return &hybridPublisher{store: store, states: make(map[string]*hybridPublishState)}
}

// Upload starts publishing the package in the background and returns
// immediately. RemoteUUIDs are only known after the merge, so the reply carries
// the object names the caller can already reference.
func (p *hybridPublisher) Upload(ctx context.Context, snapshotID string) (*cow.RemoteUUIDs, error) {
	id := strings.TrimSpace(snapshotID)
	if id == "" {
		return nil, fmt.Errorf("snapshot_id is required")
	}
	uuids, err := hybridPackageFiles(ctx, id)
	if err != nil {
		return nil, err
	}
	p.start(id)
	return uuids, nil
}

// start launches one background publish per snapshot id. A second call for the
// same id joins the running one instead of queueing another merge.
func (p *hybridPublisher) start(id string) {
	p.mu.Lock()
	if _, ok := p.states[id]; ok {
		p.mu.Unlock()
		return
	}
	state := &hybridPublishState{done: make(chan struct{}), started: time.Now()}
	p.states[id] = state
	p.mu.Unlock()

	go func() {
		// Deliberately detached from the request context: the snapshot RPC has
		// already returned, so its context is cancelled.
		ctx, cancel := context.WithTimeout(context.Background(), hybridPublishTimeout)
		defer cancel()
		// A busy object is not a failure: another export (usually a restore
		// mapping this package's memory) still holds its overlay lock, and
		// there is nothing to do but wait for it. Keeping the state alive keeps
		// remote_status in progress, which is what Master polls.
		for {
			err := p.publish(ctx, id)
			if err == nil || !errors.Is(err, errHybridPublishBusy) {
				p.mu.Lock()
				state.err = err
				p.mu.Unlock()
				break
			}
			p.mu.Lock()
			state.busyRetries++
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				p.mu.Lock()
				state.err = fmt.Errorf("publish %s still busy after %s: %w", id, hybridPublishTimeout, ctx.Err())
				p.mu.Unlock()
				goto done
			case <-time.After(hybridPublishBusyRetry):
			}
		}
	done:
		p.mu.Lock()
		state.finished = time.Now()
		p.mu.Unlock()
		close(state.done)
	}()
}

// publish merges every object of the package from the local overlay into the
// shared image, in the order the package was assembled.
func (p *hybridPublisher) publish(ctx context.Context, snapshotID string) error {
	names, err := hybridPackageObjectNames(ctx, snapshotID)
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := p.store.publishVolume(ctx, name); err != nil {
			return fmt.Errorf("publish %s: %w", name, err)
		}
		log.G(ctx).WithFields(map[string]interface{}{
			"snapshot_id": snapshotID,
			"volume":      name,
		}).Infof("hybrid publish: %s is in the shared image", name)
	}
	return publishHybridMetadata(ctx, snapshotID)
}

// UploadStatus reports the merge progress for Master's remote_status poller.
func (p *hybridPublisher) UploadStatus(ctx context.Context, snapshotID string) (*cow.RemoteStatus, error) {
	id := strings.TrimSpace(snapshotID)
	if id == "" {
		return nil, fmt.Errorf("snapshot_id is required")
	}
	p.mu.Lock()
	state := p.states[id]
	p.mu.Unlock()

	if state == nil {
		// No publish in this process: either it already finished (a previous
		// Cubelet) or the row predates asynchronous publishing. Verify against
		// the shared image rather than guessing.
		ready, err := p.alreadyPublished(ctx, id)
		switch {
		case err != nil:
			return &cow.RemoteStatus{SnapshotID: id, State: cow.RemoteStateFailed, Message: err.Error()}, nil
		case ready:
			return &cow.RemoteStatus{SnapshotID: id, State: cow.RemoteStateReady, Message: "all volumes present in shared image"}, nil
		default:
			// Nothing to resume from: kick one off so a Cubelet restart does
			// not strand the row in "in progress" forever.
			p.start(id)
			return &cow.RemoteStatus{SnapshotID: id, State: cow.RemoteStateRunning, Message: "publish restarted"}, nil
		}
	}

	select {
	case <-state.done:
		if state.err != nil {
			return &cow.RemoteStatus{SnapshotID: id, State: cow.RemoteStateFailed, Message: state.err.Error()}, nil
		}
		return &cow.RemoteStatus{
			SnapshotID:  id,
			State:       cow.RemoteStateReady,
			Message:     fmt.Sprintf("all volumes uploaded in %s", state.finished.Sub(state.started).Round(time.Millisecond)),
			RemoteUUIDs: state.uuids,
		}, nil
	default:
		p.mu.Lock()
		retries := state.busyRetries
		p.mu.Unlock()
		message := fmt.Sprintf("publishing for %s", time.Since(state.started).Round(time.Second))
		if retries > 0 {
			message += fmt.Sprintf(", deferred %d time(s): an export still holds the object", retries)
		}
		return &cow.RemoteStatus{
			SnapshotID: id,
			State:      cow.RemoteStateRunning,
			Message:    message,
		}, nil
	}
}

// alreadyPublished reports whether every object of the package has a local
// overlay marked synced, which is the on-disk record that the shared image
// already contains its pages.
func (p *hybridPublisher) alreadyPublished(ctx context.Context, snapshotID string) (bool, error) {
	names, err := hybridPackageObjectNames(ctx, snapshotID)
	if err != nil {
		return false, err
	}
	if len(names) == 0 {
		return false, nil
	}
	for _, name := range names {
		synced, err := p.store.volumeSynced(ctx, name)
		if err != nil {
			return false, err
		}
		if !synced {
			return false, nil
		}
	}
	return hybridMetadataPublished(ctx, snapshotID)
}

// hybridPackageObjectNames returns the data volumes a hybrid snapshot package
// consists of: the rootfs generation and its memory image.
func hybridPackageObjectNames(ctx context.Context, snapshotID string) ([]string, error) {
	entry, err := GetLocalSnapshotFor(ctx, cow.BackendHybrid, snapshotID)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, fmt.Errorf("hybrid package %s: %w", snapshotID, ErrSnapshotCatalogNotFound)
	}
	names := make([]string, 0, 2)
	for _, name := range []string{strings.TrimSpace(entry.RootfsVol), strings.TrimSpace(entry.MemoryVol)} {
		if name != "" {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("hybrid package %s names no volumes", snapshotID)
	}
	return names, nil
}

func hybridPackageFiles(ctx context.Context, snapshotID string) (*cow.RemoteUUIDs, error) {
	names, err := hybridPackageObjectNames(ctx, snapshotID)
	if err != nil {
		return nil, err
	}
	return &cow.RemoteUUIDs{Rootfs: firstOr(names, 0), Memory: firstOr(names, 1)}, nil
}

func firstOr(names []string, i int) string {
	if i < len(names) {
		return names[i]
	}
	return ""
}

const hybridPublishTimeout = 30 * time.Minute

// hybridPublishBusyRetry spaces out retries while an export still holds the
// object. A restore attaches its memory right before mapping it and detaches it
// when the sandbox is destroyed, so the wait can legitimately be long; the
// publish is idempotent and costs one `devices` call per tick.
const hybridPublishBusyRetry = 2 * time.Second

// hybridPublishers keeps one publisher per HybridStore so a snapshot upload and
// the status poll that follows it observe the same in-flight state.
var hybridPublishers sync.Map // *HybridStore -> *hybridPublisher

// hybridUploaderFor returns the [cow.Uploader] for the live hybrid Store.
func hybridUploaderFor() (cow.Uploader, error) {
	store, err := requireCowStoreFor(cow.BackendHybrid)
	if err != nil {
		return nil, err
	}
	hybrid, ok := store.(*HybridStore)
	if !ok || hybrid == nil {
		return nil, errHybridPublishNotConfigured
	}
	if cached, ok := hybridPublishers.Load(hybrid); ok {
		return cached.(*hybridPublisher), nil
	}
	publisher := newHybridPublisher(hybrid)
	actual, _ := hybridPublishers.LoadOrStore(hybrid, publisher)
	return actual.(*hybridPublisher), nil
}

var errHybridPublishNotConfigured = errors.New("hybrid publisher is not configured")
