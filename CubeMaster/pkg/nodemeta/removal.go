// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package nodemeta

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	cubebox "github.com/tencentcloud/CubeSandbox/CubeMaster/api/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/db/models"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/log"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/cubelet"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/localcache"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/sandboxspec"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrNodeNotIsolated          = errors.New("node must be isolated before removal")
	ErrNodeRemovalBusy          = errors.New("node still has dependent resources")
	ErrNodeInventoryUnavailable = errors.New("node inventory is unavailable")
	ErrNodeIdentityChanged      = errors.New("node registration changed during removal")
)

// NodeRemovalReferences describes resources which must be drained or cleaned
// before a node can be removed safely.
type NodeRemovalReferences struct {
	Sandboxes          int64 `json:"sandboxes"`
	ActiveRuntimeRef   int64 `json:"active_runtime_refs"`
	TemplateReplicas   int64 `json:"template_replicas"`
	ArtifactPlacements int64 `json:"artifact_placements"`
}

func (r NodeRemovalReferences) Empty() bool {
	return r.Sandboxes == 0 &&
		r.ActiveRuntimeRef == 0 &&
		r.TemplateReplicas == 0 &&
		r.ArtifactPlacements == 0
}

func (r NodeRemovalReferences) String() string {
	return fmt.Sprintf(
		"sandboxes=%d active_runtime_refs=%d template_replicas=%d artifact_placements=%d",
		r.Sandboxes, r.ActiveRuntimeRef, r.TemplateReplicas, r.ArtifactPlacements,
	)
}

// NodeRemovalBlockedError is returned when decommission prerequisites have
// not converged. It unwraps to ErrNodeRemovalBusy for stable API mapping.
type NodeRemovalBlockedError struct {
	References NodeRemovalReferences
}

func (e *NodeRemovalBlockedError) Error() string {
	return fmt.Sprintf("%s: %s", ErrNodeRemovalBusy, e.References)
}

func (e *NodeRemovalBlockedError) Unwrap() error {
	return ErrNodeRemovalBusy
}

func (s *service) lockNodeLifecycle(nodeID string) func() {
	v, _ := s.lifecycleLocks.LoadOrStore(nodeID, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// DeleteNode removes an already drained and isolated node. Dependency checks
// and metadata deletion share one transaction so no partially removed node is
// visible. A repeated request returns gorm.ErrRecordNotFound.
func DeleteNode(ctx context.Context, nodeID string) (*NodeSnapshot, error) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return nil, fmt.Errorf("node_id is required")
	}

	unlockLifecycle := global.lockNodeLifecycle(nodeID)
	defer unlockLifecycle()
	unlockLabels := global.lockNodeLabels(nodeID)
	defer unlockLabels()

	var expectedRegistration models.NodeRegistration
	if err := global.db.WithContext(ctx).Where("node_id = ?", nodeID).
		First(&expectedRegistration).Error; err != nil {
		return nil, err
	}
	preflight, err := nodeRemovalInventoryPreflight(ctx, nodeID, expectedRegistration.HostIP)
	if err != nil {
		return nil, err
	}
	if preflight > 0 {
		return nil, &NodeRemovalBlockedError{
			References: NodeRemovalReferences{Sandboxes: preflight},
		}
	}

	var removed *NodeSnapshot
	err = global.db.Transaction(func(tx *gorm.DB) error {
		var registration models.NodeRegistration
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("node_id = ?", nodeID).
			First(&registration).Error; err != nil {
			return err
		}
		if registration.HostIP != expectedRegistration.HostIP {
			return ErrNodeIdentityChanged
		}

		snap, err := snapshotFromRegistration(&registration)
		if err != nil {
			return err
		}
		if !snapshotSchedulingDisabled(snap) {
			return ErrNodeNotIsolated
		}

		refs, err := countNodeRemovalReferences(tx, nodeID)
		if err != nil {
			return err
		}
		if !refs.Empty() {
			return &NodeRemovalBlockedError{References: refs}
		}

		if err := tx.Unscoped().Where("node_id = ?", nodeID).
			Delete(&models.NodeStatus{}).Error; err != nil {
			return err
		}
		if err := tx.Where("node_id = ?", nodeID).
			Delete(&models.NodeComponentVersion{}).Error; err != nil {
			return err
		}
		if err := tx.Unscoped().Where("node_id = ?", nodeID).
			Delete(&models.NodeRegistration{}).Error; err != nil {
			return err
		}
		removed = snap
		return nil
	})
	if err != nil {
		return nil, err
	}

	global.removedNodes.Store(nodeID, expectedRegistration.ID)
	global.mu.Lock()
	delete(global.nodes, nodeID)
	global.mu.Unlock()
	global.versionWriteLocks.Delete(nodeID)
	localcache.RemoveNode(nodeID)
	if err := localcache.DeleteNodeMetric(ctx, nodeID); err != nil {
		// The key has a safety TTL and is not authoritative. Do not turn a
		// committed database removal into a client-visible failure.
		log.G(ctx).Warnf("node removed but metric cleanup failed node_id=%s err=%v", nodeID, err)
	}
	log.G(ctx).Infof("node removed node_id=%s", nodeID)
	return cloneSnapshot(removed), nil
}

func nodeRemovalInventoryPreflight(ctx context.Context, nodeID, expectedHostIP string) (int64, error) {
	node, ok := localcache.GetNode(nodeID)
	if !ok || node == nil {
		return 0, fmt.Errorf("%w: node %s is not in local cache", ErrNodeInventoryUnavailable, nodeID)
	}
	if node.HostIP() != expectedHostIP {
		return 0, fmt.Errorf("%w: cached host_ip=%s registered host_ip=%s",
			ErrNodeIdentityChanged, node.HostIP(), expectedHostIP)
	}
	if !node.SchedulingDisabled() {
		return 0, ErrNodeNotIsolated
	}
	if localcache.LocalCreateConcurrentLimit(node) > 0 {
		return 0, fmt.Errorf("%w: sandbox creation is still in progress", ErrNodeRemovalBusy)
	}

	rsp, err := cubelet.List(ctx, cubelet.GetCubeletAddr(node.HostIP()), &cubebox.ListCubeSandboxRequest{})
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrNodeInventoryUnavailable, err)
	}
	current, ok := localcache.GetNode(nodeID)
	if !ok || current == nil {
		return 0, fmt.Errorf("%w: node %s left local cache during inventory check", ErrNodeInventoryUnavailable, nodeID)
	}
	if localcache.LocalCreateConcurrentLimit(current) > 0 {
		return 0, fmt.Errorf("%w: sandbox creation started during inventory check", ErrNodeRemovalBusy)
	}
	return int64(len(rsp.GetItems())), nil
}

func countNodeRemovalReferences(tx *gorm.DB, nodeID string) (NodeRemovalReferences, error) {
	var refs NodeRemovalReferences
	if err := sandboxspec.PruneExpiredNodeCreateLeasesTx(tx, nodeID, time.Now()); err != nil {
		return refs, err
	}
	checks := []struct {
		model  interface{}
		column string
		count  *int64
	}{
		{&models.SandboxSpec{}, "host_id", &refs.Sandboxes},
		{&models.SnapshotRuntimeActive{}, "node_id", &refs.ActiveRuntimeRef},
		{&models.TemplateReplica{}, "node_id", &refs.TemplateReplicas},
		{&models.ArtifactNodePlacement{}, "node_id", &refs.ArtifactPlacements},
	}
	for _, check := range checks {
		if err := tx.Model(check.model).Where(check.column+" = ?", nodeID).Count(check.count).Error; err != nil {
			return refs, err
		}
	}
	return refs, nil
}

func snapshotFromRegistration(reg *models.NodeRegistration) (*NodeSnapshot, error) {
	labels, err := parseLabelsJSON(reg.LabelsJSON)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLabelsJSONCorrupt, err)
	}
	return &NodeSnapshot{
		NodeID:         reg.NodeID,
		registrationID: reg.ID,
		HostIP:         reg.HostIP,
		GRPCPort:       reg.GRPCPort,
		Labels:         labels,
		InstanceType:   reg.InstanceType,
		ClusterLabel:   reg.ClusterLabel,
	}, nil
}
