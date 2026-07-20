// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package nodemeta

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	cubebox "github.com/tencentcloud/CubeSandbox/CubeMaster/api/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/db/models"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/node"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/cubelet"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/localcache"
)

func TestNodeRemovalReferences(t *testing.T) {
	empty := NodeRemovalReferences{}
	if !empty.Empty() {
		t.Fatal("zero references must be removable")
	}

	refs := NodeRemovalReferences{Sandboxes: 1, ArtifactPlacements: 2}
	if refs.Empty() {
		t.Fatal("non-zero references must block removal")
	}
	err := &NodeRemovalBlockedError{References: refs}
	if !errors.Is(err, ErrNodeRemovalBusy) {
		t.Fatal("blocked error must unwrap to ErrNodeRemovalBusy")
	}
	for _, want := range []string{"sandboxes=1", "artifact_placements=2"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not include %q", err.Error(), want)
		}
	}
}

func TestSnapshotFromRegistrationRequiresValidCordon(t *testing.T) {
	reg := &models.NodeRegistration{
		NodeID:     "node-1",
		HostIP:     "10.0.0.1",
		LabelsJSON: `{"` + constants.LabelSchedulingDisabled + `":"` + constants.LabelSchedulingDisabledValue + `"}`,
	}
	snap, err := snapshotFromRegistration(reg)
	if err != nil {
		t.Fatalf("snapshotFromRegistration: %v", err)
	}
	if !snapshotSchedulingDisabled(snap) {
		t.Fatal("control-plane scheduling-disabled label must be preserved")
	}
}

func TestNodeRemovalInventoryPreflightCountsLiveSandboxes(t *testing.T) {
	cordoned := &node.Node{InsID: "node-1", IP: "10.0.0.1"}
	cordoned.SetSchedulingDisabled(true)
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	patches.ApplyFunc(localcache.GetNode, func(string) (*node.Node, bool) {
		return cordoned, true
	})
	patches.ApplyFunc(cubelet.List, func(context.Context, string, *cubebox.ListCubeSandboxRequest) (*cubebox.ListCubeSandboxResponse, error) {
		return &cubebox.ListCubeSandboxResponse{Items: []*cubebox.CubeSandbox{{Id: "sb-1"}}}, nil
	})

	count, err := nodeRemovalInventoryPreflight(context.Background(), "node-1", "10.0.0.1")
	if err != nil {
		t.Fatalf("nodeRemovalInventoryPreflight: %v", err)
	}
	if count != 1 {
		t.Fatalf("live sandbox count=%d, want 1", count)
	}
}
