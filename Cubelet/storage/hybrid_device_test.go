// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHybridCachedDeviceValidatesWorkerLifetime(t *testing.T) {
	s, calls := newFakeHybridStore(t)
	id := hybridDeviceIdentity{PID: "20", Start: "100", Socket: s.socketPath, Name: "base"}
	s.readDevice = func(string) (hybridDeviceIdentity, error) { return id, nil }
	s.rememberDevice("base", "/dev/nbd7")
	if d, e := s.findDevice(context.Background(), "base"); e != nil || d != "/dev/nbd7" {
		t.Fatalf("%s %v", d, e)
	}
	if len(*calls) != 0 {
		t.Fatalf("cached binding enumerated devices: %v", *calls)
	}
	// Reuse the same PID and device for another process lifetime. Must discover
	// again even if it happens to use the same export name.
	id.Start = "200"
	if d, e := s.findDevice(context.Background(), "base"); e != nil || d != "" {
		t.Fatalf("stale binding returned: %s %v", d, e)
	}
	if len(*calls) != 1 || (*calls)[0] != "devices" {
		t.Fatalf("missing rediscovery: %v", *calls)
	}
}

func TestHybridDeviceIdentityParsingAndReassignment(t *testing.T) {
	stat := "42 (worker (with spaces)) " + strings.Repeat("0 ", 19) + "1234 0"
	files := map[string]string{
		"/sys/block/nbd7/size": "4096", "/sys/block/nbd7/pid": "42",
		"/proc/42/stat":    stat,
		"/proc/42/cmdline": "/usr/bin/cubestorcli\x00attach\x00--unix\x00/run/test.sock\x00--name\x00base\x00",
	}
	read := func(p string) ([]byte, error) {
		if v, ok := files[p]; ok {
			return []byte(v), nil
		}
		return nil, os.ErrNotExist
	}
	id, err := readHybridDeviceIdentity(read, "/dev/nbd7")
	if err != nil || id.Start != "1234" || id.Name != "base" {
		t.Fatalf("%+v %v", id, err)
	}
	n := 0
	changing := func(p string) ([]byte, error) {
		if p == "/sys/block/nbd7/pid" {
			n++
			if n == 2 {
				return []byte("43"), nil
			}
		}
		return read(p)
	}
	if _, err := readHybridDeviceIdentity(changing, "/dev/nbd7"); err == nil {
		t.Fatal("accepted reassigned device")
	}
	files["/sys/block/nbd7/size"] = "0"
	if _, err := readHybridDeviceIdentity(read, "/dev/nbd7"); err == nil {
		t.Fatal("accepted disconnected device")
	}
}

func TestHybridTemplateClonesOverlapAndFencePublisher(t *testing.T) {
	s, _ := newFakeHybridStore(t)
	src := "tpl-tpl-concurrent-memory"
	if err := os.WriteFile(filepath.Join(s.localDir, src), []byte("local"), 0600); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	s.run = func(ctx context.Context, _ string, args ...string) ([]byte, error) {
		switch args[0] {
		case "devices":
			return nil, nil
		case "clone":
			entered <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return nil, nil
		case "size":
			return []byte("8192"), nil
		}
		return nil, fmt.Errorf("unexpected %v", args)
	}
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			_, e := s.clone(context.Background(), src, fmt.Sprintf("clone-%d", i), cowKindSnapshot)
			errs <- e
		}(i)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("same-template clones serialized")
		}
	}
	locked := make(chan struct{})
	go func() { unlock := s.nameLocks.lock(src); close(locked); unlock() }()
	select {
	case <-locked:
		t.Fatal("publisher acquired source while clones read it")
	case <-time.After(20 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	for i := 0; i < 2; i++ {
		if e := <-errs; e != nil {
			t.Fatal(e)
		}
	}
	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("writer never resumed")
	}
}

func TestHybridDiscoverySharesOverlappingCalls(t *testing.T) {
	s, _ := newFakeHybridStore(t)
	var calls atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	s.run = func(context.Context, string, ...string) ([]byte, error) {
		calls.Add(1)
		close(entered)
		<-release
		return nil, errors.New("discovery failed")
	}
	done := make(chan error, 2)
	go func() { _, e := s.findDevice(context.Background(), "a"); done <- e }()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _, e := s.findDevice(ctx, "b"); done <- e }()
	// A canceled waiter must be able to leave without canceling the discovery
	// used by the first request.
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatalf("waiter: %v", e)
	}
	close(release)
	if e := <-done; e == nil {
		t.Fatal("lost discovery error")
	}
	if calls.Load() != 1 {
		t.Fatalf("discovery calls=%d", calls.Load())
	}
}

// A slow upload must not serialize restoration, while deletion remains fenced.
func TestHybridRestoreDuringPublication(t *testing.T) {
	s, calls := newFakeHybridStore(t)
	name := "tpl-snap-online-memory"
	for _, dir := range []string{s.localDir, s.sharedDir} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("image"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	base := s.run
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	s.run = func(ctx context.Context, path string, args ...string) ([]byte, error) {
		if args[0] == "sync" {
			if !strings.Contains(strings.Join(args, " "), "--immutable") {
				t.Error("mutable publication")
			}
			close(started)
			<-release
			return nil, nil
		}
		return base(ctx, path, args...)
	}
	published := make(chan error, 1)
	go func() { published <- s.publishVolume(context.Background(), name) }()
	<-started
	restored := make(chan error, 1)
	go func() {
		_, err := s.ResolveSnapshotMemory(context.Background(), name, cowKindSnapshot)
		restored <- err
	}()
	select {
	case err := <-restored:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("restore waited for upload")
	}
	once.Do(func() { close(release) })
	if err := <-published; err != nil {
		t.Fatal(err)
	}
	for _, call := range *calls {
		if strings.HasPrefix(call, "detach ") || strings.HasPrefix(call, "clone ") {
			t.Fatalf("restore changed shared snapshot: %s", call)
		}
	}
}

func TestHybridRestoreMaterializesAndSealsMissingLocal(t *testing.T) {
	s, calls := newFakeHybridStore(t)
	base := s.run
	s.rememberSealed("tpl-snap-peer-memory") // stale cache from a removed local overlay
	attached := false
	s.run = func(ctx context.Context, path string, args ...string) ([]byte, error) {
		if args[0] == "devices" {
			if attached {
				return []byte("nbd7 size 8388608 plugin cubestor name tpl-snap-peer-memory"), nil
			}
			return nil, nil
		}
		if args[0] == "attach" {
			attached = true
		}
		if args[0] == "seal" && args[1] == "--dir" {
			*calls = append(*calls, strings.Join(args, " "))
			return nil, errors.New("object is in use")
		}
		return base(ctx, path, args...)
	}
	if _, err := s.ResolveSnapshotMemory(context.Background(), "tpl-snap-peer-memory", cowKindSnapshot); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(*calls, "\n")
	if strings.Contains(joined, "detach ") || strings.Count(joined, "attach --unix") != 1 || !strings.Contains(joined, "seal --unix") {
		t.Fatalf("peer restore must finalize online and retain its attachment: %s", joined)
	}
}
