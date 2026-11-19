// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// The worker lifetime, socket and export identify a binding. A nonzero device
// size alone cannot distinguish an NBD slot reused for a different object.
type hybridDeviceIdentity struct {
	PID    string
	Start  string
	Socket string
	Name   string
}

func (s *HybridStore) deviceIdentity(device string) (hybridDeviceIdentity, error) {
	if s.readDevice != nil {
		return s.readDevice(device)
	}
	return readHybridDeviceIdentity(os.ReadFile, device)
}

func readHybridDeviceIdentity(read func(string) ([]byte, error), device string) (hybridDeviceIdentity, error) {
	var id hybridDeviceIdentity
	base := filepath.Base(device)
	if filepath.Dir(device) != "/dev" || !strings.HasPrefix(base, "nbd") {
		return id, fmt.Errorf("invalid NBD device %q", device)
	}
	if _, err := strconv.ParseUint(strings.TrimPrefix(base, "nbd"), 10, 32); err != nil {
		return id, err
	}
	sys := filepath.Join("/sys/block", base)
	size, err := read(filepath.Join(sys, "size"))
	if err != nil {
		return id, err
	}
	sectors, err := strconv.ParseUint(strings.TrimSpace(string(size)), 10, 64)
	if err != nil || sectors == 0 {
		return id, fmt.Errorf("NBD device %s is not attached", device)
	}
	pid, err := read(filepath.Join(sys, "pid"))
	if err != nil {
		return id, err
	}
	id.PID = strings.TrimSpace(string(pid))
	if n, err := strconv.ParseUint(id.PID, 10, 32); err != nil || n == 0 {
		return id, fmt.Errorf("invalid NBD worker PID")
	}
	proc := filepath.Join("/proc", id.PID)
	stat, err := read(filepath.Join(proc, "stat"))
	if err != nil {
		return id, err
	}
	// comm (field 2) can itself contain spaces and parentheses.
	end := strings.LastIndexByte(string(stat), ')')
	if end < 0 {
		return id, fmt.Errorf("invalid worker stat")
	}
	fields := strings.Fields(string(stat[end+1:]))
	if len(fields) < 20 {
		return id, fmt.Errorf("short worker stat")
	}
	id.Start = fields[19] // field 22, starttime
	cmd, err := read(filepath.Join(proc, "cmdline"))
	if err != nil {
		return id, err
	}
	args := strings.Split(strings.TrimRight(string(cmd), "\x00"), "\x00")
	if len(args) == 0 || filepath.Base(args[0]) != "cubestorcli" {
		return id, fmt.Errorf("unmanaged NBD worker")
	}
	for i := 1; i < len(args); i++ {
		if args[i] == "--unix" && i+1 < len(args) {
			i++
			id.Socket = args[i]
		} else if args[i] == "--name" && i+1 < len(args) {
			i++
			id.Name = args[i]
		}
	}
	again, err := read(filepath.Join(sys, "pid"))
	if err != nil || strings.TrimSpace(string(again)) != id.PID || id.Socket == "" || id.Name == "" {
		return id, fmt.Errorf("NBD worker identity changed or incomplete")
	}
	return id, nil
}

func (s *HybridStore) detachDevice(ctx context.Context, name, device string) ([]byte, error) {
	return s.command(ctx, "detach", "--device", device, "--expected-name", name,
		"--unix", s.socketPath, "--local-dir", s.localDir)
}
