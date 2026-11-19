# Cube XFS + JFS 分层存储方案

## 1. 目标与范围

本方案通过 `nbdkit cubestor 插件 + cubestorcli` 向 Cube 提供统一的块设备接口：

- XFS 承担沙箱运行时的主要读写，降低直接访问 JuiceFS（JFS）的延迟。
- JFS 保存共享、可跨节点恢复的模板和快照 generation。
- rootfs 从模板制作开始就是 ext4 块设备，不考虑兼容或导入已有 ext4/raw 模板。
- memory 使用 raw 块设备，不能格式化为文件系统。
- `/dev/nbdX` 只是节点上的临时设备，不作为卷身份持久化。

## 2. 总体架构

### 2.1 前端

- CubeMaster
  - 管理模板、快照及其 generation 状态。
  - 根据 `LOCAL_READY`、`REMOTE_READY` 决定能否在本机或跨节点恢复。
  - 调度时只传递 volume ID、generation 和 manifest，不传递 NBD 设备号。
- Cubelet
  - 调用 cubestorcli 创建、克隆、attach、detach、发布和删除卷。
  - 将 cubestorcli 返回的 `/dev/nbdX` 交给 hypervisor。
  - 编排模板制作、启动、暂停、恢复、快照和销毁流程。
- Hypervisor
  - rootfs：将 ext4 NBD 设备作为虚拟机块设备。
  - memory：将 raw NBD 设备作为 `MAP_PRIVATE` 的内存 backing。

### 2.2 后端

- nbdkit
  - 对外提供标准 NBD 块设备。
  - 按页查询本地 page map。
  - 本地命中时访问 XFS；未命中时从 JFS 读取并回填 XFS。
- XFS local
  - 保存从 JFS 回填的 clean cache。
  - 保存尚未发布到 JFS 的 dirty overlay。
  - dirty overlay 在远端提交前是唯一数据副本，不能当作缓存淘汰。
- JFS shared
  - 所有节点挂载同一个共享目录。
  - 保存不可变的 rootfs/memory generation、manifest 和 commit marker。
  - 只有完成校验并提交的 generation 才能用于跨节点恢复。

节点目录固定为：

```text
/data/cubelet/storage/hybrid/
├── local/   # 节点本地 XFS：clean cache + dirty overlay
└── remote/  # JuiceFS 挂载点：共享 baseline 与已提交 generation
```

`remote/` 本身就是 JuiceFS mount point，不再使用
`/data/cubelet/storage/jfs/hybrid` 这样的二级目录。

## 3. 卷模型

每个模板或快照包含两个逻辑卷：

| 卷 | 块设备内容 | 运行时用途 | 运行时写入位置 |
| --- | --- | --- | --- |
| rootfs | ext4 文件系统 | 虚拟机系统盘 | XFS dirty overlay |
| memory | raw 内存镜像 | hypervisor 内存 backing | 匿名 CoW 页；快照时写入新卷 |

卷的持久身份由以下信息组成：

- volume ID
- generation
- logical size
- block/page size
- local/shared UUID
- checksum
- 状态与 commit marker

NBD 设备号只存在于当前节点的运行时映射表中。

## 4. 模板制作

### 4.1 rootfs 模板

不先生成普通 ext4 镜像文件，而是直接在 cubestorcli 提供的逻辑块设备上制作：

1. Cubelet 调用 cubestorcli 创建 rootfs volume。
2. cubestorcli 创建 local/shared pair，nbdkit cubestor 插件提供对应 export。
3. cubestorcli attach export，返回 `/dev/nbdX`。
4. 对 `/dev/nbdX` 执行 `mkfs.ext4`。
5. 将 `/dev/nbdX` 挂载到临时目录。
6. 在挂载目录中安装或复制模板 rootfs 内容。
7. 卸载文件系统，必要时执行 ext4 一致性检查。
8. 对 NBD export 执行 flush。
9. cubestorcli 将 generation 发布到 JFS，校验完成后写入 commit marker。
10. CubeMaster 将模板 rootfs 标记为 `REMOTE_READY`。

只能格式化 `/dev/nbdX`。不能直接对 JFS 中的 cubestor 底层文件执行 `mkfs.ext4`，否则会覆盖 cubestor header、page map 等元数据。

### 4.2 memory 模板

memory 卷必须保持为 raw 块设备：

1. Cubelet 调用 cubestorcli 创建指定大小的 memory volume。
2. attach 对应的 `/dev/nbdX`。
3. Hypervisor 或模板制作程序将完整内存镜像写入该设备。
4. 执行 flush，并发布到 JFS。
5. 校验并提交后，将 memory generation 标记为 `REMOTE_READY`。

rootfs 和 memory 必须作为同一模板 generation 对发布。只有两者及 manifest 均为 `REMOTE_READY`，模板才可被调度。

## 5. 从模板启动沙箱

1. CubeMaster 选择一个 `REMOTE_READY` 模板 generation 并调度节点。
2. Cubelet 为 rootfs 创建 writable child generation。
3. Cubelet 为 memory 准备本地 overlay，其 shared baseline 指向模板 memory generation。
4. cubestorcli 在本地 XFS 创建或复用匹配 UUID 的 page map 和数据文件。
5. cubestorcli 分别 attach rootfs 和 memory export，返回两个临时 NBD 设备。
6. Hypervisor：
   - 将 rootfs NBD 设备作为系统盘使用；
   - 对 memory NBD 设备建立 `MAP_PRIVATE` 映射并恢复虚拟机内存。
7. 冷数据首次读取时由 nbdkit 从 JFS 读取，并回填本地 XFS。
8. 后续读取优先命中 NBD page cache 或 XFS。

rootfs 写请求进入 XFS dirty overlay。memory 的运行期写请求通过 `MAP_PRIVATE` 进入匿名 CoW 页，不直接修改模板 memory generation。

## 6. 暂停与恢复

### 6.1 暂停

暂停仅停止沙箱执行，不自动生成新的远端快照：

1. Cubelet 暂停 vCPU。
2. 排空 rootfs 块设备的在途请求。
3. 对 rootfs NBD 执行 flush，保证本地 overlay 可恢复。
4. memory 的匿名页和现有 NBD mapping 保持不变。
5. 沙箱进入 paused 状态。

### 6.2 原地恢复

1. 检查原 NBD mapping 是否仍属于对应 volume/generation。
2. 如果映射仍有效，直接恢复 vCPU。
3. 如果映射失效，重新连接相同 generation，并重建 hypervisor 所需映射。
4. 恢复 vCPU。

不能只根据 `/dev/nbdX` 是否存在判断映射有效，必须校验 export、volume ID 和 generation。

## 7. 制作快照

快照由 rootfs、memory、VM state 和 manifest 组成：

1. 暂停 vCPU。
2. 排空 rootfs I/O，并执行 flush。
3. 冻结当前 generation，防止 clone 期间继续修改。
4. 为 rootfs 克隆新的 snapshot generation。
5. 以内存模板/上一个快照为 baseline，克隆新的 memory snapshot generation。
6. Hypervisor 将本次运行产生的 dirty memory pages 写入新 memory NBD 卷。
7. 对 rootfs 和 memory snapshot export 执行 flush。
8. 保存 VM state 和本地 manifest。
9. 所有本地数据持久化后，将快照标记为 `LOCAL_READY`，快照 API 可以返回。
10. 后台将 rootfs、memory、VM state 和 manifest 发布到 JFS。
11. 校验完成并写入 commit marker 后，标记为 `REMOTE_READY`。

`LOCAL_READY` 只保证源节点可以恢复；只有 `REMOTE_READY` 才允许跨节点恢复。

如果要求 ext4 文件系统级一致性，应在快照前由 guest 执行 freeze；仅暂停设备 I/O 得到的是崩溃一致性快照。

## 8. 从快照启动

### 8.1 同节点启动

- `LOCAL_READY` 或 `REMOTE_READY` 均可使用。
- 优先复用本地 XFS 中已有的 generation 和 clean cache。
- 为 rootfs 创建新的 writable child，避免修改快照。
- memory snapshot 作为 `MAP_PRIVATE` backing。

### 8.2 跨节点启动

1. CubeMaster 只选择 `REMOTE_READY` 快照。
2. 目标 Cubelet 校验远端 commit marker 和 manifest。
3. 在目标节点 XFS 创建空 local overlay。
4. 获取新的 writer lease 和 fencing epoch。
5. 连接新的 rootfs/memory NBD 设备。
6. Hypervisor恢复 VM state 和 memory。
7. 工作集按需从 JFS 回源并回填目标节点 XFS。
8. 健康检查通过后更新沙箱节点绑定。

旧节点即使重新上线，也不能继续使用旧 lease 写入同一 generation。

## 9. 销毁与垃圾回收

销毁沙箱时：

1. 停止 hypervisor。
2. 解除 memory mmap。
3. 断开 rootfs/memory NBD 设备。
4. 关闭对应 export。
5. 清理运行时设备映射。

本地数据按以下规则处理：

- 从 JFS 回填的 clean cache 可以保留，也可以按 LRU 淘汰。
- 已经 `REMOTE_READY` 的 local generation 可以作为缓存回收。
- 尚未发布的 dirty overlay 必须 pin，不能删除。
- 删除远端 generation 前，必须确认不存在 active lease、mmap pin、上传任务、模板/快照引用和后代 generation。

## 10. 状态机

```text
CREATING
   |
   v
ACTIVE_LOCAL
   |
   v
SEALED_LOCAL / LOCAL_READY
   |
   v
PUBLISHING
   |
   v
REMOTE_READY
```

- `ACTIVE_LOCAL`：当前沙箱持有 writer lease。
- `LOCAL_READY`：本地 generation 已封存，源节点可恢复。
- `PUBLISHING`：正在上传和校验，dirty overlay 不可删除。
- `REMOTE_READY`：远端 generation 已原子提交，可以跨节点恢复。

状态只能单向推进。失败的发布不能覆盖上一个 `REMOTE_READY` generation。

## 11. 一致性要求

- nbdkit 必须正确实现 NBD `FLUSH` 和 FUA 语义。
- 本地持久化顺序至少保证 data、page map、header 和目录项均可恢复。
- clone 源必须被冻结或受稳定 lease 保护。
- 远端 generation 使用临时名称写入，校验后通过 commit marker 原子发布。
- manifest 必须同时引用匹配的 rootfs、memory 和 VM state。
- local/shared UUID 不匹配时禁止复用本地缓存。
- clean cache 可丢；未发布 dirty overlay 不可丢。

## 12. 实现与验证重点

- rootfs volume 创建、ext4 格式化、挂载和模板填充。
- ext4 文件系统扩容需要同时支持块设备 resize 和 `resize2fs`。
- memory NBD 的 `MAP_PRIVATE` 缺页、readahead 和大内存性能。
- nbdkit/cubestorcli 断链、重启、NBD timeout 后的错误处理。
- FLUSH/FUA、节点重启和上传中断的崩溃一致性。
- generation checksum、WAL 或等价的原子元数据更新机制。
- writer lease、fencing、并发 attach/clone/delete。
- JFS 未挂载时必须拒绝创建或发布，避免误写本地挂载点目录。
- 对比纯 JFS、XFS+JFS hybrid 和纯 XFS 的启动延迟、page-fault p99/p999、运行时 IOPS、快照耗时和恢复时间。
