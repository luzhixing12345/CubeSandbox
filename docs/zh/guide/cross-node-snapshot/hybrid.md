# Hybrid（本地 XFS + 远端 JuiceFS）存储部署

Hybrid 后端把沙箱运行时读写放在本地 XFS，并把 JuiceFS 作为共享远端层。
它与原有的纯 JuiceFS 后端相互独立：

```text
/data/cubelet/storage/hybrid/
├── local/     # 本机 XFS 数据
└── remote/    # Hybrid 专用 JuiceFS 挂载点

/data/cubelet/storage/jfs/   # 原有纯 JuiceFS 后端，不受影响
```

本文适用于 one-click 安装包。当前 Hybrid 已在 c1/c2/c3 三节点集群上验证模板
制作、从模板启动、暂停、恢复、快照和从快照启动。`REMOTE_READY` 的跨节点
发布状态机与故障注入验收仍未完成，不应把本文步骤视为生产级跨节点可靠恢复
能力已经就绪。

## 数据布局与发布语义

Hybrid 把一份数据分成两半，理解这一点是排查问题的前提：

```text
<shared_dir>/                     # 共享远端（JuiceFS）
├── <name>                        # 卷镜像基线（rootfs / memory）
└── hybrid/
    ├── snapshots/<id>/           # 模板与快照的元数据、guest 规格、VM 状态
    └── pause-snapshots/<id>/     # 暂停包
<local_dir>/<name>                # 本机 XFS 的 overlay page map
```

两点必须记住：

1. **目录元数据在共享远端。** `snapshots/`、`pause-snapshots/` 与卷镜像同处
   `shared_dir`，因此任一节点都能解析同一份模板；若把它放在本机 XFS，模板
   只会存在于构建它那台机器上。
2. **本地 overlay 必须显式发布。** cubestor 的写先落在 `local_dir` 的 page
   map 里，只有 `cubestorcli sync` 才会把脏页合并进共享镜像。Cubelet 在
   模板 rootfs 提交、模板 memory 关闭和暂停包克隆时都会执行这一步；漏掉它
   的表现是远端镜像只有 2 MiB 头部，恢复出的 guest 内存全零并立即 reset。
3. 模板只在**单节点**构建。Hybrid 的构建产物落在共享目录，多节点并行构建会
   在同一目录抢建同名 `tpl-<id>-build-rootfs` 并报 `already exists`；
   CubeMaster 因此把 Hybrid 模板固定到一个可调度节点上构建。


## 构建安装包

构建机需要同时存在 CubeSandbox 和 nbdkit 仓库。默认目录为：

```text
/data/CubeSandbox
/data/nbdkit
```

先在 nbdkit 仓库编译并确认以下文件存在：

```text
build/server/nbdkit
build/plugins/cubestor/cubestorcli
build/plugins/cubestor/.libs/nbdkit-cubestor-plugin.so
```

然后构建 one-click 安装包：

```bash
cd /data/CubeSandbox
./deploy/one-click/build-release-bundle-builder.sh
```

如果 nbdkit 仓库不在默认位置，可设置 `NBDKIT_REPO_DIR`。也可分别通过
`NBDKIT_BIN_SRC`、`CUBESTORCLI_BIN_SRC` 和 `NBDKIT_CUBESTOR_PLUGIN_SRC`
指定三个产物；变量可以写在 `deploy/one-click/build.env` 中。

默认值按 CubeSandbox 仓库的**物理路径**推导（`cd -P` 解析符号链接后再取上级
目录）。因此即使通过软链接进入仓库，例如
`/root/CubeSandbox -> /data/CubeSandbox`，也会找到 `/data/nbdkit`；如果
nbdkit 放在其它位置，用 `NBDKIT_REPO_DIR` 显式指定即可，否则 `bd` 会以
`required file not found: .../build/server/nbdkit` 失败。

安装包内应包含：

```text
hybrid/bin/nbdkit
hybrid/bin/cubestorcli
hybrid/lib/nbdkit-cubestor-plugin.so
scripts/cube-hybrid/
```

这些程序依赖目标机的动态库。部署前可执行 `ldd` 检查；出现 `not found`
时必须先安装对应动态库。临时构建产物还可能依赖构建环境提供的
`libtdsp.so` 和 `libonion.so`，因此应使用与构建机兼容的目标机。

## 安装与配置

在安装包的 `.env` 中设置：

```bash
ONE_CLICK_ENABLE_HYBRID=1
CUBE_HYBRID_META_PASSWORD="请修改为独立密码"
```

控制节点在使用 one-click 自带 MinIO 时会自动生成 Hybrid bucket，并以
控制节点 IP 推导 `redis://<控制节点IP>:6381/0`。使用外部服务时显式配置：

```bash
CUBE_HYBRID_META_URL="redis://redis.example.com:6379/0"
CUBE_HYBRID_BUCKET="https://object.example.com/cube-hybrid"
CUBE_HYBRID_ACCESS_KEY="..."
CUBE_HYBRID_SECRET_KEY="..."
```

计算节点必须配置相同的 metadata、bucket 和凭据。不要让 Hybrid 与纯 JFS
共用 metadata DB 或 volume 名称。

执行安装：

```bash
sudo ./install.sh
```

安装器把 `cubestorcli`、`nbdkit` 和 cubestor 插件分别安装到：

```text
/usr/local/bin/cubestorcli
/usr/local/bin/nbdkit
/usr/local/lib/cube-sandbox/nbdkit-cubestor-plugin.so
```

安装本身不会自动格式化或挂载 Hybrid。

## 初始化与挂载

只在控制节点初始化一次 Hybrid 专用 JuiceFS volume：

```bash
sudo /usr/local/services/cubetoolbox/scripts/cube-hybrid/init.sh
```

然后在控制节点和每台计算节点执行：

```bash
sudo /usr/local/services/cubetoolbox/scripts/cube-hybrid/mount.sh
```

该脚本会：

1. 把 Hybrid 专用 JuiceFS 挂载到
   `/data/cubelet/storage/hybrid/remote`；
2. 检查 `/data/cubelet/storage/hybrid/local` 所在文件系统是 XFS；
3. 启动 nbdkit cubestor 插件并创建 `/run/cubestor.sock`；
4. 只修改 Cubelet 的 `[cow.hybrid]` 配置并重启 Cubelet。

它不会修改 `[cow.jfs]`，原有纯 JFS 挂载点仍是
`/data/cubelet/storage/jfs`。

## 检查

每个节点都可执行：

```bash
sudo /usr/local/services/cubetoolbox/scripts/cube-hybrid/check.sh
```

检查会验证 systemd、JuiceFS、XFS、Unix socket，并创建一个临时 4 MiB
卷完成 NBD 写入、读取、flush 和 detach 测试，结束后删除临时数据。

也可手工查看：

```bash
systemctl status cube-sandbox-hybrid.service
findmnt /data/cubelet/storage/hybrid/remote
findmnt /data/cubelet/storage/hybrid/local
test -S /run/cubestor.sock
/usr/local/bin/cubestorcli devices
docker logs cube-sandbox-hybrid-jfs
```

## 使用 Hybrid 后端

创建模板时指定 Hybrid：

```bash
cubemastercli tpl create-from-image \
  --name <模板名> \
  --image <镜像> \
  --backend hybrid
```

后续沙箱启动、暂停、恢复和快照操作沿用 Cube 原有 API。已用下列命令在三节点
集群上完成端到端验证：

```bash
# 从模板启动
curl -s -X POST http://127.0.0.1:3000/sandboxes \
  -H 'Content-Type: application/json' \
  -d '{"templateID":"tpl-<id>","timeout":300}'

# 暂停 / 恢复
curl -s -X POST http://127.0.0.1:3000/sandboxes/<sandbox-id>/pause
curl -s -X POST http://127.0.0.1:3000/sandboxes/<sandbox-id>/resume

# 快照，再用快照 ID 作为 templateID 启动新沙箱
curl -s -X POST http://127.0.0.1:3000/sandboxes/<sandbox-id>/snapshots
```

验证要点：模板的 `remote/tpl-<id>-{rootfs,memory}` 应与本地 overlay 同等大小
（而不是 2 MiB）；恢复出的沙箱 `state` 应保持 `running`。

## 常见问题

| 现象 | 原因与处理 |
| --- | --- |
| `The specified bucket does not exist` | MinIO 缺桶。`minio-postcheck.sh` 会用镜像内置的 `mc` 幂等创建 `CUBE_S3_BUCKET`；确认 `cube-volumes` 存在，或手工 `mc mb`。 |
| `cubestorcli ... already exists` | 共享目录里残留同名 `build-rootfs`，或多个节点并行构建。Cubelet 会在构建前清理该临时名，CubeMaster 已限制 Hybrid 单节点构建。 |
| `hybrid storage is not configured` | 升级覆盖了 `config.toml`。`cubelet-start.sh` 每次启动都会按 `.one-click.env` 重写 `[cow.hybrid]`，确认 `ONE_CLICK_ENABLE_HYBRID=1` 后重启 Cubelet。 |
| `template ... is not available locally` | 模板元数据不在共享目录。确认 `snapshots/` 位于 `shared_dir/hybrid`，且节点已挂载同一 remote。 |
| guest 启动即 `VmExit::Reset` | 本地 overlay 未发布，远端 memory 只有 2 MiB 头部。检查 `cubestorcli sync` 是否在提交/关闭时执行，并核对远端文件大小。 |


## 停用

先删除或迁移本节点全部 Hybrid 沙箱，再执行：

```bash
sudo /usr/local/services/cubetoolbox/scripts/cube-hybrid/mount.sh unmount
```

停用会关闭 Cubelet 的 Hybrid 开关、停止 nbdkit 和 Hybrid JuiceFS 挂载，
但不会删除 local、remote、Redis 或对象存储中的数据。

## 并发与性能诊断

Cubelet 记录 NBD worker 的 PID、进程启动时间、socket 和 export 名称。
设备缓存命中时直接校验对应 sysfs/proc 信息，避免每次启动 CLI 枚举所有设备；
并发缓存缺失共用一次正在进行的发现，不缓存“未挂载”结果。
NBD 编号和 PID 都可能复用，不能仅凭设备存在或容量非零复用绑定。

Cubelet 的 detach 使用 `--expected-name`、`--unix`、`--local-dir`，
因此更新 Cubelet 时需要同步更新支持这些参数的 cubestorcli。
CLI 在 flush/disconnect 前校验 socket/export，并等待原 local inode 的锁释放。
同名 export 的其他连接通过 NBD worker 直接识别，不再反复扫描所有进程的 fd。

已存在本地 overlay 的 canonical 模板内存（`tpl-tpl-*-memory`）允许最多四个并发克隆：
源对象持共享锁，目标持独占锁；完成捕获的 `tpl-snap-*` 快照源也使用共享锁。
活跃沙箱和其他来源保留独占源锁及原有 freeze/flush 协议。
nbdkit 的全局缓存锁只保护镜像索引和同名打开/关闭状态；文件 I/O 和 flock
在全局锁外执行。不同镜像可以同时打开，同名镜像需等待前一次打开或关闭完成。

需要排查 CLI 耗时时，可给 Cubelet 设置 `CUBE_HYBRID_COMMAND_TRACE=/tmp/hybrid-commands.log`。
每次 CLI 调用记录时间、耗时（微秒）、是否失败和参数；该开关默认关闭。
它只统计命令耗时，不包含命令前等待对象锁的时间。结合 `CUBE_COMMIT_STAGE_TRACE`
和暂停阶段计时可以区分命令耗时与其他等待。

重启 Cubelet 后应等待 9999 端口就绪，并确认依赖它的 `cube-sandbox-cube-egress.service`
及 `cube-sandbox-cube-egress-net.service` 正常运行，再进行生命周期基准。

Hybrid 在运行时快照制作的同步收尾阶段调用
`cubestorcli flush --to-page-cache --unix SOCKET --device DEVICE --name NAME`。
此命令先为目标镜像建立暂缓持久化状态，再排空内核 NBD 写回，冻结镜像写入并保留只读设备。
数据已进入本地文件 page cache，最新页表仍由活跃插件持有；此时本节点可立即恢复，
但尚无进程/机器故障后的持久化保证。第一阶段不对 overlay 执行 fdatasync。

后台先执行 `cubestorcli flush --sync-page-cache --unix SOCKET --name NAME`，
按数据、页表/头部的顺序持久化，再完成 durable seal，最后执行 `sync --immutable` 上传。
普通 `flush` 的完整持久化语义不变；暂缓 flush 仅发生在显式第一阶段的写回窗口内。
本地 `.pending/NAME` 标记在第一阶段单独持久化，第二阶段完成后移除并持久化目录。
插件失去内存状态后拒绝重新打开带此标记的对象，CLI 也拒绝直接克隆/上传它。
持久化失败保留 pending 状态，可通过仍活跃的插件重试；删除会清理标记。

保留原 NBD 设备及其页缓存，恢复直接复用，不创建私有内存克隆。
正常恢复不再 seal；后台在上传前完成第二阶段。若后续快照需要克隆尚未持久化的内存源，
先完成源的第二阶段，以确保 CLI 看到完整的文件页表。
恢复不等待远端上传。nbdkit 对 sealed 镜像持共享文件锁，拒绝写入且禁用读取缓存回填。
`sync --immutable` 持共享 local 锁，合并到 shared 临时文件后原子替换，不改动 local 的
页表或头部。仅此模式保留 shared UUID，因为整个快照的逻辑内容已经固定；本机已有读取者
继续使用旧 shared inode 加同一 sealed local overlay，新读取者使用发布后的 shared，逻辑内容一致。
跨节点恢复仍必须遵守 Master 的 `remote_status=ready` 门禁；未发布的 shared 不能作为完整镜像使用。
shared 头部的 synced 标志和来源 local UUID 记录发布完成，重试不会重复复制。
克隆时清除 sealed 标志，目标仍可写。普通可写镜像仍使用要求脱离挂载的原有 sync。

发布与恢复/快照源克隆共用对象读锁；删除、捕获收尾等操作仍持写锁。
恢复设备分配另用同名互斥锁，避免并发恢复重复 attach。`seal` 只在捕获完成后调用，
不能用于仍在写入的设备。进程重启后通过头部 sealed 标志恢复状态；旧快照或本地缓存缺失时，
保留一次兼容转换，已挂载的完成态快照在线 seal，无需断开再挂载。
此功能需要配套升级 cubestorcli 与 nbdkit plugin，不能仅升级 Cubelet。

### 快照提交的快速路径与 flush 诊断

Hybrid 的快照 clone 由 CLI 原子拒绝已存在的目标，提交入口不再逐个预检查
目标对象。新版 CLI 在 clone 成功时返回 `size <virtual_size>`，Cubelet 直接
复用该尺寸；兼容旧 CLI 时才补一次 size 查询。本进程成功创建且尚未尝试
attach 的对象使用创建记录跳过设备枚举；开始 attach 后清除此记录，失败时
仍可重新发现设备。该记录不跨进程重启，也不把普通发现的空结果缓存起来。

内存设备在快照返回前完成 `--to-page-cache`；后台 `--sync-page-cache` 保留
“数据先于映射落盘”的顺序，将映射表和 header 合并到第二次 fdatasync，
持久化完成后才允许复用被映射移除的块。普通 flush 同样保留这些屏障。

需要诊断时，在 nbdkit 和 Cubelet 服务环境中设置
`CUBESTOR_FLUSH_TRACE=/tmp/cubestor-flush-trace.log`。CLI 记录设备 fsync 的
总耗时；插件记录锁等待、数据同步、映射写入、header 写入、元数据同步及
插件总耗时。记录包含实时时间戳，耗时使用单调时钟、单位微秒；插件总耗时
包含内部子阶段，不能重复相加。此功能默认关闭，诊断后移除环境变量。
