# WASM 终端检测插件沙箱

面向供应商交付的 **WASI Preview 1** 终端检测插件试跑服务：上传一个模块和一份脱敏 JSON 日志，服务在独立 runner 子进程中运行模块，把日志原样送入 stdin，并在模块正常退出后返回它写到 stdout 的 JSON。

服务不会把宿主目录、宿主环境变量、网络接口、额外文件描述符或服务器文件传给插件。插件只能看到 WASI 的 stdin/stdout/stderr；线性内存上限 64 MiB。

## 进程模型（为什么运维暂停/杀死 API 也能管住插件）

容器内有两个职责分离的进程，故障演练只作用于其中的 **API 工作进程**，容器本身和监督进程始终保持运行：

```text
容器 (只读根文件系统 + tmpfs)
└─ PID 1  app.supervisor   监督进程：拥有所有 runner，负责拉起/重启 API
   ├─ uvicorn app.main:app 单个 API 工作进程（上传接口，无 runner 子进程）
   └─ python -I app/runner.py …  每个请求一个 runner，各自独立进程组与暂存目录
```

* **API 不再是 runner 的父进程**。runner 由 PID 1 监督进程 fork，因此 API 被 `SIGSTOP` 或 `SIGKILL` 都不会让 runner 变成无人回收的孤儿进程。
* API 与监督进程之间只通过 tmpfs 上的 **Unix 域套接字**（换行分隔的 JSON 帧）通信；每次试跑用随机 id 标识，暂存目录名也唯一。
* 2 秒墙钟上限由监督进程基于单调时钟执行，**API 被暂停期间计时仍然有效**。
* 监督进程检测到 API 死亡后，立即对全部在途 runner 发 `SIGKILL`、`waitpid` 回收，并删除各自暂存目录，无需等待 API 重启；随后自动重启 API。
* 重启后的新 API 进程持有全新的客户端状态，既无法触碰旧 runner，也不会误删新请求的暂存文件。

## 快速启动

```bash
docker compose up --build
```

服务启动后：

* API 文档与上传界面：<http://localhost:8000/docs>
* 健康检查：<http://localhost:8000/healthz>

在 `/docs` 中展开 `POST /api/v1/analyze`，点击 **Try it out**：

1. `module` 选择仓库中的 `demo/classify.wasm`；
2. `log` 选择仓库中的 `demo/sample-log.json`；
3. Execute，应返回：

```json
{"verdict":"allow","score":0,"reasons":["no malicious marker found"]}
```

也可以直接用 curl：

```bash
curl -sS \
  -F 'module=@demo/classify.wasm;type=application/wasm' \
  -F 'log=@demo/sample-log.json;type=application/json' \
  http://localhost:8000/api/v1/analyze
```

## 故障演练（只暂停/杀死 API 进程，容器保持运行）

先在一个终端启动服务：

```bash
docker compose up --build
```

另开一个终端，用仓库自带脚本演练。脚本只对容器内的 API 工作进程发信号，从不重启容器。

### 1) 暂停 API（SIGSTOP）

```bash
./demo/fault-drill.sh pause
```

脚本会：`SIGSTOP` 暂停 API → 上传死循环插件 `demo/loop.wasm` → 展示 API 冻结期间监督进程仍在 2 秒内终止 runner 并清空暂存 → `SIGCONT` 恢复 API → 再次发起同一调用，返回 `504 plugin_timeout`，**不返回任何部分结果**。

手动等价操作：

```bash
# 找到容器内 API 工作进程（其父进程为 1，命令行含 uvicorn）
docker compose exec wasm-plugin-sandbox ps -eo pid,ppid,args
docker compose exec wasm-plugin-sandbox kill -STOP <api_pid>
# （此时发起死循环插件调用；监督进程照常计时）
docker compose exec wasm-plugin-sandbox kill -CONT <api_pid>
```

### 2) 杀死 API（SIGKILL）

```bash
./demo/fault-drill.sh kill
```

脚本会：并发发起三个死循环试跑 → `SIGKILL` 杀死 API → 1 秒内确认全部 runner 被终止回收、暂存目录被清空（不等待 API 重启）→ 等监督进程自动重启 API → 用一次正常分类验证重启后可继续试跑，且旧清理没有影响新调用。

手动等价操作：

```bash
docker compose exec wasm-plugin-sandbox kill -KILL <api_pid>
# 观察：runner 消失、/tmp/wasm-sandbox/runs 清空、API 被自动重新拉起
./demo/fault-drill.sh status
```

### 3) 恢复与重启的区别

* **暂停后恢复**：同一个 API 进程继续运行，被冻结的调用在恢复后得到超时响应，随后可正常试跑。
* **杀死后重启**：监督进程拉起一个全新 API 进程（新 pid），新请求与故障前的 runner/暂存完全隔离。
* 两种场景都不需要 `docker restart`；容器与 PID 1 监督进程全程在线。

## 演示模块

| 文件 | 用途 | 预期结果 |
| --- | --- | --- |
| `demo/classify.wasm` | 读取 stdin，在日志中匹配 `malicious` / `suspicious` 标记后输出分类 JSON | `200` + JSON |
| `demo/loop.wasm` | `_start` 无限循环（故障演练用） | `504 plugin_timeout` |
| `demo/spam.wasm` | 持续写 stdout，永不退出 | `413 plugin_output_too_large` |
| `demo/sample-log.json` | 脱敏正常日志 | `allow` |
| `demo/suspicious-log.json` | 脱敏可疑日志 | `review` |
| `demo/malicious-log.json` | 脱敏恶意标记日志 | `block` |
| `demo/fault-drill.sh` | 暂停 / 杀死 / 恢复 / 重启 API 的本地操作脚本 | — |

`classify.wasm` 只是无依赖的演示插件，用字节子串匹配模拟分类；真实插件可以自行解析 stdin 中的 JSON。`.wat` 源码一并保留，修改后可重新生成：

```bash
python demo/build_demo.py
```

（需要本地已安装 `requirements.txt` 中的 `wasmtime`。）

## API

### `POST /api/v1/analyze`

`multipart/form-data` 两个必填字段：

* `module`：二进制 WebAssembly 1.0 / WASI Preview 1 模块，最大 **1 MiB**
* `log`：合法 UTF-8 JSON，最大 **64 KiB**，原样写入插件 stdin

成功时返回插件 stdout 中的完整 JSON 值（状态码 `200`，`application/json`）。只有插件正常退出、stdout 是唯一一个合法 JSON 值，且总输出未超限时才会返回；否则不返回任何部分结果。请求格式与改造前完全一致。

### 控制面帧（API ↔ 监督进程，内部接口）

基于文件系统作用域的 Unix 套接字，换行分隔 JSON：`submit`（base64 模块+日志）→ `submitted{id}` → 轮询 `poll{id}` → `result{state,...}`，另有 `cancel` 与 `ping`。套接字与暂存目录都在 tmpfs 的 `/tmp/wasm-sandbox` 下，权限 `0600/0700`，属主为非特权 `sandbox` 用户。

### 错误格式

```json
{
  "error": {
    "code": "plugin_timeout",
    "message": "Plugin execution exceeded the 2 second limit."
  }
}
```

| HTTP | `code` | 说明 |
| --- | --- | --- |
| `413` | `module_too_large` / `log_too_large` | 上传文件超过上限 |
| `413` | `request_too_large` | multipart 请求体超过服务上限 |
| `413` | `plugin_output_too_large` | 插件 stdout + stderr 合计超过 64 KiB |
| `422` | `invalid_module` | 不是合法 WASI Preview 1 二进制模块、缺少合法 `_start` 等 |
| `422` | `plugin_instantiation_failed` | 链接/实例化失败 |
| `422` | `plugin_trap` | 运行中触发 Wasm 陷阱 |
| `422` | `plugin_nonzero_exit` | 插件通过 `proc_exit` 返回非零状态 |
| `422` | `log_not_utf8` / `invalid_log_json` | 日志不是 UTF-8 或不是单个合法 JSON 值 |
| `422` | `invalid_plugin_json` | 插件 stdout 不是单个合法 UTF-8 JSON 值 |
| `499` | `client_disconnected` | 客户端断开，插件已终止并回收 |
| `503` | `sandbox_unavailable` / `sandbox_interrupted` | 控制面不可用，或监督进程在 API 不可用时终止了该试跑 |
| `504` | `plugin_timeout` | runner 启动后超过 2 秒（API 被暂停时依旧生效） |
| `500` / `502` | `sandbox_internal_error` / `sandbox_process_failed` | 沙箱内部故障 |

## 安全边界

* **监督进程独立于 API**：runner 的父进程是 PID 1 监督进程而非 API；API 的 `SIGSTOP`/`SIGKILL` 不会冻结或绕过超时与回收逻辑。
* **独立 runner**：每个请求新建 Python runner 子进程（`python -I`，空环境变量）、独立临时暂存目录和独立进程组；插件死循环不会占用 API 事件循环。
* **WASI 能力最小化**：不预打开宿主目录（没有 `--dir`），WASI 环境变量为空，`argv` 固定且不含宿主路径；Preview 1 没有 socket 接口。
* **文件描述符**：runner `close_fds=True`，stdin 为 `DEVNULL`，只有监督进程持有的 stdout/stderr 管道；WASI 侧只暴露 fd 0/1/2，日志通过 runner 配置为 stdin，插件看不到暂存目录路径。
* **权限**：监督进程以 root 启动仅用于 `setuid` 降权，API 与所有 runner 都以非特权 `sandbox`（uid/gid 10001）运行。
* **内存上限**：wasmtime `StoreLimits` 将线性内存限制为 64 MiB；表元素、实例、内存和表数量也有独立上限。
* **时间上限**：监督进程基于单调时钟，对每个 runner 执行启动后 2 秒墙钟上限；超时即终止整个进程组并 `waitpid` 回收，API 暂停不影响计时。
* **输出上限**：监督进程并发读取 stdout/stderr，共享 64 KiB 计数；任一流导致合计超限即杀进程，不解析部分输出。
* **故障清理**：API 死亡会触发一次性清扫——在途 runner（含尚在启动途中、还没拿到 pid 的试跑）一律终止回收并删除暂存；终态试跑保留短暂时间供轮询后再剪枝，避免内存无限增长。
* **容器加固**：Compose 使用只读根文件系统、`/tmp` tmpfs、`cap_drop: ALL`、`no-new-privileges`、进程数和内存限制。

说明：WASI Preview 1 本身没有网络和文件系统能力；宿主侧还通过不预打开目录和清空环境变量收缩攻击面。若需要更强的操作系统级网络隔离，可在生产编排层为整个服务增加网络策略，但这不影响 WASI 插件能力边界。

## 本地测试

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
```

测试分层：

* `tests/test_api.py`：经真实监督进程 + 真实 wasmtime runner 走完整 HTTP 接口，覆盖正常分类、陷阱、非零退出、各类非法输入、输出/内存上限、空环境、超时（504 且无部分结果）、控制面不可用（503）与并发互不阻塞；
* `tests/test_supervisor.py`：用极简假 runner 验证监督进程生命周期——无轮询下 2 秒超时、API `SIGKILL` 后 1 秒内清扫并重启、启动途中故障不漏管、输出超限、并发隔离、陈旧暂存清扫；在禁止 `kill(2)` 的主机上信号类用例自动跳过；
* `tests/test_supervisor_logic.py`：用确定性的假 `waitpid`/假 kill 在进程内覆盖超时、API 死亡清扫、启动竞争、重启纪元隔离与无 pid 试跑，不依赖主机信号权限；
* `tests/test_fault_drills.py`：完整拓扑下端到端演练 `SIGSTOP`（恢复后 504、无部分结果）与 `SIGKILL`（1 秒内清扫 + 自动重启 + 新调用隔离）。

## 主要文件

```text
app/main.py        FastAPI 接口、上传校验与 OpenAPI 文档
app/middleware.py  在 multipart 解析前限制请求体
app/supervisor.py  PID 1 监督进程：管理 API、拥有并回收所有 runner、执行时限与清扫
app/sandbox.py     API 侧控制面客户端（Unix 套接字），保留原 execute_plugin 契约
app/runner.py      每个请求独立运行的 wasmtime/WASI runner（契约不变）
app/runresult.py   runner 退出码到 API 错误的统一映射
app/config.py      所有资源上限与控制面路径
demo/              演示模块、WAT 源码、脱敏日志与故障演练脚本
```
