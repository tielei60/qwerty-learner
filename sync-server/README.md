# qwerty-learner sync-server

单二进制后端同步服务，内嵌 **NATS JetStream（含 WebSocket）** 与 **SQLite（纯 Go `modernc.org/sqlite`，零 CGO）**，用于在不修改网页端与 Tauri 客户端现有本地存储方式的前提下，通过消息队列实现双向增量数据打通。

## 目录结构

```text
sync-server/
├── cmd/sync-server/main.go     # 进程入口：装配 SQLite、内嵌 NATS 与 Worker
├── internal/
│   ├── config/config.go        # 环境变量配置
│   ├── protocol/event.go       # 同步事件与快照请求协议定义
│   ├── store/sqlite.go         # SQLite 共享库（事件日志 + 权威状态 + 幂等与冲突合并）
│   ├── broker/nats.go          # 内嵌 NATS Server（JetStream + WebSocket）
│   └── worker/worker.go        # 消费 sync.up.>、落库、广播 sync.down.{userId}、响应快照
├── go.mod
└── Dockerfile
```

## 快速启动

### 本地运行（需 Go 1.22+）

```bash
cd sync-server
go mod tidy
SYNC_AUTH_TOKEN=my-secret-token go run ./cmd/sync-server
```

编译为单二进制文件（零 CGO）：

```bash
cd sync-server
CGO_ENABLED=0 go build -ldflags="-s -w" -o sync-server ./cmd/sync-server
./sync-server
```

### 环境变量

| 变量名 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `SYNC_DATA_DIR` | `./data` | 数据根目录（存放 SQLite 与 JetStream 文件） |
| `SYNC_SQLITE_PATH` | `./data/shared.db` | SQLite 数据库路径 |
| `SYNC_NATS_STORE_DIR` | `./data/jetstream` | JetStream 持久化目录 |
| `SYNC_NATS_HOST` / `SYNC_NATS_PORT` | `127.0.0.1` / `4222` | NATS TCP 监听地址 |
| `SYNC_WS_HOST` / `SYNC_WS_PORT` | `127.0.0.1` / `9322` | Web 端 / Tauri 端 WebSocket 连接地址 |
| `SYNC_WS_ALLOWED_ORIGINS` | 本地网页端（3000、5173、4173 端口）及 Tauri 来源 | 允许连接的完整 Origin，多个值用逗号分隔；自定义网页端口或托管站点需显式加入 |
| `SYNC_AUTH_TOKEN` | `qwerty-dev-token` | 连接认证 Token（上线前务必修改） |

## MQ 主题协议

- **上行发布**：`sync.up.{userId}`（JetStream `SYNC_UP`，保留 7 天）
- **下行订阅**：`sync.down.{userId}`（JetStream `SYNC_DOWN`，保留 30 天）
- **冷启动 / 增量快照**：`sync.req.snapshot.{userId}`（NATS Request-Reply，传入 `{ "userId": "...", "deviceId": "...", "afterSeq": 0 }`）

同步服务面向同一台机器上的浏览器和客户端，只绑定回环地址。容器部署需要使用能访问该回环地址的网络方式。

WebSocket 仅接受明确允许的来源，默认包括 `tauri://localhost`、`http://tauri.localhost`、`https://tauri.localhost`，以及 `127.0.0.1`、`localhost`、`[::1]` 上的 3000、5173、4173 网页端口。使用其他端口或托管站点时，设置 `SYNC_WS_ALLOWED_ORIGINS` 为完整的允许列表（包括仍需使用的 Tauri 来源），例如：

```bash
SYNC_WS_ALLOWED_ORIGINS='http://127.0.0.1:8080,tauri://localhost,http://tauri.localhost,https://tauri.localhost' ./sync-server
```

## 回归测试

在 `sync-server/` 中执行 `go test ./...`，验证来源限制、认证及服务持久化。在仓库根目录执行 `yarn test:sync`，验证复习记录 ID 转换、重复历史回填、游标隔离与服务序号重置、重新开始练习后的恢复。

浏览器测试需要 Go 和 Playwright Chromium（`yarn playwright install chromium`）；也可使用已安装的 Chrome：`PLAYWRIGHT_CHANNEL=chrome yarn test:sync`。测试会启动临时同步服务和本地网页，分别占用 18174/18175 与 18173 端口，并在结束后删除临时同步数据。
