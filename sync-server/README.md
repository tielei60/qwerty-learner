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
| `SYNC_WS_HOST` / `SYNC_WS_PORT` | `0.0.0.0` / `9322` | Web 端 / Tauri 端 WebSocket 连接地址 |
| `SYNC_AUTH_TOKEN` | `qwerty-dev-token` | 连接认证 Token（上线前务必修改） |

## MQ 主题协议

- **上行发布**：`sync.up.{userId}`（JetStream `SYNC_UP`，保留 7 天）
- **下行订阅**：`sync.down.{userId}`（JetStream `SYNC_DOWN`，保留 30 天）
- **冷启动 / 增量快照**：`sync.req.snapshot.{userId}`（NATS Request-Reply，传入 `{ "userId": "...", "deviceId": "...", "afterSeq": 0 }`）
