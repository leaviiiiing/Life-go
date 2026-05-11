# Life-go

由 Spring 示例项目迁移而来的 **Go 单二进制** 实现：主 API **8081**、运维 Agent **8082**，HTTP 契约与课程文档中的 API 约定一致。

## 本地开发

1. 准备 MySQL（库 `hmdp`）、Redis、Kafka；数据库初始化脚本已置于 [deploy/mysql/init/](deploy/mysql/init/)（`01-hmdp.sql`、`02-z_mq_kafka_log.sql`）。
2. 环境变量见 [server/internal/config/config.go](server/internal/config/config.go)（`MYSQL_DSN`、`SPRING_REDIS_*`、`KAFKA_BOOTSTRAP_SERVERS`、`HMDP_UPLOAD_DIR` 等）。
3. 启动：

```bash
cd server
set GOPROXY=https://goproxy.cn,direct
go run ./cmd/server
```

## Docker 一键编排

在仓库根目录执行（需已安装 Docker Compose）：

```bash
docker compose -f deploy/docker-compose.yml up -d --build
```

- 前端：<http://localhost:80>（Nginx 反代 `/api` → Go:8081，`/api/agent` → Go:8082）
- 主 API：<http://localhost:8081>；Agent：<http://localhost:8082>
- MySQL 映射到宿主机 **13306**（避免与本机已有 MySQL 占用的 3306 冲突）；容器内服务名仍为 `mysql:3306`。

大模型（可选）：在运行前设置环境变量 `AGENT_LLM_API_KEY`、`AGENT_LLM_MODEL`，或在 compose 同目录 `.env` 中配置。

秒杀压测示例（k6）：[deploy/stress/k6-seckill.js](deploy/stress/k6-seckill.js)。

### 故障排查

- **`bitnami/kafka:3.6` not found**：Compose 已改为 `bitnamilegacy/kafka:3.6.2-debian-12-r14`（Bitnami 免费 Kafka 镜像迁移至 Legacy 仓库）。
- **构建 `api` 时 `auth.docker.io` 超时**：`deploy/docker-compose.yml` 已对 `api` 构建传入 DaoCloud 上的 `golang` / `alpine` 基础镜像；若你更信任官方源，可改 `server/Dockerfile` 的默认 `ARG` 或在 compose 里把 `GO_BUILD_IMAGE` / `RUNTIME_IMAGE` 改为 `golang:1.22-alpine`、`alpine:3.19`。其余镜像仍走 Docker Hub，也可在 Docker Desktop → **Docker Engine** 里配置 `registry-mirrors`。
- **端口占用**：若 80 / 8081 / 6379 / 9092 被占用，请在 `deploy/docker-compose.yml` 中调整对应 `ports` 映射。

## 文档

- Go 实现说明与代码导读：[server/docs/GO_DEVELOPMENT.md](server/docs/GO_DEVELOPMENT.md)
