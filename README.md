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

大模型（可选）：在运行前设置环境变量 `AGENT_LLM_API_KEY`、`AGENT_LLM_MODEL`，或在 compose 同目录 `.env` 中配置。

## 文档

- Go 实现说明与代码导读：[server/docs/GO_DEVELOPMENT.md](server/docs/GO_DEVELOPMENT.md)
