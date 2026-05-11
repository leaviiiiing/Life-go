# Life-go 服务端（Go）开发说明

本文描述 `server/` 模块的技术栈、目录结构、运行方式，以及各功能在 Go 中的实现要点，并引用仓库内真实代码路径，便于对照 Java 版 `hm-dianping-mq` 维护与二次开发。

---

## 1. 概述与运行方式

### 1.1 设计动机

- **单进程双 HTTP 端口**：与原有 Nginx 编排一致——浏览器访问 `/api/*` 时去掉前缀转发到 **8081**；`/api/agent/*` 转发到 **8082**，无需再维护两套 JVM 镜像。
- **契约兼容**：主 API 统一 JSON 形态与 Java `Result` 一致（`success` / `errorMsg` / `data` / `total`）；鉴权头 `authorization` 与 Redis 会话结构对齐。

### 1.2 本地运行

入口：`cmd/server/main.go`，启动两个 `http.Server`，并拉起 Kafka 消费协程与后台定时任务。

```1:40:server/cmd/server/main.go
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/leaviiiiing/Life-go/server/internal/app"
	"github.com/leaviiiiing/Life-go/server/internal/config"
)
// ... 创建 sqlx.DB、redis.Client、kafka.Writer 后：
	application := app.NewApp(cfg, db, rdb, kw, app.SeckillScript(), app.FAQBytes())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	application.StartKafkaConsumers(ctx)
	application.StartSchedulers(ctx)

	gin.SetMode(gin.ReleaseMode)
	mainEngine := gin.New()
	mainEngine.Use(gin.Logger(), gin.Recovery())
	application.RegisterMain(mainEngine)

	agentEngine := gin.New()
	agentEngine.Use(gin.Logger(), gin.Recovery())
	application.RegisterAgent(agentEngine)
```

### 1.3 环境变量

见 `internal/config/config.go` 的 `Load()`：`MYSQL_DSN`、`SPRING_REDIS_*`、`KAFKA_BOOTSTRAP_SERVERS`、`HMDP_UPLOAD_DIR`、订单超时与 MQ 巡检间隔、`AGENT_*` 等。

### 1.4 与 Docker / Nginx 的关系

- Compose：`deploy/docker-compose.yml` 构建 `server/Dockerfile`，服务名 **`api`** 同时映射 **8081、8082**。
- 前端：`frontend/nginx.conf` 将 `/api/agent/` 代理到 `http://api:8082`，`/api/` 代理到 `http://api:8081/`（strip 前缀），并挂载共享 volume 提供上传图片静态路径 `/blogs/`。

---

## 2. 模块与目录结构

| 路径 | 职责 |
|------|------|
| `go.mod` | Module：`github.com/leaviiiiing/Life-go/server |
| `cmd/server/main.go` | 进程入口：依赖初始化、双 Server、信号退出 |
| `internal/config` | 环境变量驱动的配置 |
| `internal/dto` | 对外 JSON 模型（如 `Result`） |
| `internal/middleware` | Gin 中间件：Token 刷新、登录拦截 |
| `internal/app` | 业务聚合：`App` 结构体、路由注册、各 `handle_*.go`、Kafka、定时任务、Agent |
| `internal/app/seckill.lua` | `//go:embed` 与 Java 同源 Lua 脚本 |
| `internal/app/faq-rules.json` | Agent 关键词规则 |
| `Dockerfile` | 多阶段构建，静态链接 `CGO_ENABLED=0` |

业务逻辑集中在 `internal/app`，`main` 仅做组装，符合计划中「避免业务散落在 main」的约定。

---

## 3. 依赖技术栈说明

| 能力 | 选型 | 使用位置 |
|------|------|----------|
| HTTP | **gin** | `RegisterMain` / `RegisterAgent` |
| MySQL | **sqlx** + `database/sql` | 各 `handle_*.go` 中 `ExecContext` / `SelectContext` |
| Redis | **go-redis/v9** | 会话、秒杀预减、Feed ZSet、签到 Bitmap、Kafka 消费幂等 |
| 分布式锁 | **bsm/redislock** | 秒杀订单 Kafka 消费内 `lock:order:{userId}` |
| Kafka | **segmentio/kafka-go** | `kafka.Writer` 生产；`kafka.Reader` + 手动 `CommitMessages` 消费 |
| 定时任务 | **time.Ticker** | `internal/app/jobs.go` |
| 日志 | **gin.Logger** | `cmd/server/main.go` 挂载到两个 Engine |

---

## 4. HTTP 与契约

### 4.1 `Result` 与业务错误码

```1:35:server/internal/dto/result.go
package dto

// Result matches Java com.hmdp.dto.Result JSON for the main API.
type Result struct {
	Success  bool        `json:"success"`
	ErrorMsg *string     `json:"errorMsg,omitempty"`
	Data     interface{} `json:"data,omitempty"`
	Total    *int64      `json:"total,omitempty"`
}

func Ok() Result {
	return Result{Success: true}
}

func OkData(data interface{}) Result {
	return Result{Success: true, Data: data}
}
```

业务失败仍返回 **HTTP 200** + `success:false`；未登录返回 **401**，与 `frontend/dist/js/common.js` 中 axios 拦截器一致。

### 4.2 鉴权链

1. `TokenRefresh`：从 `login:token:{token}` Hash 载入 `UserDTO`，刷新 TTL（分钟数与 Java `LOGIN_USER_TTL` 对齐）。
2. `LoginRequired`：对非白名单路径要求上下文存在用户，否则 401。

```67:115:server/internal/middleware/middleware.go
// PublicRoute mirrors Java MvcConfig excludePathPatterns for LoginInterceptor.
func PublicRoute(path, method string) bool {
	if path == "/user/code" && method == http.MethodPost {
		return true
	}
	if path == "/user/login" && method == http.MethodPost {
		return true
	}
	if strings.HasPrefix(path, "/shop/") {
		return true
	}
	// ... /blog/hot、/blog/likes/*、GET /blog/{纯数字id}、/voucher/、/mq/compensation/ 等
```

其中 **`GET /blog/likes/*` 与 `GET /blog/{id}`** 为在前端 `blog-detail.html` 场景下对 Java 白名单 `/likes/**` 的兼容补强。

### 4.3 路由注册

```1:55:server/internal/app/routes.go
func (a *App) RegisterMain(r *gin.Engine) {
	r.Use(middleware.TokenRefresh(a.RDB))
	r.Use(middleware.LoginRequired())

	r.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"ok": true}) })

	u := r.Group("/user")
	{
		u.POST("/code", a.postUserCode)
		u.POST("/login", a.postUserLogin)
		// ...
	}
	// shop / blog / follow / upload / voucher / voucher-order / mq ...
}
```

---

## 5. 按业务域拆解的实现

### 5.1 用户（验证码、登录、会话）

- 验证码：`login:code:{phone}`，TTL 2 分钟。
- 登录：校验验证码后查/建 `tb_user`，生成无横线 UUID 作为 token，`HSET login:token:{token}` 写入 `id`/`nickName`/`icon`（字符串化与 Java Hutool 行为一致），`EXPIRE` 3000 分钟。

核心代码：`internal/app/handle_user.go` 中 `postUserCode`、`postUserLogin`。

### 5.2 商铺与类型

- **详情**：`cache:shop:{id}` 缓存穿透/空值短 TTL 与 Java `CasheClient.queryWithPassthrough` 对齐，见 `getShopByID`。
- **地理分页**：Redis `GEOADD` 在首次按类型查询时从 MySQL 预热 `shop:geo:{typeId}`，再 `GEORADIUS` 分页，见 `handle_shop.go`。

### 5.3 笔记、Feed、点赞

- **发布**：写 MySQL 后向 `blog.feed.topic` 发送 JSON `BlogFeedMessage`，Kafka Headers 带 `MSG_ID`、`IDEMPOTENT_KEY`（`handle_social.go` `postBlog`）。
- **点赞**：DB `liked` 字段与 ZSet `blog:liked:{blogId}` 同步（`putBlogLike`）。
- **关注流**：ZSet `feed:{userId}`，`getBlogOfFollow` 使用 `ZRevRangeByScore` + `OFFSET`/`COUNT` 近似 Java 滚动分页。

### 5.4 关注

- DB：`tb_follow`；Redis：`follows:{userId}` Set 与 Java 双写策略一致（`handle_social.go`）。

### 5.5 上传

- `multipart/form-data` 字段名 `file`，落盘 `{HMDP_UPLOAD_DIR}/blogs/{d1}/{d2}/{uuid}.{ext}`，返回相对路径（`handle_upload.go`）。
- Nginx `alias` 暴露 `/blogs/` 与容器 volume 对齐（`frontend/nginx.conf`）。

### 5.6 优惠券与秒杀

- 店铺券列表 SQL 与 MyBatis XML 等价（`handle_voucher.go` `getVoucherList`）。
- 秒杀券创建后写入 `seckill:stock:{voucherId}`（`postVoucherSeckill`）。

### 5.7 秒杀订单 + Kafka

**HTTP 侧**：`Redis EVAL` 执行嵌入的 `seckill.lua`，与 Java 相同参数顺序 `voucherId, userId, orderId`；雪花 ID 由 `icr:order:yyyy:MM:dd` 自增 + 时间戳左移拼接（`nextSnowflakeID`）。

```55:95:server/internal/app/handle_order.go
	res, err := a.RDB.Eval(ctx, a.SeckillLua, []string{}, strconv.FormatInt(voucherID, 10), strconv.FormatInt(u.ID, 10), strconv.FormatInt(orderID, 10)).Int64()
	// ...
	if err := a.KW.WriteMessages(ctx, kafka.Message{
		Topic:   voucherOrderTopic,
		Key:     []byte(strconv.FormatInt(u.ID, 10)),
		Value:   body,
		Headers: headers,
	}); err != nil {
		_ = a.logKafkaSendFailed(ctx, msgID, bizVoucher, strconv.FormatInt(orderID, 10), voucherOrderTopic, err.Error())
	}
```

**消费侧**：`internal/app/kafka_run.go` 中 `runVoucherOrderConsumer`：

- GroupTopics：`voucher.order.topic` + `voucher.order.retry`
- 幂等：`mq:kafka:consumed:{idempotentKey}`，TTL 7 天（`kafka_helpers.go`）
- 失败：写 `tb_mq_kafka_log`，并按重试次数转发 `voucher.order.retry` 或 `voucher.order.dlt`（`handleVoucherRetryOrDLT`）

**落库**：`createVoucherOrderTx` 中「一人一单（排除已取消/已退款）+ 扣 `tb_seckill_voucher.stock` + 插入 `tb_voucher_order` status=1」与 Java `VoucherOrderServiceImpl` 一致。

### 5.8 Blog Feed 消费者

`runBlogFeedConsumer`：按 `follow_user_id` 查粉丝，写入各粉丝 `feed:{fanId}` ZSet，score 为消息时间戳。

### 5.9 定时任务与 MQ 补偿 HTTP

- **支付超时**：`jobs.go` `scanPayTimeout` 扫描 `status=1` 且超时订单 → 关单 `status=4`、回补 MySQL 库存与 Redis `seckill:stock` / `seckill:order` Set。
- **MQ 巡检**：统计 `tb_mq_kafka_log` 中失败/DLT 条数打日志。
- **HTTP**：`/mq/compensation/kafka/failed-logs` 与 `/mq/compensation/kafka/voucher/republish`（`handle_mq.go`），白名单在 `PublicRoute` 中放行。

---

## 6. Agent 子系统（8082）

- 路由：`internal/app/agent.go` 中 `RegisterAgent` — `POST /api/agent/chat`，`GET/POST /api/agent/reliability/*`。
- **规则**：启动时内嵌 `faq-rules.json`，关键词子串匹配（`matchFAQ`）。
- **会话**：Redis `agent:session:{sid}` JSON，最多 20 轮（`appendAgentSession`）。
- **LLM**：`llmChat` 使用标准库 `net/http` 调 OpenAI 兼容 `chat/completions`（配置 `AGENT_LLM_*`）。
- **限流**：Redis INCR `agent:rl:{ip}`，窗口 1 分钟，超限 429 JSON（`agentRateLimit`）。
- **Reliability**：进程内直接调 `republishVoucherOrder` / `mqFailedLogsJSON`，等价于 Java `RestTemplate` 转发主后端。

---

## 7. Kafka 可靠性（与 Java 对照）

| 行为 | Java 参考 | Go 实现位置 |
|------|-------------|-------------|
| Topic 名 | `KafkaTopics` | `voucherOrderTopic`、`blogFeedTop`、`voucherRetryTopic`、`voucherDLTTopic` |
| Headers | `MSG_ID`、`IDEMPOTENT_KEY`、`RETRY_COUNT` | `handle_order.go` / `kafka_helpers.go` `headerLast` |
| 消费幂等 | `KafkaConsumeIdempotencyService` | `kafkaConsumedPref` + `rdbSet` |
| 手动 ack | `Acknowledgment` | `reader.CommitMessages` |
| 审计表 | `MqKafkaLogServiceImpl` | `logKafkaSendFailed` / `logKafkaConsumeFailed` / `logKafkaDlt` |

---

## 8. 构建与部署

- **Dockerfile**：两阶段 `golang:1.22-alpine` build + `alpine` 运行，`CGO_ENABLED=0`，单可执行文件 `ENTRYPOINT ["/app/hmdp-api"]`。
- **Compose**：见 `deploy/docker-compose.yml`；基础设施与 Java 版 compose 等效，**api** 服务替代原 `backend`+`agent` 两个容器。

---

## 9. 测试与对拍建议

1. **登录链路**：`/user/code` → `/user/login` → `/user/me`，核对 Redis Hash 字段。
2. **商铺**：带经纬度 `/shop/of/type` 与不带坐标分页结果；`GET /shop/{id}` 缓存命中与穿透。
3. **笔记**：发帖 → Kafka `blog.feed.topic` → 粉丝 `feed:{id}` ZSet；点赞 ZSet 与 DB `liked` 一致。
4. **秒杀**：Lua 返回 0 → Kafka → 消费落库；重复用户、库存不足路径；`Idempotency-Key` 幂等。
5. **Agent**：规则命中、`source` 字段、限流 429；`reliability` 返回 JSON 与主接口一致。

与 Java 并行运行时，可用相同 MySQL/Redis/Kafka，对 Kafka **JSON 消息体**字段做一次抓包比对（尤其时间类型），避免序列化差异。

---

## 附录：与 Java 路径速查

| Java | Go |
|------|-----|
| `UserController` | `handle_user.go` |
| `ShopController` / `ShopTypeController` | `handle_shop.go` |
| `BlogController` / `FollowController` | `handle_social.go` |
| `VoucherController` / `VoucherOrderController` | `handle_voucher.go` / `handle_order.go` |
| `UploadController` | `handle_upload.go` |
| `MqKafkaCompensationController` | `handle_mq.go` |
| `VoucherOrderKafkaListener` / `BlogFeedConsumer` | `kafka_run.go` + `kafka_helpers.go` |
| `VoucherOrderPayTimeoutScheduler` / `MqKafkaCompensationScheduler` | `jobs.go` |
| `agent-service` | `agent.go` |
