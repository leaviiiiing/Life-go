package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func (a *App) RegisterAgent(r *gin.Engine) {
	ag := r.Group("/api/agent")
	ag.Use(a.agentRateLimit())
	ag.POST("/chat", a.agentChat)
	rel := r.Group("/api/agent/reliability")
	rel.Use(a.agentRateLimit())
	rel.GET("/failed-logs", a.agentFailedLogs)
	rel.POST("/voucher/republish", a.agentRepublish)
}

func (a *App) agentRateLimit() gin.HandlerFunc {
	return func(c *gin.Context) {
		limit := a.Cfg.AgentRateLimitPerMin
		if limit <= 0 {
			c.Next()
			return
		}
		ip := clientIP(c)
		key := "agent:rl:" + ip
		n, err := a.RDB.Incr(c.Request.Context(), key).Result()
		if err == nil && n == 1 {
			_ = a.RDB.Expire(c.Request.Context(), key, time.Minute).Err()
		}
		if n > int64(limit) {
			c.Data(http.StatusTooManyRequests, "application/json;charset=UTF-8", []byte(`{"success":false,"errorMsg":"rate limit"}`))
			c.Abort()
			return
		}
		c.Next()
	}
}

func clientIP(c *gin.Context) string {
	xff := c.GetHeader("X-Forwarded-For")
	if xff != "" {
		if i := strings.Index(xff, ","); i > 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	return c.ClientIP()
}

type faqRule struct {
	ID       string   `json:"id"`
	Keywords []string `json:"keywords"`
	Hint     string   `json:"hint"`
}

type faqRoot struct {
	Rules []faqRule `json:"rules"`
}

func (a *App) agentChat(c *gin.Context) {
	var req struct {
		SessionID string `json:"sessionId"`
		Text      string `json:"text"`
	}
	_ = c.ShouldBindJSON(&req)
	sid := a.ensureAgentSession(req.SessionID)
	var root faqRoot
	_ = json.Unmarshal(a.FaqJSON, &root)
	ruleID, reply, kw := matchFAQ(req.Text, root.Rules)
	source := "rule"
	if ruleID == "default" && a.llmAvailable() {
		if r := a.llmChat(c.Request.Context(), sid, req.Text); r != "" {
			reply = r
			source = "llm"
		} else {
			source = "fallback"
		}
	} else if ruleID == "default" {
		source = "fallback"
	}
	a.appendAgentSession(c.Request.Context(), sid, req.Text, reply)
	c.JSON(http.StatusOK, gin.H{
		"success":         true,
		"sessionId":       sid,
		"ruleId":          ruleID,
		"reply":           reply,
		"matchedKeyword":  kw,
		"source":          source,
	})
}

func matchFAQ(text string, rules []faqRule) (id, hint string, kw *string) {
	if strings.TrimSpace(text) == "" {
		return "default", "请描述现象，例如：死信、PENDING、重放、幂等。", nil
	}
	low := strings.ToLower(text)
	for _, r := range rules {
		for _, k := range r.Keywords {
			if k == "" {
				continue
			}
			if strings.Contains(text, k) || strings.Contains(low, strings.ToLower(k)) {
				kk := k
				return r.ID, r.Hint, &kk
			}
		}
	}
	return "default", "未命中规则：可查看 tb_mq_kafka_log、Kafka Topic voucher.order.dlt，或经网关调用 GET /api/agent/reliability/failed-logs。", nil
}

func (a *App) llmAvailable() bool {
	if !a.Cfg.AgentLLMEnabled {
		return false
	}
	return strings.TrimSpace(a.Cfg.AgentLLMAPIKey) != "" && strings.TrimSpace(a.Cfg.AgentLLMModel) != ""
}

func (a *App) llmChat(ctx context.Context, sessionID, text string) string {
	base := strings.TrimSpace(a.Cfg.AgentLLMBaseURL)
	if base == "" {
		base = "https://ark.cn-beijing.volces.com/api/v3"
	}
	url := base
	if strings.HasSuffix(url, "/chat/completions") {
	} else if strings.HasSuffix(url, "/") {
		url += "chat/completions"
	} else {
		url += "/chat/completions"
	}
	msgs := []map[string]string{{"role": "system", "content": "你是「消费社交生活服务平台」的运维助手，熟悉 Kafka、Redis 秒杀、MySQL 订单与 MQ 补偿。用简洁中文回答。"}}
	for _, t := range a.agentRecentTurns(ctx, sessionID) {
		if t.User != "" {
			msgs = append(msgs, map[string]string{"role": "user", "content": t.User})
		}
		if t.Agent != "" {
			msgs = append(msgs, map[string]string{"role": "assistant", "content": t.Agent})
		}
	}
	msgs = append(msgs, map[string]string{"role": "user", "content": text})
	body, _ := json.Marshal(map[string]interface{}{
		"model":       strings.TrimSpace(a.Cfg.AgentLLMModel),
		"messages":    msgs,
		"temperature": a.Cfg.AgentLLMTemperature,
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(a.Cfg.AgentLLMAPIKey))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: a.Cfg.AgentLLMTimeout}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var root map[string]interface{}
	if json.Unmarshal(b, &root) != nil {
		return ""
	}
	ch, _ := root["choices"].([]interface{})
	if len(ch) == 0 {
		return ""
	}
	c0, _ := ch[0].(map[string]interface{})
	msg, _ := c0["message"].(map[string]interface{})
	s, _ := msg["content"].(string)
	return strings.TrimSpace(s)
}

type agentTurn struct {
	User  string `json:"user"`
	Agent string `json:"agent"`
}

func (a *App) ensureAgentSession(sid string) string {
	sid = strings.TrimSpace(sid)
	if sid != "" {
		return sid
	}
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}

func (a *App) appendAgentSession(ctx context.Context, sid, userText, agentReply string) {
	key := "agent:session:" + sid
	raw, _ := a.RDB.Get(ctx, key).Result()
	var root map[string]interface{}
	if raw != "" {
		_ = json.Unmarshal([]byte(raw), &root)
	} else {
		root = map[string]interface{}{"turns": []interface{}{}}
	}
	turns, _ := root["turns"].([]interface{})
	turns = append(turns, map[string]string{"user": userText, "agent": agentReply})
	if len(turns) > 20 {
		turns = turns[len(turns)-20:]
	}
	root["turns"] = turns
	b, _ := json.Marshal(root)
	_ = a.RDB.Set(ctx, key, string(b), 24*time.Hour).Err()
}

func (a *App) agentRecentTurns(ctx context.Context, sid string) []agentTurn {
	key := "agent:session:" + strings.TrimSpace(sid)
	raw, _ := a.RDB.Get(ctx, key).Result()
	if raw == "" {
		return nil
	}
	var root struct {
		Turns []agentTurn `json:"turns"`
	}
	_ = json.Unmarshal([]byte(raw), &root)
	return root.Turns
}

func (a *App) agentFailedLogs(c *gin.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	b, err := a.mqFailedLogsJSON(c.Request.Context(), limit)
	if err != nil {
		c.String(http.StatusInternalServerError, "{}")
		return
	}
	c.Data(http.StatusOK, "application/json", b)
}

func (a *App) agentRepublish(c *gin.Context) {
	var body struct {
		OrderID   int64 `json:"orderId"`
		UserID    int64 `json:"userId"`
		VoucherID int64 `json:"voucherId"`
	}
	_ = c.ShouldBindJSON(&body)
	msg, err := a.republishVoucherOrder(c.Request.Context(), body.OrderID, body.UserID, body.VoucherID)
	if err != nil {
		b, _ := json.Marshal(dtoFail(err.Error()))
		c.Data(http.StatusOK, "application/json", b)
		return
	}
	b, _ := json.Marshal(dtoOk(msg))
	c.Data(http.StatusOK, "application/json", b)
}

func dtoFail(msg string) map[string]interface{} {
	return map[string]interface{}{"success": false, "errorMsg": msg}
}

func dtoOk(data interface{}) map[string]interface{} {
	return map[string]interface{}{"success": true, "data": data}
}

func (a *App) mqFailedLogsJSON(ctx context.Context, limit int) ([]byte, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > 200 {
		limit = 200
	}
	var rows []MqKafkaLog
	err := a.DB.SelectContext(ctx, &rows, `SELECT id,msg_id,biz_type,biz_key,topic,partition_id,offset_val,direction,status,error_msg,create_time FROM tb_mq_kafka_log WHERE status='FAILED' ORDER BY create_time DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	return json.Marshal(dtoOk(rows))
}
