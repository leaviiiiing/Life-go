package middleware

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

const ctxUserKey = "hmdpUser"

type UserDTO struct {
	ID       int64  `json:"id"`
	NickName string `json:"nickName"`
	Icon     string `json:"icon"`
}

func UserFromCtx(c *gin.Context) (*UserDTO, bool) {
	v, ok := c.Get(ctxUserKey)
	if !ok || v == nil {
		return nil, false
	}
	u, ok := v.(*UserDTO)
	return u, ok && u != nil
}

// TokenRefresh loads session from Redis Hash login:token:{token} and stores UserDTO in context; refreshes TTL.
func TokenRefresh(rdb *redis.Client) gin.HandlerFunc {
	const loginKey = "login:token:"
	const ttlMin = 3000 // Java RedisConstants.LOGIN_USER_TTL (minutes)

	return func(c *gin.Context) {
		token := strings.TrimSpace(c.GetHeader("authorization"))
		if token == "" {
			c.Next()
			return
		}
		ctx := c.Request.Context()
		key := loginKey + token
		m, err := rdb.HGetAll(ctx, key).Result()
		if err != nil || len(m) == 0 {
			c.Next()
			return
		}
		u := &UserDTO{}
		if v := m["id"]; v != "" {
			var id int64
			for i := 0; i < len(v); i++ {
				ch := v[i]
				if ch < '0' || ch > '9' {
					break
				}
				id = id*10 + int64(ch-'0')
			}
			u.ID = id
		}
		u.NickName = m["nickName"]
		u.Icon = m["icon"]
		c.Set(ctxUserKey, u)
		_ = rdb.Expire(ctx, key, time.Duration(ttlMin)*time.Minute).Err()
		c.Next()
	}
}

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
	if strings.HasPrefix(path, "/shop-type/") {
		return true
	}
	if strings.HasPrefix(path, "/upload") { // upload/blog etc.
		return true
	}
	if path == "/blog/hot" {
		return true
	}
	// 与前端 blog-detail 对齐：匿名可查看点赞列表（Java MvcConfig 为 /likes/** 的兼容修正）
	if method == http.MethodGet && strings.HasPrefix(path, "/blog/likes/") {
		return true
	}
	// 匿名查看单篇笔记详情：GET /blog/{数字id}
	if method == http.MethodGet && strings.HasPrefix(path, "/blog/") {
		suf := strings.TrimPrefix(path, "/blog/")
		if suf != "" && !strings.Contains(suf, "/") && isAllDigits(suf) {
			return true
		}
	}
	if strings.HasPrefix(path, "/likes/") {
		return true
	}
	if strings.HasPrefix(path, "/voucher/") {
		return true
	}
	if strings.HasPrefix(path, "/mq/compensation/") {
		return true
	}
	if path == "/health" {
		return true
	}
	return false
}

func isAllDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return s != ""
}

// LoginRequired aborts with 401 when no user in context (after TokenRefresh).
func LoginRequired() gin.HandlerFunc {
	return func(c *gin.Context) {
		if PublicRoute(c.Request.URL.Path, c.Request.Method) {
			c.Next()
			return
		}
		if _, ok := UserFromCtx(c); !ok {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		c.Next()
	}
}
