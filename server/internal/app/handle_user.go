package app

import (
	crand "crypto/rand"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/leaviiiiing/Life-go/server/internal/dto"
	"github.com/leaviiiiing/Life-go/server/internal/middleware"
)

const (
	loginCodeKey = "login:code:"
	loginUserKey = "login:token:"
	userSignKey  = "sign:"
)

func validPhone(phone string) bool {
	if len(phone) != 11 || phone[0] != '1' {
		return false
	}
	for i := 1; i < len(phone); i++ {
		if phone[i] < '0' || phone[i] > '9' {
			return false
		}
	}
	return true
}

func (a *App) postUserCode(c *gin.Context) {
	phone := c.Query("phone")
	if !validPhone(phone) {
		c.JSON(http.StatusOK, dto.Fail("手机号格式错误"))
		return
	}
	code := randomDigits(6)
	ctx := c.Request.Context()
	if err := a.RDB.Set(ctx, loginCodeKey+phone, code, 2*time.Minute).Err(); err != nil {
		c.JSON(http.StatusOK, dto.Fail("服务异常"))
		return
	}
	c.JSON(http.StatusOK, dto.Ok())
}

func (a *App) postUserLogin(c *gin.Context) {
	var body struct {
		Phone    string `json:"phone"`
		Code     string `json:"code"`
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusOK, dto.Fail("参数错误"))
		return
	}
	if !validPhone(body.Phone) {
		c.JSON(http.StatusOK, dto.Fail("手机号格式错误"))
		return
	}
	ctx := c.Request.Context()
	cache, _ := a.RDB.Get(ctx, loginCodeKey+body.Phone).Result()
	if cache == "" || cache != body.Code {
		c.JSON(http.StatusOK, dto.Fail("验证码错误"))
		return
	}
	var u User
	err := a.DB.GetContext(ctx, &u, "SELECT id, phone, password, nick_name, icon, create_time, update_time FROM tb_user WHERE phone = ? LIMIT 1", body.Phone)
	if err != nil {
		nick := "user_" + randomAlphaNum(10)
		res, e2 := a.DB.ExecContext(ctx, "INSERT INTO tb_user (phone, password, nick_name, icon) VALUES (?,?,?,?)",
			body.Phone, "", nick, "")
		if e2 != nil {
			c.JSON(http.StatusOK, dto.Fail("注册失败"))
			return
		}
		id, _ := res.LastInsertId()
		u = User{ID: id, Phone: body.Phone, NickName: nick}
	}
	token := strings.ReplaceAll(uuid.New().String(), "-", "")
	h := map[string]interface{}{
		"id":       fmt.Sprintf("%d", u.ID),
		"nickName": u.NickName,
		"icon":     u.Icon,
	}
	if err := a.RDB.HSet(ctx, loginUserKey+token, h).Err(); err != nil {
		c.JSON(http.StatusOK, dto.Fail("登录失败"))
		return
	}
	_ = a.RDB.Expire(ctx, loginUserKey+token, 3000*time.Minute).Err()
	c.JSON(http.StatusOK, dto.OkData(token))
}

func (a *App) postUserLogout(c *gin.Context) {
	c.JSON(http.StatusOK, dto.Fail("功能未完成"))
}

func (a *App) getUserMe(c *gin.Context) {
	u, _ := middleware.UserFromCtx(c)
	c.JSON(http.StatusOK, dto.OkData(gin.H{"id": u.ID, "nickName": u.NickName, "icon": u.Icon}))
}

func (a *App) getUserInfo(c *gin.Context) {
	id := c.Param("id")
	ctx := c.Request.Context()
	var row struct {
		UserID    int64      `db:"user_id"`
		City      string     `db:"city"`
		Introduce *string    `db:"introduce"`
		Gender    int        `db:"gender"`
		Birthday  *time.Time `db:"birthday"`
	}
	err := a.DB.GetContext(ctx, &row, "SELECT user_id, city, introduce, gender, birthday FROM tb_user_info WHERE user_id = ? LIMIT 1", id)
	if err != nil {
		c.JSON(http.StatusOK, dto.Ok())
		return
	}
	c.JSON(http.StatusOK, dto.OkData(gin.H{
		"userId":    row.UserID,
		"city":      row.City,
		"introduce": row.Introduce,
		"gender":    row.Gender,
		"birthday":  row.Birthday,
	}))
}

func (a *App) getUserByID(c *gin.Context) {
	id := c.Param("id")
	ctx := c.Request.Context()
	var u User
	if err := a.DB.GetContext(ctx, &u, "SELECT id, phone, password, nick_name, icon, create_time, update_time FROM tb_user WHERE id = ? LIMIT 1", id); err != nil {
		c.JSON(http.StatusOK, dto.Ok())
		return
	}
	c.JSON(http.StatusOK, dto.OkData(gin.H{"id": u.ID, "nickName": u.NickName, "icon": u.Icon}))
}

func (a *App) postUserSign(c *gin.Context) {
	u, ok := middleware.UserFromCtx(c)
	if !ok {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}
	ctx := c.Request.Context()
	now := time.Now()
	key := fmt.Sprintf("%s%s:%s", userSignKey, fmt.Sprint(u.ID), now.Format("200601"))
	day := now.Day()
	bit, err := a.RDB.GetBit(ctx, key, int64(day-1)).Result()
	if err == nil && bit == 1 {
		c.JSON(http.StatusOK, dto.Fail("已登录"))
		return
	}
	_ = a.RDB.SetBit(ctx, key, int64(day-1), 1).Err()
	c.JSON(http.StatusOK, dto.Ok())
}

func (a *App) getUserSignCount(c *gin.Context) {
	u, ok := middleware.UserFromCtx(c)
	if !ok {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}
	ctx := c.Request.Context()
	now := time.Now()
	key := fmt.Sprintf("%s%s:%s", userSignKey, fmt.Sprint(u.ID), now.Format("200601"))
	day := now.Day()
	if day <= 0 {
		day = 1
	}
	uTyp := fmt.Sprintf("u%d", day)
	vals, err := a.RDB.BitField(ctx, key, "GET", uTyp, "0").Result()
	if err != nil || len(vals) == 0 {
		c.JSON(http.StatusOK, dto.OkData(0))
		return
	}
	num := vals[0]
	if num == 0 {
		c.JSON(http.StatusOK, dto.OkData(0))
		return
	}
	cnt := 0
	for num&1 == 1 {
		cnt++
		num >>= 1
	}
	c.JSON(http.StatusOK, dto.OkData(cnt))
}

func randomDigits(n int) string {
	const digits = "0123456789"
	b := make([]byte, n)
	for i := range b {
		v, _ := crand.Int(crand.Reader, big.NewInt(10))
		b[i] = digits[v.Int64()]
	}
	return string(b)
}

func randomAlphaNum(n int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		v, _ := crand.Int(crand.Reader, big.NewInt(int64(len(chars))))
		b[i] = chars[v.Int64()]
	}
	return string(b)
}
