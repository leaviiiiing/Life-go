package app

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/leaviiiiing/Life-go/server/internal/dto"
	"github.com/leaviiiiing/Life-go/server/internal/middleware"
)

const (
	voucherOrderTopic = "voucher.order.topic"
	seckillIdemPref   = "seckill:idem:"
	bizVoucher         = "VOUCHER_ORDER"
)

func (a *App) nextSnowflakeID(ctx context.Context, prefix string) (int64, error) {
	const begin = int64(1771891200)
	const shift = 32
	now := time.Now().UTC().Unix()
	ts := now - begin
	date := time.Now().UTC().Format("2006:01:02")
	key := "icr:" + prefix + ":" + date
	n, err := a.RDB.Incr(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	if n == 1 {
		_ = a.RDB.Expire(ctx, key, 48*time.Hour).Err()
	}
	return (ts << shift) | n, nil
}

func (a *App) postVoucherOrderSeckill(c *gin.Context) {
	u, ok := middleware.UserFromCtx(c)
	if !ok {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}
	voucherID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	idem := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	ctx := c.Request.Context()
	if idem != "" {
		h := md5.Sum([]byte(idem))
		key := seckillIdemPref + strconv.FormatInt(u.ID, 10) + ":" + strconv.FormatInt(voucherID, 10) + ":" + hex.EncodeToString(h[:])
		if v, err := a.RDB.Get(ctx, key).Result(); err == nil && v != "" {
			oid, _ := strconv.ParseInt(v, 10, 64)
			c.JSON(http.StatusOK, dto.OkData(oid))
			return
		}
	}
	orderID, err := a.nextSnowflakeID(ctx, "order")
	if err != nil {
		c.JSON(http.StatusOK, dto.Fail("生成订单号失败"))
		return
	}
	res, err := a.RDB.Eval(ctx, a.SeckillLua, []string{}, strconv.FormatInt(voucherID, 10), strconv.FormatInt(u.ID, 10), strconv.FormatInt(orderID, 10)).Int64()
	if err != nil {
		c.JSON(http.StatusOK, dto.Fail("秒杀失败"))
		return
	}
	if res == 1 {
		c.JSON(http.StatusOK, dto.Fail("库存不足！"))
		return
	}
	if res == 2 {
		c.JSON(http.StatusOK, dto.Fail("无法重复下单！"))
		return
	}
	vo := VoucherOrder{ID: orderID, UserID: u.ID, VoucherID: voucherID}
	body, _ := json.Marshal(vo)
	msgID := bizVoucher + ":" + strconv.FormatInt(orderID, 10) + ":" + strings.ReplaceAll(uuid.New().String(), "-", "")
	headers := []kafka.Header{
		{Key: "MSG_ID", Value: []byte(msgID)},
		{Key: "IDEMPOTENT_KEY", Value: []byte(msgID)},
	}
	if err := a.KW.WriteMessages(ctx, kafka.Message{
		Topic:   voucherOrderTopic,
		Key:     []byte(strconv.FormatInt(u.ID, 10)),
		Value:   body,
		Headers: headers,
	}); err != nil {
		_ = a.logKafkaSendFailed(ctx, msgID, bizVoucher, strconv.FormatInt(orderID, 10), voucherOrderTopic, err.Error())
	}
	if idem != "" {
		h := md5.Sum([]byte(idem))
		key := seckillIdemPref + strconv.FormatInt(u.ID, 10) + ":" + strconv.FormatInt(voucherID, 10) + ":" + hex.EncodeToString(h[:])
		_ = a.RDB.Set(ctx, key, strconv.FormatInt(orderID, 10), 24*time.Hour).Err()
	}
	c.JSON(http.StatusOK, dto.OkData(orderID))
}

func (a *App) postVoucherOrderPay(c *gin.Context) {
	u, ok := middleware.UserFromCtx(c)
	if !ok {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}
	oid, _ := strconv.ParseInt(c.Param("orderId"), 10, 64)
	ctx := c.Request.Context()
	var st int
	var uid int64
	err := a.DB.QueryRowContext(ctx, `SELECT status,user_id FROM tb_voucher_order WHERE id=?`, oid).Scan(&st, &uid)
	if err != nil {
		c.JSON(http.StatusOK, dto.Fail("订单不存在"))
		return
	}
	if uid != u.ID {
		c.JSON(http.StatusOK, dto.Fail("无权操作该订单"))
		return
	}
	if st != 1 {
		c.JSON(http.StatusOK, dto.Fail("订单非未支付状态，无法确认支付"))
		return
	}
	res, err := a.DB.ExecContext(ctx, `UPDATE tb_voucher_order SET status=2,pay_time=NOW(),update_time=NOW() WHERE id=? AND status=1`, oid)
	if err != nil {
		c.JSON(http.StatusOK, dto.Fail("支付确认失败，请重试"))
		return
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		c.JSON(http.StatusOK, dto.Fail("支付确认失败，请重试"))
		return
	}
	c.JSON(http.StatusOK, dto.Ok())
}

func (a *App) logKafkaSendFailed(ctx context.Context, msgID, biz, bizKey, topic, errMsg string) error {
	_, e := a.DB.ExecContext(ctx, `INSERT INTO tb_mq_kafka_log (msg_id,biz_type,biz_key,topic,direction,status,error_msg) VALUES (?,?,?,?,?,?,?)`,
		msgID, biz, bizKey, topic, "SEND", "FAILED", truncate(errMsg, 1000))
	return e
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func (a *App) republishVoucherOrder(ctx context.Context, orderID, userID, voucherID int64) (string, error) {
	var cnt int
	_ = a.DB.GetContext(ctx, &cnt, `SELECT COUNT(*) FROM tb_voucher_order WHERE id=?`, orderID)
	if cnt > 0 {
		return "", fmt.Errorf("订单已存在，无需重投")
	}
	vo := VoucherOrder{ID: orderID, UserID: userID, VoucherID: voucherID}
	body, _ := json.Marshal(vo)
	msgID := fmt.Sprintf("%s:%d:%s:comp", bizVoucher, orderID, strings.ReplaceAll(uuid.New().String(), "-", ""))
	idemKey := fmt.Sprintf("%s:%d:comp", bizVoucher, orderID)
	headers := []kafka.Header{
		{Key: "MSG_ID", Value: []byte(msgID)},
		{Key: "IDEMPOTENT_KEY", Value: []byte(idemKey)},
	}
	if err := a.KW.WriteMessages(ctx, kafka.Message{
		Topic:   voucherOrderTopic,
		Key:     []byte(strconv.FormatInt(userID, 10)),
		Value:   body,
		Headers: headers,
	}); err != nil {
		_ = a.logKafkaSendFailed(ctx, msgID, bizVoucher, strconv.FormatInt(orderID, 10), voucherOrderTopic, err.Error())
	}
	return "已发起重投，msgId=" + msgID, nil
}
