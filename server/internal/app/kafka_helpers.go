package app

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func headerLast(m *kafka.Message, name string) string {
	var last []byte
	for _, h := range m.Headers {
		if h.Key == name {
			last = h.Value
		}
	}
	if last == nil {
		return ""
	}
	return string(last)
}

func parseRetryCount(s string) int {
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0
	}
	return n
}

func (a *App) rdbHas(ctx context.Context, key string) bool {
	ok, _ := a.RDB.Exists(ctx, key).Result()
	return ok > 0
}

func (a *App) rdbSet(ctx context.Context, key string, ttl time.Duration) error {
	return a.RDB.Set(ctx, key, "1", ttl).Err()
}

func (a *App) logKafkaConsumeFailed(ctx context.Context, msgID, biz, bizKey, topic string, partition int, offset int64, errMsg string) error {
	p := partition
	o := offset
	_, e := a.DB.ExecContext(ctx, `INSERT INTO tb_mq_kafka_log (msg_id,biz_type,biz_key,topic,partition_id,offset_val,direction,status,error_msg) VALUES (?,?,?,?,?,?,?,?,?)`,
		msgID, biz, bizKey, topic, &p, &o, "CONSUME", "FAILED", truncate(errMsg, 1000))
	return e
}

func (a *App) logKafkaDlt(ctx context.Context, msgID, biz, bizKey, topic, errMsg string) error {
	_, e := a.DB.ExecContext(ctx, `INSERT INTO tb_mq_kafka_log (msg_id,biz_type,biz_key,topic,direction,status,error_msg) VALUES (?,?,?,?,?,?,?)`,
		msgID, biz, bizKey, topic, "DLT", "FAILED", truncate(errMsg, 1000))
	return e
}

func (a *App) createVoucherOrderTx(ctx context.Context, vo *VoucherOrder) error {
	tx, err := a.DB.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var cnt int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM tb_voucher_order WHERE user_id=? AND voucher_id=? AND status NOT IN (4,6)`, vo.UserID, vo.VoucherID).Scan(&cnt); err != nil {
		return err
	}
	if cnt > 0 {
		return fmt.Errorf("duplicate order")
	}
	res, err := tx.ExecContext(ctx, `UPDATE tb_seckill_voucher SET stock=stock-1 WHERE voucher_id=? AND stock>0`, vo.VoucherID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("stock insufficient")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO tb_voucher_order (id,user_id,voucher_id,pay_type,status) VALUES (?,?,?,?,1)`, vo.ID, vo.UserID, vo.VoucherID, 1)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (a *App) handleVoucherRetryOrDLT(ctx context.Context, m *kafka.Message, vo *VoucherOrder, idemKey, msgID string, retry int) {
	if retry < maxRetryBeforeDLT {
		next := retry + 1
		newMsgID := idemKey + ":r" + strconv.Itoa(next)
		body, _ := json.Marshal(vo)
		headers := []kafka.Header{
			{Key: "MSG_ID", Value: []byte(newMsgID)},
			{Key: "IDEMPOTENT_KEY", Value: []byte(idemKey)},
			{Key: "RETRY_COUNT", Value: []byte(strconv.Itoa(next))},
		}
		_ = a.KW.WriteMessages(ctx, kafka.Message{
			Topic:   voucherRetryTopic,
			Key:     m.Key,
			Value:   body,
			Headers: headers,
		})
		return
	}
	body, _ := json.Marshal(vo)
	_ = a.KW.WriteMessages(ctx, kafka.Message{Topic: voucherDLTTopic, Key: m.Key, Value: body})
	_ = a.logKafkaDlt(ctx, msgID, bizVoucher, strconv.FormatInt(vo.ID, 10), voucherDLTTopic, "max retry")
}
