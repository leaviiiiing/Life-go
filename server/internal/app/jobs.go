package app

import (
	"context"
	"log"
	"strconv"
	"time"
)

func (a *App) StartSchedulers(ctx context.Context) {
	go a.loopPayTimeout(ctx)
	go a.loopMQAudit(ctx)
}

func (a *App) loopPayTimeout(ctx context.Context) {
	t := time.NewTicker(a.Cfg.OrderPayScanInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !a.Cfg.OrderPayTimeoutEnabled {
				continue
			}
			a.scanPayTimeout(ctx)
		}
	}
}

func (a *App) scanPayTimeout(ctx context.Context) {
	min := a.Cfg.OrderPayTimeoutMinutes
	if min < 1 {
		min = 1
	}
	batch := a.Cfg.OrderPayTimeoutBatch
	if batch < 1 {
		batch = 1
	}
	if batch > 500 {
		batch = 500
	}
	var orders []VoucherOrder
	err := a.DB.SelectContext(ctx, &orders, `SELECT id,user_id,voucher_id FROM tb_voucher_order WHERE status=1 AND create_time < DATE_SUB(NOW(), INTERVAL ? MINUTE) LIMIT ?`, min, batch)
	if err != nil || len(orders) == 0 {
		return
	}
	for _, o := range orders {
		if err := a.cancelUnpaidOrder(ctx, &o); err != nil {
			log.Printf("pay timeout cancel err order=%d: %v", o.ID, err)
		}
	}
}

func (a *App) cancelUnpaidOrder(ctx context.Context, o *VoucherOrder) error {
	tx, err := a.DB.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE tb_voucher_order SET status=4,update_time=NOW() WHERE id=? AND status=1`, o.ID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil
	}
	_, err = tx.ExecContext(ctx, `UPDATE tb_seckill_voucher SET stock=stock+1 WHERE voucher_id=?`, o.VoucherID)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	stockKey := "seckill:stock:" + strconv.FormatInt(o.VoucherID, 10)
	orderKey := "seckill:order:" + strconv.FormatInt(o.VoucherID, 10)
	if ok, _ := a.RDB.Exists(ctx, stockKey).Result(); ok > 0 {
		_, _ = a.RDB.Incr(ctx, stockKey).Result()
	}
	_, _ = a.RDB.SRem(ctx, orderKey, strconv.FormatInt(o.UserID, 10)).Result()
	return nil
}

func (a *App) loopMQAudit(ctx context.Context) {
	t := time.NewTicker(a.Cfg.MQCompensationScanInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			var c1, c2 int64
			_ = a.DB.GetContext(ctx, &c1, `SELECT COUNT(*) FROM tb_mq_kafka_log WHERE direction='CONSUME' AND status='FAILED'`)
			_ = a.DB.GetContext(ctx, &c2, `SELECT COUNT(*) FROM tb_mq_kafka_log WHERE direction='DLT'`)
			if c1 > 0 || c2 > 0 {
				log.Printf("[MQ补偿巡检] 审计表消费失败条数=%d DLT条数=%d（详见 tb_mq_kafka_log）", c1, c2)
			}
		}
	}
}
