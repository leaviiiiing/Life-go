package app

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"time"

	"github.com/bsm/redislock"
	goredis "github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func (a *App) StartKafkaConsumers(ctx context.Context) {
	go a.runVoucherOrderConsumer(ctx)
	go a.runBlogFeedConsumer(ctx)
}

func (a *App) runVoucherOrderConsumer(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        a.Cfg.KafkaBrokers,
		GroupID:        "voucher-order-consumer-group",
		GroupTopics:    []string{voucherOrderTopic, voucherRetryTopic},
		CommitInterval: 0,
	})
	defer r.Close()
	locker := redislock.New(a.RDB)
	for {
		if ctx.Err() != nil {
			return
		}
		m, err := r.FetchMessage(ctx)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		msgID := headerLast(&m, "MSG_ID")
		idem := headerLast(&m, "IDEMPOTENT_KEY")
		if idem == "" {
			idem = msgID
		}
		retry := parseRetryCount(headerLast(&m, "RETRY_COUNT"))
		var vo VoucherOrder
		_ = json.Unmarshal(m.Value, &vo)
		if vo.ID == 0 {
			_ = r.CommitMessages(ctx, m)
			continue
		}
		if a.rdbHas(ctx, kafkaConsumedPref+idem) {
			_ = r.CommitMessages(ctx, m)
			continue
		}
		lock, err := locker.Obtain(ctx, "lock:order:"+strconv.FormatInt(vo.UserID, 10), 30*time.Second, nil)
		if errors.Is(err, redislock.ErrNotObtained) {
			continue
		}
		if err != nil {
			log.Printf("voucher lock: %v", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		func() {
			defer lock.Release(ctx)
			if err := a.createVoucherOrderTx(ctx, &vo); err != nil {
				_ = a.logKafkaConsumeFailed(ctx, msgID, bizVoucher, strconv.FormatInt(vo.ID, 10), m.Topic, m.Partition, m.Offset, err.Error())
				a.handleVoucherRetryOrDLT(ctx, &m, &vo, idem, msgID, retry)
				return
			}
			_ = a.rdbSet(ctx, kafkaConsumedPref+idem, 7*24*time.Hour)
		}()
		_ = r.CommitMessages(ctx, m)
	}
}

func (a *App) runBlogFeedConsumer(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        a.Cfg.KafkaBrokers,
		GroupID:        "blog-feed-consumer-group",
		Topic:          blogFeedTop,
		CommitInterval: 0,
	})
	defer r.Close()
	for {
		if ctx.Err() != nil {
			return
		}
		m, err := r.FetchMessage(ctx)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		msgID := headerLast(&m, "MSG_ID")
		idem := headerLast(&m, "IDEMPOTENT_KEY")
		if idem == "" {
			idem = msgID
		}
		var payload BlogFeedMessage
		_ = json.Unmarshal(m.Value, &payload)
		if payload.BlogID == 0 {
			_ = r.CommitMessages(ctx, m)
			continue
		}
		if a.rdbHas(ctx, kafkaConsumedPref+idem) {
			_ = r.CommitMessages(ctx, m)
			continue
		}
		rows, err := a.DB.QueryContext(ctx, `SELECT user_id FROM tb_follow WHERE follow_user_id=?`, payload.UserID)
		if err == nil {
			for rows.Next() {
				var fan int64
				_ = rows.Scan(&fan)
				key := feedKey + strconv.FormatInt(fan, 10)
				_ = a.RDB.ZAdd(ctx, key, goredis.Z{Score: float64(payload.Timestamp), Member: strconv.FormatInt(payload.BlogID, 10)}).Err()
			}
			_ = rows.Close()
		}
		_ = a.rdbSet(ctx, kafkaConsumedPref+idem, 7*24*time.Hour)
		_ = r.CommitMessages(ctx, m)
	}
}
