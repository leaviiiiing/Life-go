package app

import (
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/leaviiiiing/Life-go/server/internal/config"
)

// App wires HTTP handlers, persistence, Redis, and Kafka producers.
type App struct {
	Cfg        *config.Config
	DB         *sqlx.DB
	RDB        *redis.Client
	KW         *kafka.Writer
	SeckillLua string
	FaqJSON    []byte
}

func NewApp(cfg *config.Config, db *sqlx.DB, rdb *redis.Client, kw *kafka.Writer, seckillLua string, faq []byte) *App {
	return &App{Cfg: cfg, DB: db, RDB: rdb, KW: kw, SeckillLua: seckillLua, FaqJSON: faq}
}
