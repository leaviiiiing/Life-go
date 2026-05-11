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

func main() {
	cfg := config.Load()
	db, err := sqlx.Connect("mysql", cfg.MySQLDSN)
	if err != nil {
		log.Fatalf("mysql: %v", err)
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	defer rdb.Close()
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis: %v", err)
	}

	kw := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.KafkaBrokers...),
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
	}
	defer func() { _ = kw.Close() }()

	application := app.NewApp(cfg, db, rdb, kw, app.SeckillScript(), app.FAQBytes())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	application.StartKafkaConsumers(ctx)
	application.StartSchedulers(ctx)

	gin.SetMode(gin.ReleaseMode)
	mainEngine := gin.New()
	mainEngine.Use(gin.Recovery())
	application.RegisterMain(mainEngine)

	agentEngine := gin.New()
	agentEngine.Use(gin.Recovery())
	application.RegisterAgent(agentEngine)

	mainSrv := &http.Server{Addr: cfg.MainAddr, Handler: mainEngine}
	agentSrv := &http.Server{Addr: cfg.AgentAddr, Handler: agentEngine}

	go func() {
		log.Printf("main API listening %s", cfg.MainAddr)
		if err := mainSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	go func() {
		log.Printf("agent listening %s", cfg.AgentAddr)
		if err := agentSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	shutdownCtx, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	_ = mainSrv.Shutdown(shutdownCtx)
	_ = agentSrv.Shutdown(shutdownCtx)
	cancel()
}
