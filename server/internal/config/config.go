package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime settings (env-first, aligned with Java / agent compose).
type Config struct {
	MainAddr  string
	AgentAddr string

	MySQLDSN string

	RedisAddr     string
	RedisPassword string
	RedisDB       int

	KafkaBrokers []string

	UploadDir string

	OrderPayTimeoutEnabled bool
	OrderPayTimeoutMinutes int
	OrderPayTimeoutBatch   int
	OrderPayScanInterval   time.Duration

	MQCompensationScanInterval time.Duration

	AgentBackendBaseURL    string
	AgentRateLimitPerMin   int
	AgentLLMEnabled        bool
	AgentLLMAPIKey         string
	AgentLLMBaseURL        string
	AgentLLMModel          string
	AgentLLMTimeout        time.Duration
	AgentLLMTemperature    float64
}

func getenv(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getenvBool(key string, def bool) bool {
	if v := strings.TrimSpace(strings.ToLower(os.Getenv(key))); v != "" {
		return v == "1" || v == "true" || v == "yes"
	}
	return def
}

func getenvDuration(key string, def time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func getenvMillisAsDuration(key string, defMs int) time.Duration {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Millisecond
		}
	}
	return time.Duration(defMs) * time.Millisecond
}

func getenvFloat(key string, def float64) float64 {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func Load() *Config {
	kb := getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	brokers := strings.Split(kb, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	return &Config{
		MainAddr:  getenv("MAIN_HTTP_ADDR", ":8081"),
		AgentAddr: getenv("AGENT_HTTP_ADDR", ":8082"),

		MySQLDSN: getenv("MYSQL_DSN", "root:123456@tcp(127.0.0.1:3306)/hmdp?parseTime=true&loc=Asia%2FShanghai&charset=utf8mb4"),

		RedisAddr:     getenv("SPRING_REDIS_HOST", "127.0.0.1") + ":" + getenv("SPRING_REDIS_PORT", "6379"),
		RedisPassword: getenv("SPRING_REDIS_PASSWORD", ""),
		RedisDB:       getenvInt("SPRING_REDIS_DATABASE", 0),

		KafkaBrokers: brokers,

		UploadDir: getenv("HMDP_UPLOAD_DIR", "./data/uploads"),

		OrderPayTimeoutEnabled: getenvBool("ORDER_PAY_TIMEOUT_ENABLED", true),
		OrderPayTimeoutMinutes: getenvInt("ORDER_PAY_TIMEOUT_MINUTES", 15),
		OrderPayTimeoutBatch:   getenvInt("ORDER_PAY_TIMEOUT_BATCH_SIZE", 100),
		OrderPayScanInterval:   getenvMillisAsDuration("ORDER_PAY_TIMEOUT_SCAN_MS", 60_000),

		MQCompensationScanInterval: getenvMillisAsDuration("MQ_COMPENSATION_SCAN_MS", 300_000),

		AgentBackendBaseURL:  getenv("AGENT_BACKEND_BASE_URL", "http://127.0.0.1:8081"),
		AgentRateLimitPerMin: getenvInt("AGENT_RATE_LIMIT", 120),
		AgentLLMEnabled:      getenvBool("AGENT_LLM_ENABLED", true),
		AgentLLMAPIKey:       os.Getenv("AGENT_LLM_API_KEY"),
		AgentLLMBaseURL:      getenv("AGENT_LLM_BASE_URL", "https://ark.cn-beijing.volces.com/api/v3"),
		AgentLLMModel:        os.Getenv("AGENT_LLM_MODEL"),
		AgentLLMTimeout:      getenvMillisAsDuration("AGENT_LLM_TIMEOUT_MS", 90_000),
		AgentLLMTemperature:  getenvFloat("AGENT_LLM_TEMPERATURE", 0.6),
	}
}
