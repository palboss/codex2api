package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// DatabaseConfig PostgreSQL 核心配置
type DatabaseConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

// DSN 返回 PostgreSQL 连接字符串
func (d *DatabaseConfig) DSN() string {
	sslMode := d.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		d.Host, d.Port, d.User, d.Password, d.DBName, sslMode)
}

// RedisConfig Redis 核心配置
type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

// Config 全局核心环境配置（物理隔离的服务器参数）
// 业务逻辑参数（如 ProxyURL，APIKeys，MaxConcurrency）已全部移至数据库 SystemSettings 进行化
type Config struct {
	Port     int
	Database DatabaseConfig
	Redis    RedisConfig
}

// Load 从 .env 文件加载核心环境配置，支持环境变量覆盖
func Load(envPath string) (*Config, error) {
	// 尝试加载 .env 文件（可选，如果文件不存在则忽略并使用当前环境变量）
	if envPath == "" {
		envPath = ".env"
	}
	_ = godotenv.Load(envPath) 

	cfg := &Config{Port: 8080}

	// Web服务端口
	if port := os.Getenv("CODEX_PORT"); port != "" {
		fmt.Sscanf(port, "%d", &cfg.Port)
	} else if port := os.Getenv("PORT"); port != "" {
		fmt.Sscanf(port, "%d", &cfg.Port)
	}

	// 数据库配置
	cfg.Database.Host = os.Getenv("DATABASE_HOST")
	if v := os.Getenv("DATABASE_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			cfg.Database.Port = p
		}
	}
	cfg.Database.User = os.Getenv("DATABASE_USER")
	cfg.Database.Password = os.Getenv("DATABASE_PASSWORD")
	cfg.Database.DBName = os.Getenv("DATABASE_NAME")
	if v := os.Getenv("DATABASE_SSLMODE"); v != "" {
		cfg.Database.SSLMode = v
	}

	// Redis配置
	cfg.Redis.Addr = os.Getenv("REDIS_ADDR")
	cfg.Redis.Password = os.Getenv("REDIS_PASSWORD")
	if v := os.Getenv("REDIS_DB"); v != "" {
		if db, err := strconv.Atoi(v); err == nil {
			cfg.Redis.DB = db
		}
	}

	// 校验必填物理层配置
	if cfg.Database.Host == "" {
		return nil, fmt.Errorf("必须通过 .env 或环境变量配置 PostgreSQL (DATABASE_HOST)")
	}
	if cfg.Database.Port == 0 {
		cfg.Database.Port = 5432
	}
	if cfg.Database.SSLMode == "" {
		cfg.Database.SSLMode = "disable"
	}
	if cfg.Redis.Addr == "" {
		return nil, fmt.Errorf("必须通过 .env 或环境变量配置 Redis (REDIS_ADDR)")
	}

	return cfg, nil
}
