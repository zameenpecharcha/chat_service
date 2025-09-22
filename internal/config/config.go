package config

import (
    "errors"
    "fmt"
    "os"
    "strconv"
    "time"

    "github.com/joho/godotenv"
    "gopkg.in/yaml.v3"
)

type Config struct {
    ServiceName   string `yaml:"service_name"`
    Port          int    `yaml:"port"`
    LogLevel      string `yaml:"log_level"`
    RedisAddr     string `yaml:"redis_addr"`
    RedisPassword string `yaml:"redis_password"`
}

func Default() Config {
    return Config{
        ServiceName: "chat-service",
        Port:        50060,
        LogLevel:    "info",
        RedisAddr:   "",
    }
}

func Load() (Config, error) {
    _ = godotenv.Load()

    cfg := Default()

    // Optional config from YAML
    if path := getenv("CONFIG_FILE", ""); path != "" {
        if y, err := os.ReadFile(path); err == nil {
            _ = yaml.Unmarshal(y, &cfg)
        }
    } else {
        if y, err := os.ReadFile("config/config.yaml"); err == nil {
            _ = yaml.Unmarshal(y, &cfg)
        }
    }

    if v := os.Getenv("SERVICE_NAME"); v != "" {
        cfg.ServiceName = v
    }
    if v := os.Getenv("PORT"); v != "" {
        if p, err := strconv.Atoi(v); err == nil {
            cfg.Port = p
        }
    }
    if v := os.Getenv("LOG_LEVEL"); v != "" {
        cfg.LogLevel = v
    }
    if v := os.Getenv("REDIS_ADDR"); v != "" {
        cfg.RedisAddr = v
    }
    if v := os.Getenv("REDIS_PASSWORD"); v != "" {
        cfg.RedisPassword = v
    }

    return cfg, validate(cfg)
}

func validate(c Config) error {
    if c.Port <= 0 || c.Port > 65535 {
        return errors.New("invalid port")
    }
    return nil
}

func getenv(key, fallback string) string { if v := os.Getenv(key); v != "" { return v }; return fallback }

func DurationEnv(key string, def time.Duration) time.Duration {
    if v := os.Getenv(key); v != "" {
        if d, err := time.ParseDuration(v); err == nil { return d }
    }
    return def
}

func Addr(port int) string { return fmt.Sprintf(":%d", port) }


