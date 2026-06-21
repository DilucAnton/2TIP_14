package config

import "time"

type Config struct {
	RedisAddr             string
	RedisPassword         string
	CacheTTL              time.Duration
	CacheTTLJitter        time.Duration
	RedisDialTimeout      time.Duration
	RedisReadTimeout      time.Duration
	RedisWriteTimeout     time.Duration
}

func New() Config {
	return Config{
		RedisAddr:         "localhost:6379",
		RedisPassword:     "",
		CacheTTL:          120 * time.Second,
		CacheTTLJitter:    30 * time.Second,
		RedisDialTimeout:  2 * time.Second,
		RedisReadTimeout:  2 * time.Second,
		RedisWriteTimeout: 2 * time.Second,
	}
}
