package svc

import (
	"github.com/ocean386/stock-task/internal/config"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type ServiceContext struct {
	Config config.Config
	Redis  *redis.Redis
}

func NewServiceContext(cfg config.Config) *ServiceContext {

	redisClient := redis.MustNewRedis(cfg.RedisConf)
	return &ServiceContext{
		Config: cfg,
		Redis:  redisClient,
	}
}
