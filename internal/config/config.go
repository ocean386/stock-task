package config

import (
	"github.com/ocean386/common/zorm"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
)

type Config struct {
	rest.RestConf
	Auth      rest.AuthConf
	RedisConf redis.RedisConf
	ZormConf  zorm.DBConfig
}
