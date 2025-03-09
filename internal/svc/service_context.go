package svc

import (
	"github.com/go-redis/redis/v8"
	"github.com/ocean386/common/gormcache/cache"
	"github.com/ocean386/common/gormcache/config"
	"github.com/ocean386/common/gormcache/storage"
	"github.com/ocean386/common/orm/dao"
	"github.com/ocean386/common/snowflake"
	"github.com/ocean386/common/zorm"
	stockCfg "github.com/ocean386/stock-task/internal/config"
	zeroRedis "github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/gorm"
)

type ServiceContext struct {
	Config          stockCfg.Config
	Redis           *zeroRedis.Redis
	GormDB          *gorm.DB
	SnowFlakeWorker *snowflake.SnowFlakeIdWorker
}

func NewServiceContext(cfg stockCfg.Config) *ServiceContext {

	gormDB := zorm.NewClient(&cfg.ZormConf)
	if cfg.RedisConf.Type == zeroRedis.NodeType { //Redis 节点
		redisClient := redis.NewClient(&redis.Options{
			Addr:     cfg.RedisConf.Host,
			Password: cfg.RedisConf.Pass,
		})

		cache, err := cache.NewGorm2Cache(&config.CacheConfig{
			CacheLevel:           config.CacheLevelAll,
			CacheStorage:         storage.NewRedis(&storage.RedisStoreConfig{Client: redisClient}),
			InvalidateWhenUpdate: true,           // create/update/delete 操作, 缓存失效
			CacheTTL:             1000 * 60 * 20, // 缓存时间-20分钟
			CacheMaxItemCnt:      50,             // 设置Sql语句一次查询,记录最大数量,超过该数值 则缓存失效
			Tables:               []string{},
		})
		if err != nil {
			panic(err)
		}

		err = gormDB.Use(cache)
		if err != nil {
			panic(err)
		}
	} else { // Redis集群
		redisClient := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: []string{cfg.RedisConf.Host},
		})
		cache, err := cache.NewGorm2Cache(&config.CacheConfig{
			CacheLevel: config.CacheLevelAll,
			CacheStorage: storage.NewClusterRedis(&storage.ClusterRedisStoreConfig{Client: redisClient, Options: &redis.ClusterOptions{
				Addrs: []string{cfg.RedisConf.Host},
			}}),
			InvalidateWhenUpdate: true,           // create/update/delete 操作, 缓存失效
			CacheTTL:             1000 * 60 * 20, // 缓存时间-20分钟
			CacheMaxItemCnt:      50,             // 设置Sql语句一次查询,记录最大数量,超过该数值 则缓存失效
			Tables:               []string{},
		})
		if err != nil {
			panic(err)
		}

		err = gormDB.Use(cache)
		if err != nil {
			panic(err)
		}
	}

	dao.SetDefault(gormDB) //设置dao默认数据库实例
	redisClient := zeroRedis.MustNewRedis(cfg.RedisConf)

	svc := &ServiceContext{
		Config:          cfg,
		Redis:           redisClient,
		GormDB:          gormDB,
		SnowFlakeWorker: snowflake.NewSnowFlakeWorker(redisClient),
	}

	return svc
}
