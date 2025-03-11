package main

import (
	"flag"
	"github.com/dcron-contrib/redisdriver"
	"github.com/libi/dcron"
	"github.com/ocean386/stock-task/internal/config"
	"github.com/ocean386/stock-task/internal/handler"
	"github.com/ocean386/stock-task/internal/nacos"
	"github.com/ocean386/stock-task/internal/svc"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest"
	"time"
)

var (
	nacosDataId = "stocktask" // 配置 Data ID
	nacosGroup  = "DEV"       // 配置组名
)

// 服务启动参数[编辑配置-环境]: NACOS_USERNAME=nacos;NACOS_PASSWORD=OKNacos;NACOS_IP=127.0.0.1
func main() {
	flag.Parse()

	// 加载yaml配置信息-naocs
	strNacosCfg, err := nacos.GetConfigFromNacos(nacosDataId, nacosGroup)
	if err != nil {
		logx.Errorf("Failed to get config from Nacos: %v", err)
		return
	}

	// 将 Nacos 配置内容加载到结构体中
	var cfg config.Config
	if err = conf.LoadFromYamlBytes([]byte(strNacosCfg), &cfg); err != nil {
		logx.Errorf("Failed to load config: %v", err)
		return
	}

	server := rest.MustNewServer(cfg.RestConf, rest.WithCors("*"))
	defer server.Stop()

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisConf.Host,
		Password: cfg.RedisConf.Pass,
	})

	svcCtx := svc.NewServiceContext(cfg)
	redisDriver := redisdriver.NewDriver(redisClient)
	dCron := dcron.NewDcronWithOption("DCronServer", redisDriver,
		dcron.WithHashReplicas(10),
		dcron.WithNodeUpdateDuration(time.Second*10),
		dcron.CronOptionSeconds(),
	)

	//dCron.AddFunc("StockHTTP", "00 07 18 * * *", func() {
	//	task.IsStockNew()
	//})

	go dCron.Start()
	handler.RegisterHandlers(server, svcCtx)

	logx.Infof("Starting API Server [%s:%d]", cfg.Host, cfg.Port)
	server.Start()
}
